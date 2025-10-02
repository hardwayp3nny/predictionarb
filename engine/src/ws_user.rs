use crate::auth::ApiCreds;
use crate::model::{OrderUpdate, Side, UserEvent, UserTrade};
use crate::ports::UserStream;
use anyhow::Result;
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Clone)]
pub struct UserWs {
    url: String,
    creds: Arc<ApiCreds>,
    tx: mpsc::Sender<UserEvent>,
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<UserEvent>>>,
    _bg: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
}

impl UserWs {
    pub fn new(url: &str, creds: ApiCreds) -> Self {
        let (tx, rx) = mpsc::channel(10_000);
        Self {
            url: url.to_string(),
            creds: Arc::new(creds),
            tx,
            rx: Arc::new(tokio::sync::Mutex::new(rx)),
            _bg: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    fn side_from(s: &str) -> Side {
        match s {
            "BUY" | "buy" | "bid" => Side::Buy,
            _ => Side::Sell,
        }
    }

    fn parse_user_event(v: &Value) -> Option<UserEvent> {
        // format 1: { topic:"clob_user", type:"order"|"trade", payload:{...} }
        if v.get("topic").and_then(|t| t.as_str()) == Some("clob_user") {
            let et = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
            let p = v.get("payload")?;
            return Self::parse_payload(et, p);
        }
        // format 2: direct payload
        if let Some(et) = v.get("event_type").and_then(|t| t.as_str()) {
            return Self::parse_payload(et, v);
        }
        None
    }

    fn parse_events_with_kind(v: Value) -> Vec<(UserEvent, String)> {
        fn kind_from(v: &Value) -> String {
            if v.get("topic").and_then(|t| t.as_str()) == Some("clob_user") {
                return v
                    .get("type")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string();
            }
            v.get("event_type")
                .and_then(|t| t.as_str())
                .unwrap_or("")
                .to_string()
        }
        let mut out = Vec::new();
        match v {
            Value::Array(arr) => {
                for item in arr {
                    let k = kind_from(&item);
                    if let Some(ev) = Self::parse_user_event(&item) {
                        out.push((ev, k));
                    }
                }
            }
            Value::Object(_) => {
                let k = kind_from(&v);
                if let Some(ev) = Self::parse_user_event(&v) {
                    out.push((ev, k));
                }
            }
            _ => {}
        }
        out
    }

    fn num_to_f64(v: Option<&Value>) -> f64 {
        match v {
            Some(Value::String(s)) => s.parse().unwrap_or(0.0),
            Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0),
            Some(Value::Bool(b)) => {
                if *b {
                    1.0
                } else {
                    0.0
                }
            }
            _ => 0.0,
        }
    }

    fn num_to_i64(v: Option<&Value>) -> i64 {
        match v {
            Some(Value::String(s)) => s.parse().unwrap_or(0),
            Some(Value::Number(n)) => n.as_i64().unwrap_or(0),
            _ => 0,
        }
    }

    fn any_to_string(v: Option<&Value>) -> String {
        match v {
            Some(Value::String(s)) => s.to_string(),
            Some(Value::Number(n)) => n.to_string(),
            Some(Value::Bool(b)) => b.to_string(),
            _ => String::new(),
        }
    }

    fn parse_payload(et: &str, p: &Value) -> Option<UserEvent> {
        match et {
            "order" | "order_update" => {
                let id = Self::any_to_string(p.get("id"));
                if id.is_empty() {
                    return None;
                }
                let asset_id = p
                    .get("asset_id")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let market = p
                    .get("market")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let price = Self::num_to_f64(p.get("price"));
                let side = p
                    .get("side")
                    .and_then(|x| x.as_str())
                    .map(Self::side_from)
                    .unwrap_or(Side::Buy);
                let size_matched = Self::num_to_f64(p.get("size_matched"));
                let ts = {
                    let c = Self::num_to_i64(p.get("created_at"));
                    if c != 0 {
                        c
                    } else {
                        let t = Self::num_to_i64(p.get("timestamp"));
                        if t != 0 {
                            t
                        } else {
                            Self::num_to_i64(p.get("last_update"))
                        }
                    }
                };
                let status = p
                    .get("status")
                    .and_then(|x| x.as_str())
                    .or_else(|| p.get("state").and_then(|x| x.as_str()))
                    .unwrap_or("")
                    .to_string();
                let ou = OrderUpdate {
                    event_type: "order".into(),
                    id,
                    asset_id,
                    market,
                    price,
                    side,
                    size_matched,
                    status,
                    timestamp: ts,
                };
                Some(UserEvent::Order(ou))
            }
            "trade" | "user_trade" => {
                let id = Self::any_to_string(p.get("id"));
                let asset_id = p
                    .get("asset_id")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let market = p
                    .get("market")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let price = Self::num_to_f64(p.get("price"));
                let side = p
                    .get("side")
                    .and_then(|x| x.as_str())
                    .map(Self::side_from)
                    .unwrap_or(Side::Buy);
                let size = Self::num_to_f64(p.get("size"));
                let status = p
                    .get("status")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let ts = {
                    let lu = Self::num_to_i64(p.get("last_update"));
                    if lu != 0 {
                        lu
                    } else {
                        let t = Self::num_to_i64(p.get("timestamp"));
                        if t != 0 {
                            t
                        } else {
                            Self::num_to_i64(p.get("match_time"))
                        }
                    }
                };
                let ut = UserTrade {
                    event_type: "trade".into(),
                    id,
                    asset_id,
                    market,
                    price,
                    side,
                    size,
                    status,
                    timestamp: ts,
                };
                Some(UserEvent::Trade(ut))
            }
            _ => None,
        }
    }
}

#[async_trait]
impl UserStream for UserWs {
    async fn connect(&self) -> Result<()> {
        tracing::info!(target: "ws_user", "connecting to {}", &self.url);
        let (ws, _) = connect_async(&self.url).await?;
        let mut stream = ws;
        tracing::info!(target: "ws_user", "connected");
        // send subscribe message
        let sub = serde_json::json!({
            "action": "subscribe",
            "subscriptions": [{
                "topic": "clob_user",
                "type": "*",
                "clob_auth": {
                    "key": self.creds.api_key,
                    "secret": self.creds.secret,
                    "passphrase": self.creds.passphrase,
                }
            }]
        });
        let txt = serde_json::to_string(&sub)?;
        tracing::info!(target: "ws_user", "sending subscribe to clob_user");
        stream.send(Message::Text(txt)).await?;
        tracing::info!(target: "ws_user", "subscribe sent");

        // spawn read loop with reconnect
        let tx = self.tx.clone();
        let url = self.url.clone();
        let creds = self.creds.clone();
        let handle = tokio::spawn(async move {
            let mut stream_opt = Some(stream);
            let mut backoff = Duration::from_secs(1);
            loop {
                if let Some(mut s) = stream_opt.take() {
                    let mut hb = tokio::time::interval(Duration::from_secs(15));
                    let mut last_pong = std::time::Instant::now();
                    loop {
                        tokio::select! {
                            _ = hb.tick() => {
                                if let Err(e) = s.send(Message::Ping(Vec::new())).await {
                                    tracing::warn!(target: "ws_user", "ping send failed: {}", e);
                                    break;
                                }
                                if last_pong.elapsed() > Duration::from_secs(60) {
                                    tracing::warn!(target: "ws_user", "pong timeout, reconnecting");
                                    break;
                                }
                            }
                            msg = s.next() => match msg {
                            Some(Ok(Message::Text(t))) => {
                                if t == "pong" {
                                        tracing::debug!(target: "ws_user", "received pong");
                                        last_pong = std::time::Instant::now();
                                        continue;
                                }
                                if let Ok(v) = serde_json::from_str::<Value>(&t) {
                                    let events = UserWs::parse_events_with_kind(v);
                                    let n = events.len();
                                    if n > 0 {
                                        let (first_ev, src_kind) = &events[0];
                                        let norm = match first_ev { UserEvent::Order(_) => "order", UserEvent::Trade(_) => "trade" };
                                        tracing::info!(target: "ws_user", "received {} user event(s), first_kind={} (src={}) (text)", n, norm, src_kind);
                                        for (ev, _k) in events.into_iter() {
                                            match &ev {
                                                UserEvent::Order(ou) => {
                                                    tracing::info!(target: "ws_user", "order id={} asset={} mkt={} side={:?} px={} matched={} status={} ts={}", ou.id, ou.asset_id, ou.market, ou.side, ou.price, ou.size_matched, ou.status, ou.timestamp);
                                                }
                                                UserEvent::Trade(ut) => {
                                                    tracing::info!(target: "ws_user", "trade id={} asset={} mkt={} side={:?} px={} size={} status={} ts={}", ut.id, ut.asset_id, ut.market, ut.side, ut.price, ut.size, ut.status, ut.timestamp);
                                                }
                                            }
                                            let _ = tx.try_send(ev);
                                        }
                                    }
                                }
                                }
                                Some(Ok(Message::Binary(b))) => {
                                if let Ok(v) = serde_json::from_slice::<Value>(&b) {
                                    let events = UserWs::parse_events_with_kind(v);
                                    let n = events.len();
                                    if n > 0 {
                                        let (first_ev, src_kind) = &events[0];
                                        let norm = match first_ev { UserEvent::Order(_) => "order", UserEvent::Trade(_) => "trade" };
                                        tracing::info!(target: "ws_user", "received {} user event(s), first_kind={} (src={}) (bin)", n, norm, src_kind);
                                        for (ev, _k) in events.into_iter() {
                                            match &ev {
                                                UserEvent::Order(ou) => {
                                                    tracing::info!(target: "ws_user", "order id={} asset={} mkt={} side={:?} px={} matched={} status={} ts={}", ou.id, ou.asset_id, ou.market, ou.side, ou.price, ou.size_matched, ou.status, ou.timestamp);
                                                }
                                                UserEvent::Trade(ut) => {
                                                    tracing::info!(target: "ws_user", "trade id={} asset={} mkt={} side={:?} px={} size={} status={} ts={}", ut.id, ut.asset_id, ut.market, ut.side, ut.price, ut.size, ut.status, ut.timestamp);
                                                }
                                            }
                                            let _ = tx.try_send(ev);
                                        }
                                    }
                                }
                                }
                                Some(Ok(Message::Ping(payload))) => { let _ = s.send(Message::Pong(payload)).await; }
                                Some(Ok(Message::Pong(_))) => { last_pong = std::time::Instant::now(); }
                                Some(Ok(Message::Close(_))) | Some(Err(_)) => { break; }
                                Some(Ok(_)) => {}
                                None => { break; }
                            }
                        }
                    }
                }
                // reconnect
                tracing::info!(target: "ws_user", "reconnecting in {:?}", backoff);
                sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                if let Ok((mut s, _)) = connect_async(&url).await {
                    tracing::info!(target: "ws_user", "reconnected");
                    let sub = serde_json::json!({
                        "action": "subscribe",
                        "subscriptions": [{
                            "topic": "clob_user",
                            "type": "*",
                            "clob_auth": {
                                "key": creds.api_key,
                                "secret": creds.secret,
                                "passphrase": creds.passphrase,
                            }
                        }]
                    });
                    tracing::info!(target: "ws_user", "sending subscribe to clob_user (reconnect)");
                    let _ = s
                        .send(Message::Text(serde_json::to_string(&sub).unwrap()))
                        .await;
                    tracing::info!(target: "ws_user", "subscribe sent (reconnect)");
                    stream_opt = Some(s);
                    backoff = Duration::from_secs(1);
                }
            }
        });
        *self._bg.lock().await = Some(handle);
        Ok(())
    }

    async fn next(&self) -> Result<Option<UserEvent>> {
        let mut rx = self.rx.lock().await;
        Ok(rx.recv().await)
    }
}
