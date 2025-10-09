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

const SUBSCRIBE_TOPIC: &str = "user";
const USER_CHANNEL_TYPE: &str = "user";
const PING_TEXT: &str = "PING";

#[derive(Clone)]
pub struct UserWsV2 {
    url: String,
    creds: Arc<ApiCreds>,
    tx: mpsc::Sender<UserEvent>,
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<UserEvent>>>,
    _bg: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
}

impl UserWsV2 {
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

    fn side_from(value: &str) -> Side {
        match value {
            "BUY" | "buy" | "bid" => Side::Buy,
            _ => Side::Sell,
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

    fn parse_payload(event_type: &str, payload: &Value) -> Option<UserEvent> {
        match event_type.to_ascii_lowercase().as_str() {
            "order" => Self::parse_order(payload),
            "trade" => Self::parse_trade(payload),
            _ => None,
        }
    }

    fn parse_order(p: &Value) -> Option<UserEvent> {
        let id = Self::any_to_string(p.get("id"));
        if id.is_empty() {
            return None;
        }
        let asset_id = Self::any_to_string(p.get("asset_id"));
        let market = Self::any_to_string(p.get("market"));
        let price = Self::num_to_f64(p.get("price"));
        let side = p
            .get("side")
            .and_then(|v| v.as_str())
            .map(Self::side_from)
            .unwrap_or(Side::Buy);
        let size_matched = Self::num_to_f64(p.get("size_matched"));
        let mut timestamp = Self::num_to_i64(p.get("timestamp"));
        if timestamp == 0 {
            timestamp = Self::num_to_i64(p.get("last_update"));
        }
        if timestamp == 0 {
            timestamp = Self::num_to_i64(p.get("matchtime"));
        }
        if timestamp == 0 {
            timestamp = Self::num_to_i64(p.get("created_at"));
        }
        let mut status = Self::any_to_string(p.get("status"));
        if status.is_empty() {
            status = Self::any_to_string(p.get("type"));
        }
        let event = OrderUpdate {
            event_type: "order".into(),
            id,
            asset_id,
            market,
            price,
            side,
            size_matched,
            status,
            timestamp,
        };
        Some(UserEvent::Order(event))
    }

    fn parse_trade(p: &Value) -> Option<UserEvent> {
        let id = Self::any_to_string(p.get("id"));
        if id.is_empty() {
            return None;
        }
        let asset_id = Self::any_to_string(p.get("asset_id"));
        let market = Self::any_to_string(p.get("market"));
        let price = Self::num_to_f64(p.get("price"));
        let side = p
            .get("side")
            .and_then(|v| v.as_str())
            .map(Self::side_from)
            .unwrap_or(Side::Buy);
        let size = Self::num_to_f64(p.get("size"));
        let mut status = Self::any_to_string(p.get("status"));
        if status.is_empty() {
            status = Self::any_to_string(p.get("type"));
        }
        let mut timestamp = Self::num_to_i64(p.get("timestamp"));
        if timestamp == 0 {
            timestamp = Self::num_to_i64(p.get("last_update"));
        }
        if timestamp == 0 {
            timestamp = Self::num_to_i64(p.get("matchtime"));
        }
        if timestamp == 0 {
            timestamp = Self::num_to_i64(p.get("created_at"));
        }

        if let Some(makers) = p.get("maker_orders").and_then(|v| v.as_array()) {
            tracing::debug!(
                target: "ws_user2",
                trade_id = %id,
                makers = makers.len(),
                "trade contains maker order metadata"
            );
        }

        let event = UserTrade {
            event_type: "trade".into(),
            id,
            asset_id,
            market,
            price,
            side,
            size,
            status,
            timestamp,
        };
        Some(UserEvent::Trade(event))
    }

    fn parse_events(value: Value) -> Vec<UserEvent> {
        match &value {
            Value::Array(arr) => arr
                .iter()
                .filter_map(|item| Self::parse_event_object(item))
                .collect(),
            Value::Object(_) => Self::parse_event_object(&value).into_iter().collect(),
            _ => Vec::new(),
        }
    }

    fn parse_event_object(v: &Value) -> Option<UserEvent> {
        if let Some(topic) = v.get("topic").and_then(|t| t.as_str()) {
            if topic != SUBSCRIBE_TOPIC && topic != "clob_user" {
                return None;
            }
        }
        let event_type = if let Some(kind) = v.get("event_type").and_then(|t| t.as_str()) {
            kind
        } else if let Some(kind) = v.get("type").and_then(|t| t.as_str()) {
            kind
        } else {
            ""
        };
        let payload = v.get("payload").unwrap_or(v);
        Self::parse_payload(event_type, payload)
    }

    fn subscribe_payload(&self) -> Value {
        serde_json::json!({
            "markets": [],
            "type": USER_CHANNEL_TYPE,
            "auth": {
                "apiKey": self.creds.api_key,
                "secret": self.creds.secret,
                "passphrase": self.creds.passphrase,
            }
        })
    }
}

#[async_trait]
impl UserStream for UserWsV2 {
    async fn connect(&self) -> Result<()> {
        tracing::info!(target: "ws_user2", "connecting to {}", &self.url);
        let (ws, _) = connect_async(&self.url).await?;
        let mut stream = ws;
        tracing::info!(target: "ws_user2", "connected");

        let subscribe_value = self.subscribe_payload();
        let subscribe_text = serde_json::to_string(&subscribe_value)?;
        tracing::info!(target: "ws_user2", payload = %subscribe_text, "sending subscribe");
        stream.send(Message::Text(subscribe_text.clone())).await?;
        let subscribe_text = Arc::new(subscribe_text);

        let tx = self.tx.clone();
        let url = self.url.clone();
        let subscribe_text_bg = Arc::clone(&subscribe_text);
        let handle = tokio::spawn(async move {
            let mut stream_opt = Some(stream);
            let mut backoff = Duration::from_secs(1);
            loop {
                if let Some(mut s) = stream_opt.take() {
                    let mut hb = tokio::time::interval(Duration::from_secs(10));
                    let mut last_pong = std::time::Instant::now();
                    loop {
                        tokio::select! {
                            _ = hb.tick() => {
                                if let Err(e) = s.send(Message::Text(PING_TEXT.into())).await {
                                    tracing::warn!(target: "ws_user2", "ping send failed: {}", e);
                                    break;
                                }
                                if last_pong.elapsed() > Duration::from_secs(60) {
                                    tracing::warn!(target: "ws_user2", "pong timeout, reconnecting");
                                    break;
                                }
                            }
                            msg = s.next() => match msg {
                                Some(Ok(Message::Text(txt))) => {
                                    tracing::info!(target: "ws_user2", raw = %txt, "received raw user message" );
                                    if txt.eq_ignore_ascii_case("pong") {
                                        last_pong = std::time::Instant::now();
                                        continue;
                                    }
                                    if let Ok(value) = serde_json::from_str::<Value>(&txt) {
                                        let events = Self::parse_events(value);
                                        for ev in events {
                                            if tx.try_send(ev).is_err() {
                                                tracing::warn!(target: "ws_user2", "dropping user event due to full channel");
                                            }
                                        }
                                    }
                                }
                                Some(Ok(Message::Binary(bin))) => {
                                    tracing::info!(target: "ws_user2", raw = ?bin, "received raw user message (binary)" );
                                    if let Ok(value) = serde_json::from_slice::<Value>(&bin) {
                                        let events = Self::parse_events(value);
                                        for ev in events {
                                            if tx.try_send(ev).is_err() {
                                                tracing::warn!(target: "ws_user2", "dropping user event due to full channel");
                                            }
                                        }
                                    }
                                }
                                Some(Ok(Message::Ping(payload))) => {
                                    if s.send(Message::Pong(payload)).await.is_err() {
                                        tracing::warn!(target: "ws_user2", "failed to answer ping");
                                        break;
                                    }
                                }
                                Some(Ok(Message::Pong(_))) => {
                                    last_pong = std::time::Instant::now();
                                }
                                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                                    break;
                                }
                                Some(Ok(_)) => {}
                            }
                        }
                    }
                }

                tracing::info!(target: "ws_user2", "reconnecting in {:?}", backoff);
                sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));

                match connect_async(&url).await {
                    Ok((mut new_stream, _)) => {
                        tracing::info!(target: "ws_user2", "reconnected" );
                        if let Err(err) = new_stream
                            .send(Message::Text((*subscribe_text_bg).clone()))
                            .await
                        {
                            tracing::warn!(target: "ws_user2", ?err, "failed to send subscribe during reconnect");
                            continue;
                        }
                        stream_opt = Some(new_stream);
                        backoff = Duration::from_secs(1);
                    }
                    Err(err) => {
                        tracing::warn!(target: "ws_user2", ?err, "reconnect failed");
                    }
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
