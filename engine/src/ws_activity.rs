use crate::model::{ActivityEvent, Side};
use crate::ports::ActivityStream;
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
pub struct ActivityWs {
    url: String,
    tx: mpsc::Sender<ActivityEvent>,
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<ActivityEvent>>>,
    _bg: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
}

impl ActivityWs {
    pub fn new(url: &str) -> Self {
        let (tx, rx) = mpsc::channel(10_000);
        Self {
            url: url.to_string(),
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

    fn parse_activity_event(v: &Value) -> Option<ActivityEvent> {
        let payload = if v.get("topic").and_then(|t| t.as_str()) == Some("activity") {
            v.get("payload")?
        } else {
            v
        };

        let side = payload
            .get("side")
            .and_then(|s| s.as_str())
            .map(Self::side_from)
            .unwrap_or(Side::Buy);

        let outcome_index = payload
            .get("outcomeIndex")
            .and_then(|idx| idx.as_i64())
            .and_then(|idx| if idx >= 0 { Some(idx as usize) } else { None });

        let price = payload
            .get("price")
            .and_then(|p| {
                if p.is_string() {
                    p.as_str().and_then(|s| s.parse::<f64>().ok())
                } else {
                    p.as_f64()
                }
            })
            .unwrap_or(0.0);

        let size = payload
            .get("size")
            .and_then(|p| {
                if p.is_string() {
                    p.as_str().and_then(|s| s.parse::<f64>().ok())
                } else {
                    p.as_f64()
                }
            })
            .unwrap_or(0.0);

        let usdc_size = payload.get("usdcSize").and_then(|p| {
            if p.is_string() {
                p.as_str().and_then(|s| s.parse::<f64>().ok())
            } else {
                p.as_f64()
            }
        });

        let timestamp = payload
            .get("timestamp")
            .and_then(|t| {
                if t.is_string() {
                    t.as_str().and_then(|s| s.parse::<i64>().ok())
                } else {
                    t.as_i64()
                }
            })
            .unwrap_or(0);

        Some(ActivityEvent {
            asset: payload
                .get("asset")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            bio: payload
                .get("bio")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            condition_id: payload
                .get("conditionId")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            event_slug: payload
                .get("eventSlug")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            icon: payload
                .get("icon")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            name: payload
                .get("name")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            outcome: payload
                .get("outcome")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            outcome_index,
            price,
            profile_image: payload
                .get("profileImage")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            proxy_wallet: payload
                .get("proxyWallet")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            pseudonym: payload
                .get("pseudonym")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            side,
            size,
            usdc_size,
            slug: payload
                .get("slug")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            timestamp,
            title: payload
                .get("title")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            transaction_hash: payload
                .get("transactionHash")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
        })
    }

    fn parse_events(v: Value) -> Vec<ActivityEvent> {
        match v {
            Value::Array(arr) => arr
                .into_iter()
                .filter_map(|item| ActivityWs::parse_activity_event(&item))
                .collect(),
            Value::Object(_) => ActivityWs::parse_activity_event(&v).into_iter().collect(),
            _ => Vec::new(),
        }
    }
}

#[async_trait]
impl ActivityStream for ActivityWs {
    async fn connect(&self) -> Result<()> {
        tracing::info!(target: "ws_activity", "connecting to {}", &self.url);
        let (ws, _) = connect_async(&self.url).await?;
        let mut stream = ws;
        tracing::info!(target: "ws_activity", "connected");

        let sub = serde_json::json!({
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "activity",
                    "type": "*"
                }
            ]
        });
        stream
            .send(Message::Text(serde_json::to_string(&sub)?))
            .await?;
        tracing::info!(target: "ws_activity", "subscribe sent");

        let tx = self.tx.clone();
        let url = self.url.clone();
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
                                    tracing::warn!(target: "ws_activity", "ping failed: {}", e);
                                    break;
                                }
                                if last_pong.elapsed() > Duration::from_secs(60) {
                                    tracing::warn!(target: "ws_activity", "pong timeout");
                                    break;
                                }
                            }
                            msg = s.next() => match msg {
                                Some(Ok(Message::Text(t))) => {
                                    if t == "pong" {
                                        last_pong = std::time::Instant::now();
                                        continue;
                                    }
                                    match serde_json::from_str::<Value>(&t) {
                                        Ok(val) => {
                                            let events = ActivityWs::parse_events(val);
                                            for ev in events {
                                                let _ = tx.try_send(ev);
                                            }
                                        }
                                        Err(err) => {
                                            tracing::debug!(target: "ws_activity", ?err, "failed to parse text message");
                                        }
                                    }
                                }
                                Some(Ok(Message::Binary(b))) => {
                                    match serde_json::from_slice::<Value>(&b) {
                                        Ok(val) => {
                                            let events = ActivityWs::parse_events(val);
                                            for ev in events {
                                                let _ = tx.try_send(ev);
                                            }
                                        }
                                        Err(err) => {
                                            tracing::debug!(target: "ws_activity", ?err, "failed to parse binary message");
                                        }
                                    }
                                }
                                Some(Ok(Message::Ping(payload))) => {
                                    let _ = s.send(Message::Pong(payload)).await;
                                }
                                Some(Ok(Message::Pong(_))) => {
                                    last_pong = std::time::Instant::now();
                                }
                                Some(Ok(Message::Close(_))) | Some(Err(_)) => {
                                    break;
                                }
                                Some(Ok(_)) => {}
                                None => break,
                            }
                        }
                    }
                }

                tracing::info!(target: "ws_activity", "reconnecting in {:?}", backoff);
                sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));

                match connect_async(&url).await {
                    Ok((mut s, _)) => {
                        tracing::info!(target: "ws_activity", "reconnected");
                        let sub = serde_json::json!({
                            "action": "subscribe",
                            "subscriptions": [
                                {
                                    "topic": "activity",
                                    "type": "*"
                                }
                            ]
                        });
                        if let Err(err) = s
                            .send(Message::Text(serde_json::to_string(&sub).unwrap()))
                            .await
                        {
                            tracing::warn!(target: "ws_activity", ?err, "failed to send subscribe on reconnect");
                        } else {
                            stream_opt = Some(s);
                            backoff = Duration::from_secs(1);
                        }
                    }
                    Err(err) => {
                        tracing::warn!(target: "ws_activity", ?err, "reconnect failed");
                    }
                }
            }
        });
        *self._bg.lock().await = Some(handle);
        Ok(())
    }

    async fn next(&self) -> Result<Option<ActivityEvent>> {
        let mut rx = self.rx.lock().await;
        Ok(rx.recv().await)
    }
}
