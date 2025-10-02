use crate::{model::MarketEvent, ports::MarketStream, ws_parser::parse_market_bytes};
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::{
    sync::{mpsc, RwLock},
    task::JoinHandle,
    time::{sleep, Duration},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

#[derive(Clone)]
pub struct MarketWs {
    url: Arc<Url>,
    tx: mpsc::Sender<MarketEvent>,
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<MarketEvent>>>,
    _bg: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
    assets: Arc<RwLock<Vec<String>>>,
}

impl MarketWs {
    pub fn new(ws_url: &str) -> Result<Self> {
        let (tx, rx) = mpsc::channel(10_000);
        Ok(Self {
            url: Arc::new(Url::parse(ws_url)?),
            tx,
            rx: Arc::new(tokio::sync::Mutex::new(rx)),
            _bg: Arc::new(tokio::sync::Mutex::new(None)),
            assets: Arc::new(RwLock::new(vec![])),
        })
    }

    pub async fn close(&self) -> Result<()> {
        if let Some(h) = self._bg.lock().await.take() {
            h.abort();
        }
        Ok(())
    }
}

#[async_trait]
impl MarketStream for MarketWs {
    async fn subscribe(&self, asset_ids: Vec<String>) -> Result<()> {
        {
            let mut w = self.assets.write().await;
            *w = asset_ids;
        }

        // 若已有后台任务，重启以立即应用新订阅
        if let Some(h) = self._bg.lock().await.take() {
            h.abort();
        }

        let url = self.url.clone();
        let tx = self.tx.clone();
        let assets = self.assets.clone();
        let handle = tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            let max_backoff = Duration::from_secs(30);
            loop {
                tracing::info!(target: "ws_market", "connecting to {}", url);
                match connect_async(url.as_str()).await {
                    Ok((mut stream, _)) => {
                        tracing::info!(target: "ws_market", "connected");
                        // snapshot current assets
                        let ids = { assets.read().await.clone() };
                        // Send a broad-compatible subscribe message
                        // Some servers expect "asset_ids", some use "assets_ids"; include both.
                        // Also include an explicit action field like user stream does.
                        let sub_body = serde_json::json!({
                            "action": "subscribe",
                            "type": "market",
                            "asset_ids": ids,
                            "assets_ids": ids,
                        });
                        if let Ok(sub_text) = serde_json::to_string(&sub_body) {
                            if let Err(e) = stream.send(Message::Text(sub_text)).await {
                                tracing::warn!(target: "ws_market", "send subscribe failed: {}", e);
                            } else {
                                tracing::info!(target: "ws_market", "subscribed to {} assets", ids.len());
                            }
                        }
                        backoff = Duration::from_secs(1); // reset backoff on success
                                                          // heartbeat + read loop
                        let mut hb = tokio::time::interval(Duration::from_secs(15));
                        let mut last_pong = std::time::Instant::now();
                        loop {
                            tokio::select! {
                                _ = hb.tick() => {
                                    if let Err(e) = stream.send(Message::Ping(Vec::new())).await {
                                        tracing::warn!(target: "ws_market", "ping send failed: {}", e);
                                        break;
                                    }
                                    if last_pong.elapsed() > Duration::from_secs(60) {
                                        tracing::warn!(target: "ws_market", "pong timeout, reconnecting");
                                        break;
                                    }
                                }
                                msg = stream.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            if let Some(events) = parse_market_bytes(text.as_bytes()) {
                                                for e in events { let _ = tx.try_send(e); }
                                            } else {
                                                let sample = if text.len() > 300 { &text[..300] } else { &text };
                                                tracing::info!(target: "ws_market", "unparsed text msg: {}", sample);
                                            }
                                        }
                                        Some(Ok(Message::Binary(bin))) => {
                                            if let Some(events) = parse_market_bytes(&bin) {
                                                for e in events { let _ = tx.try_send(e); }
                                            } else {
                                                let sample_len = bin.len().min(64);
                                                tracing::info!(target: "ws_market", "unparsed binary msg: {} bytes (head {} bytes)", bin.len(), sample_len);
                                            }
                                        }
                                        Some(Ok(Message::Ping(payload))) => { let _ = stream.send(Message::Pong(payload)).await; }
                                        Some(Ok(Message::Pong(_))) => { last_pong = std::time::Instant::now(); }
                                        Some(Ok(Message::Close(_))) => { tracing::warn!(target: "ws_market", "server closed"); break; }
                                        Some(Err(e)) => { tracing::warn!(target: "ws_market", "recv error: {}", e); break; }
                                        Some(Ok(_)) => {}
                                        None => { tracing::warn!(target: "ws_market", "stream ended"); break; }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(target: "ws_market", "connect failed: {}", e);
                    }
                }
                tracing::info!(target: "ws_market", "reconnecting in {:?}", backoff);
                sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        });

        *self._bg.lock().await = Some(handle);
        Ok(())
    }

    async fn next(&self) -> Result<Option<MarketEvent>> {
        let mut rx = self.rx.lock().await;
        Ok(rx.recv().await)
    }
}
