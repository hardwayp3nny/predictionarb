use crate::{model::MarketEvent, ports::MarketStream, ws_market::MarketWs};
use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::{sync::mpsc, task::JoinHandle};

#[derive(Clone)]
pub struct MarketMultiWs {
    url: String,
    max_per_conn: usize,
    tx: mpsc::Sender<MarketEvent>,
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<MarketEvent>>>,
    tasks: Arc<tokio::sync::Mutex<Vec<JoinHandle<()>>>>,
    children: Arc<tokio::sync::Mutex<Vec<MarketWs>>>,
}

impl MarketMultiWs {
    pub fn new(ws_url: &str, max_per_conn: usize) -> Result<Self> {
        let (tx, rx) = mpsc::channel(10_000);
        Ok(Self {
            url: ws_url.to_string(),
            max_per_conn: if max_per_conn == 0 { 500 } else { max_per_conn },
            tx,
            rx: Arc::new(tokio::sync::Mutex::new(rx)),
            tasks: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            children: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        })
    }

    fn partition(&self, mut ids: Vec<String>) -> Vec<Vec<String>> {
        // 去重，保持稳定顺序（首次出现保留）
        let mut seen = HashSet::new();
        ids.retain(|x| seen.insert(x.clone()));
        if ids.is_empty() {
            return vec![];
        }
        let mut out = Vec::new();
        let mut i = 0usize;
        while i < ids.len() {
            let end = (i + self.max_per_conn).min(ids.len());
            out.push(ids[i..end].to_vec());
            i = end;
        }
        out
    }
}

#[async_trait]
impl MarketStream for MarketMultiWs {
    async fn subscribe(&self, asset_ids: Vec<String>) -> Result<()> {
        // stop forwarders and close previous ws children
        {
            let mut tasks = self.tasks.lock().await;
            for h in tasks.drain(..) {
                h.abort();
            }
        }
        {
            let mut kids = self.children.lock().await;
            for ws in kids.drain(..) {
                let _ = ws.close().await;
            }
        }

        let parts = self.partition(asset_ids);
        if parts.is_empty() {
            return Ok(());
        }

        // 为每个分片创建一个子连接，并启动转发任务
        for chunk in parts.into_iter() {
            let ws = MarketWs::new(&self.url)?;
            ws.subscribe(chunk).await?;
            let mut_rx_tx = self.tx.clone();
            let ws_clone = ws.clone();
            let handle = tokio::spawn(async move {
                loop {
                    match ws_clone.next().await {
                        Ok(Some(ev)) => {
                            let _ = mut_rx_tx.send(ev).await;
                        }
                        Ok(None) => {
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        }
                        Err(e) => {
                            tracing::warn!(target: "ws_market_multi", "child next error: {}", e);
                            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                        }
                    }
                }
            });
            self.tasks.lock().await.push(handle);
            self.children.lock().await.push(ws);
        }
        Ok(())
    }

    async fn next(&self) -> Result<Option<MarketEvent>> {
        let mut rx = self.rx.lock().await;
        Ok(rx.recv().await)
    }
}
