use crate::{model::*, ports::ExchangeClient};
use alloy_primitives::U256 as U256p;
use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Clone, Default)]
pub struct OrderManager {
    orders: Arc<DashMap<String, OpenOrder>>,
    stats: Arc<RwLock<EngineStats>>,
    position: Arc<RwLock<Option<UserPosition>>>,
}

impl OrderManager {
    fn status_is_terminal(status: &OrderStatus) -> bool {
        matches!(
            status,
            OrderStatus::Matched | OrderStatus::Rejected | OrderStatus::Cancelled
        )
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert_pending(&self, id: String, args: &OrderArgs) {
        let oo = OpenOrder {
            id: id.clone(),
            status: OrderStatus::PendingNew,
            market: args.token_id.clone(),
            size: args.size,
            price: args.price,
            side: args.side,
            size_matched: 0.0,
            asset_id: args.token_id.clone(),
            created_at: 0,
        };
        self.orders.insert(id, oo);
    }

    pub fn on_ack(&self, ack: &OrderAck) {
        if let Some(id) = ack.order_id.as_ref() {
            let mut remove_entry = false;
            if let Some(mut o) = self.orders.get_mut(id) {
                o.status = if ack.success {
                    OrderStatus::Live
                } else {
                    OrderStatus::Rejected
                };
                if !ack.success {
                    remove_entry = true;
                }
            }
            let mut s = self.stats.write();
            if ack.success {
                s.orders_succeeded += 1;
            } else {
                s.orders_failed += 1;
            }
            drop(s);
            if remove_entry {
                self.orders.remove(id);
            }
        }
    }

    pub fn on_user_event(&self, ev: &UserEvent) {
        match ev {
            UserEvent::Order(upd) => {
                let mut remove_entry = false;
                if let Some(mut o) = self.orders.get_mut(&upd.id) {
                    o.size_matched = upd.size_matched;
                    let st = upd.status.to_uppercase();
                    if st == "CANCELLED" || st == "CANCELED" {
                        o.status = OrderStatus::Cancelled;
                    } else if st == "REJECTED" {
                        o.status = OrderStatus::Rejected;
                    } else if st == "LIVE" {
                        o.status = OrderStatus::Live;
                    } else if st == "PENDING_NEW" {
                        o.status = OrderStatus::PendingNew;
                    }
                    if o.size_matched >= o.size {
                        o.status = OrderStatus::Matched;
                    }
                    if Self::status_is_terminal(&o.status) {
                        remove_entry = true;
                    }
                }
                if remove_entry {
                    self.orders.remove(&upd.id);
                }
            }
            UserEvent::Trade(_) => {}
        }
    }

    pub fn stats(&self) -> EngineStats {
        self.stats.read().clone()
    }

    pub fn upsert_snapshot(&self, order: OpenOrder) {
        self.orders.insert(order.id.clone(), order);
    }

    pub fn set_position(&self, pos: UserPosition) {
        *self.position.write() = Some(pos);
    }
    pub fn get_position(&self) -> Option<UserPosition> {
        self.position.read().clone()
    }

    pub fn list_by_asset(&self, asset_id: &str) -> Vec<OpenOrder> {
        let mut v = Vec::new();
        for e in self.orders.iter() {
            if e.value().asset_id == asset_id {
                v.push(e.value().clone());
            }
        }
        v
    }

    pub fn remove(&self, id: &str) -> Option<OpenOrder> {
        self.orders.remove(id).map(|(_, order)| order)
    }

    pub fn replace_id(&self, old_id: &str, new_id: &str) {
        if old_id == new_id {
            return;
        }
        if let Some((_, mut order)) = self.orders.remove(old_id) {
            order.id = new_id.to_string();
            self.orders.insert(new_id.to_string(), order);
        }
    }
}

#[async_trait]
pub trait OmsApi: Send + Sync {
    async fn submit_order(
        &self,
        args: OrderArgs,
        typ: OrderType,
        opts: CreateOrderOptions,
    ) -> Result<OrderAck>;

    async fn submit_orders_bulk(
        &self,
        batch: Vec<(OrderArgs, OrderType, CreateOrderOptions)>,
    ) -> Result<Vec<OrderAck>>;

    async fn cancel_orders(&self, ids: Vec<String>) -> Result<CancelAck>;

    fn on_user_event(&self, ev: &UserEvent);
}

pub struct Oms<E: ExchangeClient + 'static> {
    exchange: Arc<E>,
    store: OrderManager,
    limiter: Arc<Semaphore>,
    cancel_chunk: usize,
    token_id: String,
}

impl<E: ExchangeClient + 'static> Oms<E> {
    pub fn new_for_token(exchange: Arc<E>, token_id: String) -> Self {
        Self::with_config(exchange, token_id, num_cpus::get().max(4), 50)
    }

    pub fn with_config(
        exchange: Arc<E>,
        token_id: String,
        max_concurrency: usize,
        cancel_chunk: usize,
    ) -> Self {
        Self {
            exchange,
            store: OrderManager::new(),
            limiter: Arc::new(Semaphore::new(max_concurrency.max(1))),
            cancel_chunk: cancel_chunk.max(1),
            token_id,
        }
    }

    pub fn store(&self) -> &OrderManager {
        &self.store
    }
}

impl<E: ExchangeClient + 'static> Clone for Oms<E> {
    fn clone(&self) -> Self {
        Self {
            exchange: self.exchange.clone(),
            store: self.store.clone(),
            limiter: self.limiter.clone(),
            cancel_chunk: self.cancel_chunk,
            token_id: self.token_id.clone(),
        }
    }
}

#[async_trait]
impl<E: ExchangeClient + 'static> OmsApi for Oms<E> {
    async fn submit_order(
        &self,
        args: OrderArgs,
        typ: OrderType,
        opts: CreateOrderOptions,
    ) -> Result<OrderAck> {
        // enforce per-token OMS
        if args.token_id != self.token_id {
            return Err(anyhow::anyhow!(
                "token_id mismatch: expect {} got {}",
                self.token_id,
                args.token_id
            ));
        }
        let id_hint = new_order_id();
        self.store.upsert_pending(id_hint, &args);
        let ack = self.exchange.create_order(args, typ, opts).await?;
        self.store.on_ack(&ack);
        Ok(ack)
    }

    async fn submit_orders_bulk(
        &self,
        batch: Vec<(OrderArgs, OrderType, CreateOrderOptions)>,
    ) -> Result<Vec<OrderAck>> {
        use futures::stream::{FuturesUnordered, StreamExt};
        let mut futs = FuturesUnordered::new();
        for (args, typ, opts) in batch.into_iter() {
            if args.token_id != self.token_id {
                continue;
            }
            let sem = self.limiter.clone();
            let ex = self.exchange.clone();
            let store = self.store.clone();
            futs.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.expect("semaphore");
                let id_hint = new_order_id();
                store.upsert_pending(id_hint, &args);
                let res = ex.create_order(args, typ, opts).await;
                if let Ok(ref ack) = res {
                    store.on_ack(ack);
                }
                res
            }));
        }

        let mut out = Vec::new();
        while let Some(res) = futs.next().await {
            // unwrap join, leave Result<OrderAck>
            out.push(res.expect("join error")?);
        }
        Ok(out)
    }

    async fn cancel_orders(&self, ids: Vec<String>) -> Result<CancelAck> {
        if ids.is_empty() {
            return Ok(CancelAck {
                canceled: vec![],
                not_canceled: vec![],
            });
        }

        let mut canceled = Vec::new();
        let mut not_canceled = Vec::new();

        for chunk in ids.chunks(self.cancel_chunk) {
            let ack = self.exchange.cancel_orders(chunk.to_vec()).await?;
            canceled.extend(ack.canceled);
            not_canceled.extend(ack.not_canceled);
        }
        Ok(CancelAck {
            canceled,
            not_canceled,
        })
    }

    fn on_user_event(&self, ev: &UserEvent) {
        self.store.on_user_event(ev)
    }
}

// Helper: fetch USDC.e (6 decimals) balance via Polygon RPC
pub async fn fetch_usdce_balance(rpc_url: &str, wallet_addr: &str) -> anyhow::Result<f64> {
    let contract = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
    let addr_lc = if wallet_addr.starts_with("0x") {
        wallet_addr.to_lowercase()
    } else {
        format!("0x{}", wallet_addr.to_lowercase())
    };
    let addr_noprefix = addr_lc.trim_start_matches("0x");
    let data = format!("0x70a08231000000000000000000000000{}", addr_noprefix);
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [
            {"to": contract, "data": data},
            "latest"
        ]
    });

    // 直接使用 reqwest，不走内部 HttpPool；并兼容 wss:// → https://
    let url = if rpc_url.starts_with("wss://") {
        rpc_url.replacen("wss://", "https://", 1)
    } else if rpc_url.starts_with("ws://") {
        rpc_url.replacen("ws://", "http://", 1)
    } else {
        rpc_url.to_string()
    };

    let resp = reqwest::Client::new().post(&url).json(&body).send().await?;

    let js: serde_json::Value = resp.json().await?;
    if let Some(res) = js.get("result").and_then(|v| v.as_str()) {
        let hex = res.trim_start_matches("0x");
        if hex.is_empty() {
            return Ok(0.0);
        }
        if let Ok(v) = U256p::from_str_radix(hex, 16) {
            let s = v.to_string();
            if let Ok(intv) = s.parse::<f64>() {
                return Ok(intv / 1_000_000.0);
            }
        }
    }
    anyhow::bail!("eth_call balanceOf failed")
}
