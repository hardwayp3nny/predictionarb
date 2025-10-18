use crate::{
    metrics::Metrics,
    model::{BookSnapshot, OpenOrder, OrderAck},
    oms::Oms,
    ports::{BooksSnapshot, ExchangeClient, OrdersSnapshot},
};
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use engine_core::strategy::{
    CancelCallback, CancelOrderRequest, Counter, OrderCallback, PlaceOrderRequest,
};
use std::sync::Arc;

pub struct EngineCounter<R>
where
    R: ExchangeClient + BooksSnapshot + OrdersSnapshot + Send + Sync + 'static,
{
    exchange: Arc<R>,
    metrics: Arc<Metrics>,
    oms: Arc<DashMap<String, Oms<R>>>,
}

impl<R> EngineCounter<R>
where
    R: ExchangeClient + BooksSnapshot + OrdersSnapshot + Send + Sync + 'static,
{
    pub fn new(exchange: Arc<R>, metrics: Arc<Metrics>, oms: Arc<DashMap<String, Oms<R>>>) -> Self {
        Self {
            exchange,
            metrics,
            oms,
        }
    }

    fn ensure_oms(&self, token_id: &str) -> Oms<R> {
        if let Some(oms) = self.oms.get(token_id) {
            return oms.clone();
        }
        let oms = Oms::new_for_token(self.exchange.clone(), token_id.to_string());
        self.oms.insert(token_id.to_string(), oms.clone());
        oms
    }

    async fn place_order_inner(&self, request: PlaceOrderRequest) -> Result<OrderAck> {
        let batch = vec![(
            request.args.clone(),
            request.order_type,
            request.options.clone(),
        )];
        let mut response = self.exchange.create_orders_batch(batch).await?;
        let ack = response.remove(0);
        if let Some(order_id) = ack.order_id.clone() {
            let oms = self.ensure_oms(&request.args.token_id);
            oms.store().upsert_pending(order_id.clone(), &request.args);
            oms.store().on_ack(&ack);
        }
        Ok(ack)
    }

    async fn fetch_book_snapshot(&self, asset_id: &str) -> Result<BookSnapshot> {
        let mut books = self.exchange.fetch_books(&[asset_id.to_string()]).await?;
        books
            .pop()
            .ok_or_else(|| anyhow!("no snapshot returned for token {}", asset_id))
    }
}

#[async_trait::async_trait]
impl<R> Counter for EngineCounter<R>
where
    R: ExchangeClient + BooksSnapshot + OrdersSnapshot + Send + Sync + 'static,
{
    async fn fetch_book(&self, asset_id: &str) -> Result<BookSnapshot> {
        self.fetch_book_snapshot(asset_id).await
    }

    async fn fetch_open_orders(&self, asset_id: Option<&str>) -> Result<Vec<OpenOrder>> {
        if let Some(token) = asset_id {
            let oms = self.ensure_oms(token);
            return Ok(oms.store().list_by_asset(token));
        }
        let mut all = Vec::new();
        for entry in self.oms.iter() {
            all.extend(entry.value().store().list_by_asset(&entry.key()));
        }
        Ok(all)
    }

    fn place_order(&self, request: PlaceOrderRequest, callback: OrderCallback) -> Result<()> {
        let this = self.clone();
        tokio::spawn(async move {
            this.metrics.orders_sent.inc();
            this.metrics.inflight_orders.inc();
            let result = this.place_order_inner(request).await;
            this.metrics.inflight_orders.dec();
            (callback)(result);
        });
        Ok(())
    }

    fn cancel_orders(&self, request: CancelOrderRequest, callback: CancelCallback) -> Result<()> {
        let exchange = self.exchange.clone();
        tokio::spawn(async move {
            let result = exchange.cancel_orders(request.order_ids).await;
            (callback)(result);
        });
        Ok(())
    }

    fn now_ms(&self) -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

impl<R> Clone for EngineCounter<R>
where
    R: ExchangeClient + BooksSnapshot + OrdersSnapshot + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            exchange: self.exchange.clone(),
            metrics: self.metrics.clone(),
            oms: self.oms.clone(),
        }
    }
}
