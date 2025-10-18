use crate::model::{
    BookSnapshot, CancelAck, CreateOrderOptions, DepthUpdate, OpenOrder, OrderAck, OrderArgs,
    OrderBook, OrderStatus, OrderType, OrderUpdate, PublicTrade, TickSizeChange, TimestampMs,
    UserTrade,
};
use anyhow::Result;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::sync::Arc;

/// Raw configuration payload passed to strategies.
#[derive(Clone, Debug)]
pub struct StrategyConfig {
    raw: Value,
}

impl StrategyConfig {
    pub fn new(raw: Value) -> Self {
        Self { raw }
    }

    pub fn raw(&self) -> &Value {
        &self.raw
    }

    pub fn deserialize<T: DeserializeOwned>(&self) -> Result<T> {
        Ok(serde_json::from_value(self.raw.clone())?)
    }
}

/// `StrategyContext` exposes engine services to strategies during runtime.
#[derive(Clone)]
pub struct StrategyContext {
    counter: Arc<dyn Counter>,
}

impl StrategyContext {
    pub fn new(counter: Arc<dyn Counter>) -> Self {
        Self { counter }
    }

    pub fn counter(&self) -> Arc<dyn Counter> {
        self.counter.clone()
    }
}

/// Subscription / initialisation directives emitted by `Strategy::on_start`.
#[derive(Clone, Debug, Default)]
pub struct StartAction {
    pub subscribe_tokens: Vec<String>,
    pub subscribe_user_stream: bool,
    pub priming_requests: Vec<PrimingRequest>,
}

#[derive(Clone, Debug)]
pub enum PrimingRequest {
    SyncOpenOrders,
    SyncPositions,
    Custom(String),
}

/// Result passed to order callbacks.
pub type OrderResult = Result<OrderAck>;

pub type OrderCallback = Arc<dyn Fn(OrderResult) + Send + Sync + 'static>;

/// Result passed to cancel callbacks.
pub type CancelResult = Result<CancelAck>;

pub type CancelCallback = Arc<dyn Fn(CancelResult) + Send + Sync + 'static>;

#[derive(Clone, Debug)]
pub struct PlaceOrderRequest {
    pub args: OrderArgs,
    pub order_type: OrderType,
    pub options: CreateOrderOptions,
    pub client_order_id: Option<String>,
}

impl PlaceOrderRequest {
    pub fn new(args: OrderArgs, order_type: OrderType) -> Self {
        Self {
            args,
            order_type,
            options: CreateOrderOptions::default(),
            client_order_id: None,
        }
    }

    pub fn with_options(mut self, options: CreateOrderOptions) -> Self {
        self.options = options;
        self
    }

    pub fn with_client_order_id<S: Into<String>>(mut self, cid: S) -> Self {
        self.client_order_id = Some(cid.into());
        self
    }
}

#[derive(Clone, Debug)]
pub struct CancelOrderRequest {
    pub asset_id: String,
    pub order_ids: Vec<String>,
}

impl CancelOrderRequest {
    pub fn new(asset_id: String, order_ids: Vec<String>) -> Self {
        Self {
            asset_id,
            order_ids,
        }
    }
}

/// Strategy trait with synchronous data callbacks.
#[async_trait]
pub trait Strategy: Send + Sync {
    fn new(config: StrategyConfig) -> Result<Self>
    where
        Self: Sized;

    async fn on_start(&mut self, ctx: &StrategyContext) -> Result<StartAction>;

    fn on_order_book(&mut self, _event: &OrderBook) {}

    fn on_depth_update(&mut self, _event: &DepthUpdate) {}

    fn on_tick_size_update(&mut self, _event: &TickSizeChange) {}

    fn on_public_trade(&mut self, _event: &PublicTrade) {}

    fn on_order_update(&mut self, _event: &OrderUpdate) {}

    fn on_user_trade(&mut self, _event: &UserTrade) {}

    fn on_timer(&mut self, _now_ms: TimestampMs) {}
}

/// Execution interface exposed to strategies.
#[async_trait]
pub trait Counter: Send + Sync {
    async fn fetch_book(&self, asset_id: &str) -> Result<BookSnapshot>;

    async fn fetch_open_orders(&self, asset_id: Option<&str>) -> Result<Vec<OpenOrder>>;

    fn place_order(&self, request: PlaceOrderRequest, callback: OrderCallback) -> Result<()>;

    fn cancel_orders(&self, request: CancelOrderRequest, callback: CancelCallback) -> Result<()>;

    fn now_ms(&self) -> TimestampMs;
}

/// Convenience counter implementation that panics on use.
#[derive(Clone, Default)]
pub struct NullCounter;

#[async_trait]
impl Counter for NullCounter {
    async fn fetch_book(&self, _asset_id: &str) -> Result<BookSnapshot> {
        panic!("NullCounter should not be used");
    }

    async fn fetch_open_orders(&self, _asset_id: Option<&str>) -> Result<Vec<OpenOrder>> {
        panic!("NullCounter should not be used");
    }

    fn place_order(&self, _request: PlaceOrderRequest, _callback: OrderCallback) -> Result<()> {
        panic!("NullCounter should not be used");
    }

    fn cancel_orders(&self, _request: CancelOrderRequest, _callback: CancelCallback) -> Result<()> {
        panic!("NullCounter should not be used");
    }

    fn now_ms(&self) -> TimestampMs {
        panic!("NullCounter should not be used");
    }
}

/// Helper for translating `OrderUpdate` status into enums.
pub fn parse_order_status(status: &str) -> OrderStatus {
    match status.to_uppercase().as_str() {
        "LIVE" => OrderStatus::Live,
        "PENDING_NEW" => OrderStatus::PendingNew,
        "MATCHED" => OrderStatus::Matched,
        "CANCELLED" | "CANCELED" => OrderStatus::Cancelled,
        "REJECTED" => OrderStatus::Rejected,
        _ => OrderStatus::Submitting,
    }
}
