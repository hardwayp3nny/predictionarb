use crate::model::*;
use async_trait::async_trait;

#[async_trait]
pub trait ExchangeClient: Send + Sync {
    async fn create_order(
        &self,
        args: OrderArgs,
        typ: OrderType,
        opts: CreateOrderOptions,
    ) -> anyhow::Result<OrderAck>;
    async fn create_orders_batch(
        &self,
        batch: Vec<(OrderArgs, OrderType, CreateOrderOptions)>,
    ) -> anyhow::Result<Vec<OrderAck>>;
    async fn cancel_orders(&self, ids: Vec<String>) -> anyhow::Result<CancelAck>;
    async fn get_orders(&self) -> anyhow::Result<Vec<OpenOrder>>;
}

// 新端口：通过 REST 同步订单（与 OMS 集成使用）
#[async_trait]
pub trait OrdersSnapshot: Send + Sync {
    async fn fetch_orders(&self) -> anyhow::Result<Vec<OpenOrder>>;
}

#[async_trait]
impl<T: ExchangeClient + ?Sized> OrdersSnapshot for T {
    async fn fetch_orders(&self) -> anyhow::Result<Vec<OpenOrder>> {
        self.get_orders().await
    }
}

// Position snapshot port
#[async_trait]
pub trait PositionsSnapshot: Send + Sync {
    async fn fetch_positions(&self) -> anyhow::Result<Vec<UserPosition>>;
}

#[async_trait]
pub trait BooksSnapshot: Send + Sync {
    async fn fetch_books(&self, token_ids: &[String]) -> anyhow::Result<Vec<BookSnapshot>>;
}

// no blanket impl; concrete exchanges should implement

#[async_trait]
pub trait MarketStream: Send + Sync {
    async fn subscribe(&self, asset_ids: Vec<String>) -> anyhow::Result<()>;
    async fn next(&self) -> anyhow::Result<Option<MarketEvent>>;
}

#[async_trait]
pub trait UserStream: Send + Sync {
    async fn connect(&self) -> anyhow::Result<()>;
    async fn next(&self) -> anyhow::Result<Option<UserEvent>>;
}

#[async_trait]
pub trait ActivityStream: Send + Sync {
    async fn connect(&self) -> anyhow::Result<()>;
    async fn next(&self) -> anyhow::Result<Option<ActivityEvent>>;
}

#[async_trait]
pub trait OrderStore: Send + Sync {
    async fn on_ack(&self, ack: &OrderAck);
    async fn on_user_event(&self, ev: &UserEvent);
}
