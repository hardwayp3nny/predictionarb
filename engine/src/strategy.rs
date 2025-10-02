use crate::model::*;
use async_trait::async_trait;

#[async_trait]
pub trait Strategy: Send + Sync {
    async fn on_start(&self) -> Vec<String>;

    async fn on_order_book(&self, _event: OrderBook) {}
    async fn on_depth_update(&self, _event: DepthUpdate) {}
    async fn on_tick_size_change(&self, _event: TickSizeChange) {}
    async fn on_public_trade(&self, _event: PublicTrade) {}
    async fn on_order_update(&self, _event: OrderUpdate) {}
    async fn on_user_trade(&self, _event: UserTrade) {}
    async fn on_signal(&self, _signal: serde_json::Value) {}
}
