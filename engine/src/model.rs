use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use uuid::Uuid;

pub type TimestampMs = i64;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderType {
    Gtc,
    Fok,
    Fak,
    Gtd,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderStatus {
    PendingNew,
    Submitting,
    Live,
    Matched,
    Rejected,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderArgs {
    pub token_id: String,
    pub price: f64,
    pub size: f64,
    pub side: Side,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CreateOrderOptions {
    pub tick_size: Option<f64>,
    pub neg_risk: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderAck {
    pub success: bool,
    pub error_message: Option<String>,
    pub order_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelAck {
    pub canceled: Vec<String>,
    pub not_canceled: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenOrder {
    pub id: String,
    pub status: OrderStatus,
    pub market: String,
    pub size: f64,
    pub price: f64,
    pub side: Side,
    pub size_matched: f64,
    pub asset_id: String,
    pub created_at: TimestampMs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Level {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBook {
    pub event_type: String,
    pub asset_id: String,
    pub market: String,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub timestamp: TimestampMs,
    pub hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookSnapshot {
    pub order_book: OrderBook,
    pub tick_size: f64,
    pub min_order_size: Option<f64>,
    pub neg_risk: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceChange {
    pub asset_id: String,
    pub price: f64,
    pub size: f64,
    pub side: Side,
    pub hash: String,
    pub best_bid: f64,
    pub best_ask: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthUpdate {
    pub event_type: String,
    pub market: String,
    pub price_changes: Vec<PriceChange>,
    pub timestamp: TimestampMs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickSizeChange {
    pub event_type: String,
    pub asset_id: String,
    pub market: String,
    pub old_tick_size: f64,
    pub new_tick_size: f64,
    pub timestamp: TimestampMs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicTrade {
    pub event_type: String,
    pub asset_id: String,
    pub market: String,
    pub price: f64,
    pub side: Side,
    pub size: f64,
    pub timestamp: TimestampMs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserTrade {
    pub event_type: String,
    pub id: String,
    pub asset_id: String,
    pub market: String,
    pub price: f64,
    pub side: Side,
    pub size: f64,
    pub status: String,
    pub timestamp: TimestampMs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderUpdate {
    pub event_type: String,
    pub id: String,
    pub asset_id: String,
    pub market: String,
    pub price: f64,
    pub side: Side,
    pub size_matched: f64,
    pub status: String,
    pub timestamp: TimestampMs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum MarketEvent {
    OrderBook(OrderBook),
    DepthUpdate(DepthUpdate),
    TickSizeChange(TickSizeChange),
    PublicTrade(PublicTrade),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum UserEvent {
    Order(OrderUpdate),
    Trade(UserTrade),
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EngineStats {
    pub orders_sent: u64,
    pub orders_succeeded: u64,
    pub orders_failed: u64,
    pub ws_events: u64,
}

pub fn new_order_id() -> String {
    Uuid::new_v4().to_string()
}

// -------------------------
// User positions
// -------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserPosition {
    pub asset_id: String,
    pub size: f64,
}
