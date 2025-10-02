use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum SigType {
    Eoa = 0,
    PolyProxy = 1,
    PolyGnosisSafe = 2,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignedOrderRequest {
    pub salt: u64,
    pub maker: String,
    pub signer: String,
    pub taker: String,
    pub token_id: String,
    pub maker_amount: String,
    pub taker_amount: String,
    pub expiration: String,
    pub nonce: String,
    pub fee_rate_bps: String,
    pub side: String,
    pub signature_type: u8,
    pub signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedOrderWithId {
    pub order: SignedOrderRequest,
    pub order_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CreateOrderOptionsRs {
    pub tick_size: Option<f64>,
    pub neg_risk: Option<bool>,
}
