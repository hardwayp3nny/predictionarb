use crate::http_pool::HttpPool;
use anyhow::{Context, Result};
use crate::pmrs as prs;
use prs::data::{CreateOrderOptions as RsCreateOrderOptions, ExtraOrderArgs, OrderArgs as RsOrderArgs, ApiCreds};
use alloy_signer_local::PrivateKeySigner;
use rust_decimal::Decimal;
use reqwest::header::HeaderMap;

pub struct OrderSigner {
    pub(crate) signer: Box<dyn prs::eth_utils::EthSigner>,
    chain_id: u64,
    pk_hex: String,
}

impl OrderSigner {
    pub fn from_private_key_hex(pk: &str, chain_id: u64) -> Result<Self> {
        let signer = Box::new(pk.parse::<PrivateKeySigner>()?);
        Ok(Self { signer, chain_id, pk_hex: pk.to_string() })
    }

    pub fn address_hex(&self) -> String {
        use alloy_primitives::hex::encode_prefixed;
        encode_prefixed(self.signer.address().as_slice())
    }
}

pub struct OrderBuilderRs {
    inner: prs::orders::OrderBuilder,
    chain_id: u64,
}

impl OrderBuilderRs {
    pub fn new_from_signer(order_signer: &OrderSigner) -> Self {
        let signer = Box::new(order_signer.pk_hex.parse::<PrivateKeySigner>().expect("invalid pk")) as Box<dyn prs::eth_utils::EthSigner>;
        let inner = prs::orders::OrderBuilder::new(signer, Some(prs::orders::SigType::PolyGnosisSafe), None);
        Self { inner, chain_id: order_signer.chain_id }
    }

    pub fn build_limit_order(&self, token_id: &str, side: crate::model::Side, price: f64, size: f64, tick_size: f64, neg_risk: bool, expiration: u64) -> Result<prs::orders::SignedOrderRequest> {
        let rs_args = RsOrderArgs::new(token_id, Decimal::from_f64_retain(price).context("price decimal")?, Decimal::from_f64_retain(size).context("size decimal")?, match side { crate::model::Side::Buy => prs::data::Side::BUY, crate::model::Side::Sell => prs::data::Side::SELL });
        let opts = RsCreateOrderOptions { tick_size: Some(Decimal::from_f64_retain(tick_size).context("tick size decimal")?), neg_risk: Some(neg_risk) };
        let extras = ExtraOrderArgs::default();
        let signed = self.inner.create_order(self.chain_id, &rs_args, expiration, &extras, opts)?;
        Ok(signed)
    }
}

pub async fn post_signed_order(pool: &HttpPool, api_creds: &ApiCreds, signer: &OrderSigner, signed: prs::orders::SignedOrderRequest, order_type: &str) -> Result<crate::model::OrderAck> {
    // Build L2 headers using polymarket-rs-client
    let headers_map = prs::headers::create_l2_headers::<serde_json::Value>(&*signer.signer, api_creds, "POST", "/order", None)?;
    let mut req_headers = HeaderMap::new();
    for (k, v) in headers_map { if let Ok(val) = reqwest::header::HeaderValue::from_str(&v) { if let Ok(name) = reqwest::header::HeaderName::from_bytes(k.as_bytes()) { req_headers.insert(name, val); } } }

    let post_body = serde_json::json!({
        "order": signed,
        "owner": api_creds.api_key,
        "orderType": order_type,
    });

    let resp = pool.post("/order", Some(req_headers), Some(&post_body)).await?;
    // Expect JSON with success/orderID
    if let Some(js) = resp.json {
        let success = js.get("success").and_then(|v| v.as_bool()).unwrap_or(false);
        let order_id = js.get("orderID").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let err = js.get("errorMsg").and_then(|v| v.as_str()).unwrap_or("").to_string();
        return Ok(crate::model::OrderAck{ success, error_message: if err.is_empty(){None}else{Some(err)}, order_id: if order_id.is_empty(){None}else{Some(order_id)} });
    }
    Ok(crate::model::OrderAck{ success: false, error_message: Some("non-json response".into()), order_id: None })
}
