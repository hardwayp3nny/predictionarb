use crate::{
    auth::{create_l2_headers, ApiCreds},
    http_pool::HttpPool,
    model::*,
    order_builder::{build_signed_order, PreparedOrder},
    order_types::{CreateOrderOptionsRs, SigType},
    ports::{BooksSnapshot, ExchangeClient},
};
use alloy_primitives::U256;
use alloy_signer_local::PrivateKeySigner;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use serde_json::{json, Value};

use std::collections::HashSet;
use std::sync::Arc;
use urlencoding::encode;

fn parse_f64_field(value: Option<&Value>) -> Result<f64> {
    let val = value.ok_or_else(|| anyhow!("missing number field"))?;
    match val {
        Value::String(s) => s
            .parse::<f64>()
            .with_context(|| format!("parse f64 from string '{}'", s)),
        Value::Number(n) => n.as_f64().ok_or_else(|| anyhow!("invalid number")),
        _ => Err(anyhow!("unexpected type for number field")),
    }
}

fn parse_i64_field(value: Option<&Value>) -> Result<i64> {
    let val = value.ok_or_else(|| anyhow!("missing integer field"))?;
    match val {
        Value::String(s) => s
            .parse::<i64>()
            .with_context(|| format!("parse i64 from string '{}'", s)),
        Value::Number(n) => n.as_i64().ok_or_else(|| anyhow!("invalid integer")),
        _ => Err(anyhow!("unexpected type for integer field")),
    }
}

fn sort_levels(levels: &mut Vec<Level>, is_bid: bool) {
    if is_bid {
        levels.sort_by(|a, b| {
            b.price
                .partial_cmp(&a.price)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    } else {
        levels.sort_by(|a, b| {
            a.price
                .partial_cmp(&b.price)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }
}

fn parse_levels(value: Option<&Value>, is_bid: bool) -> Result<Vec<Level>> {
    let arr = value
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("levels not array"))?;
    let mut out = Vec::with_capacity(arr.len());
    for (idx, lvl_val) in arr.iter().enumerate() {
        if let Some(obj) = lvl_val.as_object() {
            let price = parse_f64_field(obj.get("price"))
                .with_context(|| format!("parse price at index {}", idx))?;
            let size = parse_f64_field(obj.get("size"))
                .with_context(|| format!("parse size at index {}", idx))?;
            out.push(Level { price, size });
        } else if let Some(list) = lvl_val.as_array() {
            if list.len() >= 2 {
                let price = parse_f64_field(list.get(0))
                    .with_context(|| format!("parse price list at index {}", idx))?;
                let size = parse_f64_field(list.get(1))
                    .with_context(|| format!("parse size list at index {}", idx))?;
                out.push(Level { price, size });
            }
        }
    }
    sort_levels(&mut out, is_bid);
    Ok(out)
}

#[derive(Clone)]
pub struct PolymarketHttpExchange {
    pool: Arc<HttpPool>,
    signer: Arc<PrivateKeySigner>,
    creds: Arc<ApiCreds>,
    chain_id: u64,
    maker_override: Option<String>,
}

impl PolymarketHttpExchange {
    pub fn new(
        pool: Arc<HttpPool>,
        signer: PrivateKeySigner,
        creds: ApiCreds,
        chain_id: u64,
        maker_override: Option<String>,
    ) -> Self {
        Self {
            pool,
            signer: Arc::new(signer),
            creds: Arc::new(creds),
            chain_id,
            maker_override,
        }
    }

    fn map_order_type(t: OrderType) -> (&'static str, &'static str) {
        match t {
            OrderType::Gtc => ("GTC", "GTC"),
            OrderType::Fok => ("FOK", "FOK"),
            OrderType::Fak => ("FAK", "FAK"),
            OrderType::Gtd => ("GTD", "GTD"),
        }
    }

    fn build_body(
        &self,
        args: &OrderArgs,
        typ: OrderType,
        opts: &CreateOrderOptions,
    ) -> Result<(Value, String)> {
        let (tif, order_type) = Self::map_order_type(typ);
        let maker = self.maker_override.as_deref();
        let with_id = build_signed_order(
            &self.signer,
            self.chain_id,
            &args.token_id,
            args.side,
            args.price,
            args.size,
            CreateOrderOptionsRs {
                tick_size: opts.tick_size,
                neg_risk: opts.neg_risk,
            },
            0,
            0,
            U256::from(0u8),
            "0x0000000000000000000000000000000000000000",
            maker,
            SigType::PolyGnosisSafe,
        )?;
        let order_id = with_id.order_id.clone();
        let signed = with_id.order;
        let body = serde_json::json!({
            "order": signed,
            "owner": self.creds.api_key,
            "orderType": order_type,
            "tif": tif,
        });
        Ok((body, order_id))
    }

    pub async fn fetch_books(&self, token_ids: &[String]) -> Result<Vec<BookSnapshot>> {
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        let body = json!(token_ids
            .iter()
            .map(|id| json!({ "token_id": id }))
            .collect::<Vec<_>>());
        let resp = self
            .pool
            .post("/books", None, Some(&body))
            .await
            .context("post /books")?;
        let items = resp
            .json
            .as_ref()
            .and_then(|v| v.as_array())
            .cloned()
            .ok_or_else(|| anyhow!("/books response is not array"))?;

        let mut out = Vec::with_capacity(items.len());
        for (idx, item) in items.into_iter().enumerate() {
            let obj = item
                .as_object()
                .ok_or_else(|| anyhow!("/books[{}] not object", idx))?;
            let asset_id = obj
                .get("asset_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("missing asset_id in /books[{}]", idx))?
                .to_string();
            let market = obj
                .get("market")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("missing market in /books[{}]", idx))?
                .to_string();
            let timestamp = parse_i64_field(obj.get("timestamp"))
                .with_context(|| format!("parse timestamp for {}", asset_id))?;
            let hash = obj
                .get("hash")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let bids = parse_levels(obj.get("bids"), true)
                .with_context(|| format!("parse bids for {}", asset_id))?;
            let asks = parse_levels(obj.get("asks"), false)
                .with_context(|| format!("parse asks for {}", asset_id))?;
            let tick_size = parse_f64_field(obj.get("tick_size"))
                .with_context(|| format!("parse tick_size for {}", asset_id))?;
            let min_order_size = match obj.get("min_order_size") {
                Some(v) => Some(
                    parse_f64_field(Some(v))
                        .with_context(|| format!("parse min_order_size for {}", asset_id))?,
                ),
                None => None,
            };
            let neg_risk = obj
                .get("neg_risk")
                .and_then(|v| v.as_bool())
                .or_else(|| obj.get("negRisk").and_then(|v| v.as_bool()));

            let mut order_book = OrderBook {
                event_type: "book".into(),
                asset_id,
                market,
                bids,
                asks,
                timestamp,
                hash,
            };
            sort_levels(&mut order_book.bids, true);
            sort_levels(&mut order_book.asks, false);

            out.push(BookSnapshot {
                order_book,
                tick_size,
                min_order_size,
                neg_risk,
            });
        }
        Ok(out)
    }

    pub async fn submit_prepared_order(&self, prepared: &PreparedOrder) -> Result<OrderAck> {
        let (tif, order_type) = Self::map_order_type(prepared.order_type);
        let body = json!({
            "order": prepared.signed.order.clone(),
            "owner": self.creds.api_key.clone(),
            "orderType": order_type,
            "tif": tif,
        });
        let headers = create_l2_headers(&self.signer, &self.creds, "POST", "/order", Some(&body))?;
        let resp = self.pool.post("/order", Some(headers), Some(&body)).await?;
        let mut ack = OrderAck {
            success: false,
            error_message: None,
            order_id: Some(prepared.signed.order_id.clone()),
        };
        if let Some(js) = resp.json {
            ack.success = js.get("success").and_then(|v| v.as_bool()).unwrap_or(false);
            if let Some(err) = js
                .get("error")
                .or_else(|| js.get("errorMsg"))
                .and_then(|v| v.as_str())
            {
                ack.error_message = Some(err.to_string());
            }
            if let Some(order_id) = js.get("orderID").and_then(|v| v.as_str()) {
                ack.order_id = Some(order_id.to_string());
            }
        } else if let Some(text) = resp.text {
            ack.error_message = Some(text);
        }
        Ok(ack)
    }
}

#[async_trait]
impl ExchangeClient for PolymarketHttpExchange {
    async fn create_order(
        &self,
        args: OrderArgs,
        typ: OrderType,
        opts: CreateOrderOptions,
    ) -> Result<OrderAck> {
        let (body, pre_id) = self.build_body(&args, typ, &opts)?;
        let headers = create_l2_headers(&self.signer, &self.creds, "POST", "/order", Some(&body))?;
        let resp = self.pool.post("/order", Some(headers), Some(&body)).await?;
        let mut ack = OrderAck {
            success: false,
            error_message: None,
            order_id: Some(pre_id),
        };
        if let Some(js) = resp.json.as_ref() {
            let suc = js.get("success").and_then(|v| v.as_bool()).unwrap_or(false);
            let err = js
                .get("error")
                .or_else(|| js.get("errorMsg"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let oid = js
                .get("orderID")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            ack.success = suc;
            ack.error_message = err;
            if oid.is_some() {
                ack.order_id = oid;
            }
        } else if let Some(txt) = resp.text.as_ref() {
            ack.error_message = Some(txt.clone());
        }
        Ok(ack)
    }

    async fn cancel_orders(&self, ids: Vec<String>) -> Result<CancelAck> {
        let body = serde_json::json!(ids);
        let headers =
            create_l2_headers(&self.signer, &self.creds, "DELETE", "/orders", Some(&body))?;
        let resp = self
            .pool
            .delete("/orders", Some(headers), Some(&body))
            .await?;
        let js = resp.json.unwrap_or(serde_json::json!({}));

        let mut canceled: Vec<String> = vec![];
        if let Some(arr) = js
            .get("canceled")
            .and_then(|v| v.as_array())
            .or_else(|| js.get("cancelled").and_then(|v| v.as_array()))
        {
            for v in arr {
                if let Some(s) = v.as_str() {
                    canceled.push(s.to_string());
                } else if let Some(o) = v.as_object() {
                    if let Some(id) = o.get("id").and_then(|x| x.as_str()) {
                        canceled.push(id.to_string());
                    }
                }
            }
        }

        let mut not_canceled: Vec<(String, String)> = vec![];
        if let Some(arr) = js
            .get("not_canceled")
            .and_then(|v| v.as_array())
            .or_else(|| js.get("notCanceled").and_then(|v| v.as_array()))
        {
            for v in arr {
                if let Some(pair) = v.as_array() {
                    if pair.len() >= 2 {
                        let id = pair[0].as_str().unwrap_or("").to_string();
                        let reason = pair[1].as_str().unwrap_or("").to_string();
                        not_canceled.push((id, reason));
                    }
                } else if let Some(o) = v.as_object() {
                    let id = o
                        .get("id")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string();
                    let reason = o
                        .get("error")
                        .and_then(|x| x.as_str())
                        .or_else(|| o.get("reason").and_then(|x| x.as_str()))
                        .or_else(|| o.get("message").and_then(|x| x.as_str()))
                        .unwrap_or("")
                        .to_string();
                    if !id.is_empty() {
                        not_canceled.push((id, reason));
                    }
                }
            }
        }
        Ok(CancelAck {
            canceled,
            not_canceled,
        })
    }

    async fn create_orders_batch(
        &self,
        batch: Vec<(OrderArgs, OrderType, CreateOrderOptions)>,
    ) -> Result<Vec<OrderAck>> {
        use serde_json::json;
        // Build bodies and precomputed ids
        let mut items: Vec<Value> = Vec::new();
        let mut pre_ids: Vec<String> = Vec::new();
        for (args, typ, opts) in batch.iter() {
            let (body, pre_id) = self.build_body(args, *typ, opts)?;
            items.push(body);
            pre_ids.push(pre_id);
        }
        // POST /orders
        let headers = create_l2_headers(
            &self.signer,
            &self.creds,
            "POST",
            "/orders",
            Some(&json!(items)),
        )?;
        let resp = self
            .pool
            .post("/orders", Some(headers), Some(&json!(items)))
            .await?;

        let mut out: Vec<OrderAck> = Vec::new();
        if let Some(js) = resp.json {
            // Expect array of results; fall back to object with "data"
            let arr = if let Some(a) = js.as_array() {
                a.clone()
            } else {
                js.get("data")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default()
            };
            for (i, r) in arr.iter().enumerate() {
                let mut ack = OrderAck {
                    success: false,
                    error_message: None,
                    order_id: Some(pre_ids.get(i).cloned().unwrap_or_default()),
                };
                ack.success = r.get("success").and_then(|v| v.as_bool()).unwrap_or(false);
                if let Some(oid) = r.get("orderID").and_then(|v| v.as_str()) {
                    ack.order_id = Some(oid.to_string());
                }
                let err = r
                    .get("error")
                    .or_else(|| r.get("errorMsg"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                ack.error_message = err;
                out.push(ack);
            }
        } else if let Some(txt) = resp.text.as_ref() {
            // non-json fallback: mark all failed with same message
            for pre in pre_ids.iter().cloned() {
                out.push(OrderAck {
                    success: false,
                    error_message: Some(txt.clone()),
                    order_id: Some(pre),
                });
            }
        }
        // If response shorter than batch, pad with failures
        while out.len() < items.len() {
            let pre = pre_ids.get(out.len()).cloned().unwrap_or_default();
            out.push(OrderAck {
                success: false,
                error_message: Some("missing result".into()),
                order_id: Some(pre),
            });
        }
        Ok(out)
    }

    async fn get_orders(&self) -> Result<Vec<OpenOrder>> {
        let mut out = Vec::new();
        let mut seen = HashSet::<String>::new();
        let mut cursor: Option<String> = None;

        loop {
            let path = if let Some(cur) = cursor.as_ref() {
                format!("/data/orders?next_cursor={}&geo_block_token=", encode(cur))
            } else {
                "/data/orders".to_string()
            };

            let headers = create_l2_headers(&self.signer, &self.creds, "GET", &path, None)?;
            let resp = self.pool.get(&path, Some(headers)).await?;
            let mut page_added = 0usize;

            if let Some(js) = resp.json {
                let arr = js
                    .get("data")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                for it in arr.into_iter() {
                    let id = it
                        .get("id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    if id.is_empty() || !seen.insert(id.clone()) {
                        continue;
                    }
                    let asset_id = it
                        .get("asset_id")
                        .and_then(|v| v.as_str())
                        .or_else(|| it.get("market").and_then(|v| v.as_str()))
                        .unwrap_or("")
                        .to_string();
                    let market = it
                        .get("market")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let price = it
                        .get("price")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .or_else(|| it.get("price").and_then(|v| v.as_f64()))
                        .unwrap_or(0.0);
                    let size = it
                        .get("original_size")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .or_else(|| {
                            it.get("size")
                                .and_then(|v| v.as_str())
                                .and_then(|s| s.parse::<f64>().ok())
                        })
                        .or_else(|| it.get("original_size").and_then(|v| v.as_f64()))
                        .or_else(|| it.get("size").and_then(|v| v.as_f64()))
                        .unwrap_or(0.0);
                    let size_matched = it
                        .get("size_matched")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .or_else(|| it.get("size_matched").and_then(|v| v.as_f64()))
                        .unwrap_or(0.0);
                    let side_s = it
                        .get("side")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_uppercase();
                    let side = if side_s == "BUY" {
                        Side::Buy
                    } else {
                        Side::Sell
                    };
                    let status_s = it
                        .get("status")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_uppercase();
                    let status = match status_s.as_str() {
                        "LIVE" => OrderStatus::Live,
                        "MATCHED" => OrderStatus::Matched,
                        "PENDING_NEW" => OrderStatus::PendingNew,
                        "CANCELLED" | "CANCELED" => OrderStatus::Cancelled,
                        "REJECTED" => OrderStatus::Rejected,
                        _ => OrderStatus::Live,
                    };
                    let created_at = it.get("created_at").and_then(|v| v.as_i64()).unwrap_or(0);
                    out.push(OpenOrder {
                        id,
                        status,
                        market,
                        size,
                        price,
                        side,
                        size_matched,
                        asset_id,
                        created_at,
                    });
                    page_added += 1;
                }

                cursor = js
                    .get("next_cursor")
                    .and_then(|v| v.as_str())
                    .and_then(|s| {
                        if s.is_empty() || s == "LTE=" {
                            None
                        } else {
                            Some(s.to_string())
                        }
                    });
            } else {
                cursor = None;
            }

            if page_added == 0 || cursor.is_none() {
                break;
            }
        }
        Ok(out)
    }
}

// Extend with positions snapshot via data-api
#[async_trait]
impl crate::ports::PositionsSnapshot for PolymarketHttpExchange {
    async fn fetch_positions(&self) -> Result<Vec<UserPosition>> {
        // prefer maker_override(Safe/browser address) else signer address
        let addr = if let Some(a) = self.maker_override.as_ref() {
            a.clone()
        } else {
            use alloy_primitives::hex::encode_prefixed;
            encode_prefixed(self.signer.address().as_slice())
        };
        // Use data-api positions endpoint with explicit pagination/sorting params
        let url = format!(
            "https://data-api.polymarket.com/positions?user={}&sizeThreshold=.1&limit=500&offset=0&sortBy=PRICE&sortDirection=DESC",
            addr
        );
        let resp = self.pool.get(&url, None).await?;
        let mut out = Vec::new();
        let arr = if let Some(js) = resp.json.as_ref() {
            js.as_array().cloned().unwrap_or_else(|| {
                js.get("data")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default()
            })
        } else {
            vec![]
        };
        for p in arr.into_iter() {
            // asset id
            let asset_id = p
                .get("asset")
                .and_then(|v| v.as_str())
                .or_else(|| p.get("asset_id").and_then(|v| v.as_str()))
                .or_else(|| p.get("token_id").and_then(|v| v.as_str()))
                .or_else(|| p.get("tokenId").and_then(|v| v.as_str()))
                .or_else(|| p.get("market").and_then(|v| v.as_str()))
                .unwrap_or("")
                .to_string();
            if asset_id.is_empty() {
                continue;
            }
            // size: try various fields commonly seen
            let size = p
                .get("size")
                .and_then(|v| v.as_f64())
                .or_else(|| {
                    p.get("size")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                })
                .or_else(|| {
                    p.get("net_position")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                })
                .or_else(|| p.get("netPosition").and_then(|v| v.as_f64()))
                .or_else(|| p.get("position").and_then(|v| v.as_f64()))
                .or_else(|| {
                    p.get("balance")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                })
                .or_else(|| p.get("amount").and_then(|v| v.as_f64()))
                .unwrap_or(0.0);
            let current_value = p
                .get("currentValue")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            // optional: parse currentValue if present for later portfolio computation
            // let current_value = p.get("currentValue").and_then(|v| v.as_f64());
            out.push(UserPosition { asset_id, size });
            // We don't change struct here to avoid ripple; currentValue will be used by caller via raw json if needed
        }
        Ok(out)
    }
}

#[async_trait]
impl BooksSnapshot for PolymarketHttpExchange {
    async fn fetch_books(&self, token_ids: &[String]) -> Result<Vec<BookSnapshot>> {
        self.fetch_books(token_ids).await
    }
}

impl PolymarketHttpExchange {
    fn wallet_address(&self) -> String {
        if let Some(a) = self.maker_override.as_ref() {
            a.clone()
        } else {
            use alloy_primitives::hex::encode_prefixed;
            encode_prefixed(self.signer.address().as_slice())
        }
    }

    pub async fn fetch_positions_detail(&self) -> Result<Vec<PositionInfo>> {
        let addr = self.wallet_address();
        let url = format!(
            "https://data-api.polymarket.com/positions?user={}&sizeThreshold=.1&limit=500&offset=0&sortBy=CURRENT&sortDirection=DESC",
            addr
        );
        let resp = self.pool.get(&url, None).await?;
        let items = resp
            .json
            .as_ref()
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut out = Vec::with_capacity(items.len());
        for item in items.into_iter() {
            let info: PositionInfo = serde_json::from_value(item)?;
            out.push(info);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EngineConfig;
    use prometheus::Registry;
    use std::str::FromStr;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires network access to Polymarket /books endpoint"]
    async fn fetch_books_prints_snapshots() {
        let mut cfg = EngineConfig::default();
        cfg.http.timeout_ms = 15_000;
        let registry = Registry::new();
        let pool = HttpPool::new(&cfg, &registry).expect("http pool");
        let signer = PrivateKeySigner::from_str(
            "0x59c6995e998f97a5a0044966f0945384214f54d4d0f5e36c4a3b1b6ce6b265a3",
        )
        .expect("signer");
        let creds = ApiCreds {
            api_key: String::new(),
            secret: String::new(),
            passphrase: String::new(),
        };
        let exchange = PolymarketHttpExchange::new(Arc::new(pool), signer, creds, 137, None);
        let token_ids = vec![
            "42334954850219754195241248003172889699504912694714162671145392673031415571339"
                .to_string(),
            "293710927830619537076255809082647305282099747736151586082571069419008202233"
                .to_string(),
        ];
        let books = exchange.fetch_books(&token_ids).await.expect("fetch books");
        for book in &books {
            println!("Fetched book: {:?}", book);
        }
        assert_eq!(books.len(), token_ids.len());
        let mut ids = books
            .iter()
            .map(|b| b.order_book.asset_id.clone())
            .collect::<Vec<_>>();
        ids.sort();
        let mut expected = token_ids.clone();
        expected.sort();
        assert_eq!(ids, expected);
        for snapshot in &books {
            assert!(
                !snapshot.order_book.bids.is_empty() || !snapshot.order_book.asks.is_empty(),
                "{} has empty book",
                snapshot.order_book.asset_id
            );
        }
    }
}
