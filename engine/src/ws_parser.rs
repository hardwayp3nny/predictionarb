use crate::model::*;
use serde::Deserialize;
use serde_json::Value;

// 轻量中间体，尽量减少复制与转换
#[derive(Debug, Deserialize)]
struct BookLevel {
    price: String,
    size: String,
}

#[derive(Debug, Deserialize)]
struct BookMsg {
    event_type: String,
    asset_id: String,
    market: String,
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
    #[serde(default)]
    timestamp: i64,
    #[serde(default)]
    hash: String,
}

#[derive(Debug, Deserialize)]
struct PriceChangeInner {
    asset_id: String,
    price: String,
    size: String,
    side: String,
    hash: String,
    best_bid: String,
    best_ask: String,
}

#[derive(Debug, Deserialize)]
struct DepthMsg {
    event_type: String,
    market: String,
    #[serde(rename = "price_changes")]
    price_changes: Vec<PriceChangeInner>,
    #[serde(default)]
    timestamp: i64,
}

#[derive(Debug, Deserialize)]
struct TickSizeMsg {
    event_type: String,
    asset_id: String,
    market: String,
    old_tick_size: String,
    new_tick_size: String,
    #[serde(default)]
    timestamp: i64,
}

#[derive(Debug, Deserialize)]
struct TradeMsg {
    event_type: String,
    asset_id: String,
    market: String,
    price: String,
    side: String,
    size: String,
    #[serde(default)]
    timestamp: i64,
}

#[inline]
fn parse_side(s: &str) -> Side {
    match s {
        "BUY" | "bid" | "Bid" | "Buy" => Side::Buy,
        _ => Side::Sell,
    }
}

#[inline]
fn normalize_num_str(s: &str) -> String {
    if s.starts_with("-.") {
        let mut t = String::from("-0");
        t.push_str(&s[1..]);
        return t;
    }
    if s.starts_with('.') {
        let mut t = String::from("0");
        t.push_str(s);
        return t;
    }
    s.to_string()
}

#[inline]
fn p(s: &str) -> f64 {
    normalize_num_str(s).parse().unwrap_or(0.0)
}

pub fn parse_market_bytes(bytes: &[u8]) -> Option<Vec<MarketEvent>> {
    // 消息可能是对象或数组
    if bytes.is_empty() {
        return None;
    }
    match bytes[0] {
        b'{' => parse_one(bytes).map(|e| vec![e]),
        b'[' => parse_many(bytes),
        _ => None,
    }
}

fn parse_many(bytes: &[u8]) -> Option<Vec<MarketEvent>> {
    let vals: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    let arr = vals.as_array()?;
    let mut out = Vec::with_capacity(arr.len());
    for v in arr {
        if let Some(obj) = v.as_object() {
            if let Some(ev) = parse_value(obj) {
                out.push(ev);
            }
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn parse_one(bytes: &[u8]) -> Option<MarketEvent> {
    // 先快速探测 event_type，避免多次解析
    let val: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    let obj = val.as_object()?;
    parse_value(obj)
}

fn parse_value(obj: &serde_json::Map<String, serde_json::Value>) -> Option<MarketEvent> {
    // 允许 event_type 缺失，按字段推断
    if let Some(v) = obj.get("event_type").and_then(|v| v.as_str()) {
        match v {
            "book" => return parse_book_obj(obj),
            "price_change" | "depth_update" => return parse_depth_obj(obj),
            "tick_size_change" => return parse_tick_obj(obj),
            "last_trade_price" => return parse_trade_obj(obj),
            _ => {}
        }
    }

    // 无 event_type 的情况
    if obj.contains_key("price_changes") {
        return parse_depth_obj(obj);
    }
    if obj.contains_key("bids") && obj.contains_key("asks") {
        return parse_book_obj(obj);
    }
    if obj.contains_key("old_tick_size") && obj.contains_key("new_tick_size") {
        return parse_tick_obj(obj);
    }
    None
}

fn parse_book_obj(obj: &serde_json::Map<String, Value>) -> Option<MarketEvent> {
    let asset_id = obj.get("asset_id")?.as_str()?.to_string();
    let market = obj.get("market")?.as_str()?.to_string();
    let bids_v = obj.get("bids")?.as_array()?;
    let asks_v = obj.get("asks")?.as_array()?;
    let mut bids = Vec::with_capacity(bids_v.len());
    let mut asks = Vec::with_capacity(asks_v.len());

    for l in bids_v {
        if let Some(m) = l.as_object() {
            let price = to_f64(m.get("price"));
            let size = to_f64(m.get("size"));
            bids.push(Level { price, size });
        } else if let Some(arr) = l.as_array() {
            if arr.len() >= 2 {
                let price = to_f64(arr.get(0));
                let size = to_f64(arr.get(1));
                bids.push(Level { price, size });
            }
        }
    }
    for l in asks_v {
        if let Some(m) = l.as_object() {
            let price = to_f64(m.get("price"));
            let size = to_f64(m.get("size"));
            asks.push(Level { price, size });
        } else if let Some(arr) = l.as_array() {
            if arr.len() >= 2 {
                let price = to_f64(arr.get(0));
                let size = to_f64(arr.get(1));
                asks.push(Level { price, size });
            }
        }
    }
    let ts = to_i64(obj.get("timestamp"));
    let hash = obj
        .get("hash")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    Some(MarketEvent::OrderBook(OrderBook {
        event_type: "book".into(),
        asset_id,
        market,
        bids,
        asks,
        timestamp: ts,
        hash,
    }))
}

fn parse_depth_obj(obj: &serde_json::Map<String, Value>) -> Option<MarketEvent> {
    let market = obj.get("market")?.as_str()?.to_string();
    let arr = obj.get("price_changes")?.as_array()?;
    let mut pcs = Vec::with_capacity(arr.len());
    for c in arr {
        if let Some(m) = c.as_object() {
            let asset_id = m.get("asset_id")?.as_str()?.to_string();
            let price = to_f64(m.get("price"));
            let size = to_f64(m.get("size"));
            let side = m
                .get("side")
                .and_then(|v| v.as_str())
                .map(parse_side)
                .unwrap_or(Side::Buy);
            let hash = m
                .get("hash")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let best_bid = to_f64(m.get("best_bid"));
            let best_ask = to_f64(m.get("best_ask"));
            pcs.push(PriceChange {
                asset_id,
                price,
                size,
                side,
                hash,
                best_bid,
                best_ask,
            });
        }
    }
    let ts = to_i64(obj.get("timestamp"));
    Some(MarketEvent::DepthUpdate(DepthUpdate {
        event_type: "price_change".into(),
        market,
        price_changes: pcs,
        timestamp: ts,
    }))
}

fn parse_tick_obj(obj: &serde_json::Map<String, Value>) -> Option<MarketEvent> {
    let asset_id = obj.get("asset_id")?.as_str()?.to_string();
    let market = obj.get("market")?.as_str()?.to_string();
    let old_tick_size = to_f64(obj.get("old_tick_size"));
    let new_tick_size = to_f64(obj.get("new_tick_size"));
    let ts = to_i64(obj.get("timestamp"));
    Some(MarketEvent::TickSizeChange(TickSizeChange {
        event_type: "tick_size_change".into(),
        asset_id,
        market,
        old_tick_size,
        new_tick_size,
        timestamp: ts,
    }))
}

fn parse_trade_obj(obj: &serde_json::Map<String, Value>) -> Option<MarketEvent> {
    let asset_id = obj.get("asset_id")?.as_str()?.to_string();
    let market = obj.get("market")?.as_str()?.to_string();
    let price = to_f64(obj.get("price"));
    let size = to_f64(obj.get("size"));
    let side = obj
        .get("side")
        .and_then(|v| v.as_str())
        .map(parse_side)
        .unwrap_or(Side::Buy);
    let hash = obj
        .get("hash")
        .or_else(|| obj.get("trade_hash"))
        .or_else(|| obj.get("tradeHash"))
        .or_else(|| obj.get("transaction_hash"))
        .or_else(|| obj.get("transactionHash"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let ts = to_i64(obj.get("timestamp"));
    Some(MarketEvent::PublicTrade(PublicTrade {
        event_type: "last_trade_price".into(),
        asset_id,
        market,
        price,
        side,
        size,
        timestamp: ts,
        hash,
    }))
}

#[inline]
fn to_f64(v: Option<&Value>) -> f64 {
    match v {
        Some(Value::String(s)) => p(s),
        Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0),
        Some(Value::Bool(b)) => {
            if *b {
                1.0
            } else {
                0.0
            }
        }
        _ => 0.0,
    }
}

#[inline]
fn to_i64(v: Option<&Value>) -> i64 {
    match v {
        Some(Value::String(s)) => s.parse().unwrap_or(0),
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0),
        _ => 0,
    }
}
