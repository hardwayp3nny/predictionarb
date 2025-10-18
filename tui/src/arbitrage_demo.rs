use anyhow::Result;
use async_trait::async_trait;
use engine::metrics::Metrics;
use engine::{
    auth::create_or_derive_api_creds, eip712_sign::parse_pk, http_exchange::PolymarketHttpExchange,
    http_pool::HttpPool, model::*, ws_market_multi::MarketMultiWs, ws_user::UserWs, Engine,
    EngineConfig, Strategy,
};
use prometheus::Registry;
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    fs,
    sync::Arc,
    time::Duration,
};
use tokio::sync::mpsc;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Debug, Deserialize)]
struct FullConfig {
    client_config: ClientConfig,
    api_creds: Option<ApiCredsCfg>,
    pool_config: PoolCfg,
    symbols: Vec<SymbolCfg>,
    arbitrage: Option<ArbCfg>,
}

#[derive(Debug, Deserialize)]
struct ClientConfig {
    host: Option<String>,
    private_key: String,
    chain_id: Option<u64>,
    browser_address: Option<String>,
}
#[derive(Debug, Deserialize)]
struct ApiCredsCfg {
    api_key: String,
    api_secret: String,
    api_passphrase: String,
}
#[derive(Debug, Deserialize)]
struct PoolCfg {
    max_connections: Option<usize>,
    max_idle_time: Option<u64>,
    max_connection_age: Option<u64>,
    connection_timeout: Option<u64>,
}
#[derive(Debug, Deserialize, Clone)]
struct SymbolCfg {
    symbol: Option<String>,
    token_id: String,
    tick_size: Option<serde_json::Value>,
    neg_risk_id: Option<String>,
}
#[derive(Debug, Deserialize)]
struct ArbCfg {
    margin: Option<f64>,
}

#[derive(Clone)]
struct SymbolState {
    name: String,
    token_id: String,
    neg_risk_id: Option<String>,
    tick_size: f64,
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
    mbuy10: f64,
    mbuy50: f64,
    mbuy200: f64,
    mbuy500: f64,
    last_order_price: Option<f64>,
}

impl SymbolState {
    fn new(cfg: &SymbolCfg) -> Self {
        let tick = match &cfg.tick_size {
            Some(v) => {
                if let Some(s) = v.as_str() {
                    s.parse::<f64>().unwrap_or(0.01)
                } else if let Some(n) = v.as_f64() {
                    n
                } else {
                    0.01
                }
            }
            None => 0.01,
        };
        Self {
            name: cfg.symbol.clone().unwrap_or_else(|| cfg.token_id.clone()),
            token_id: cfg.token_id.clone(),
            neg_risk_id: cfg.neg_risk_id.clone(),
            tick_size: tick,
            bids: Vec::new(),
            asks: Vec::new(),
            mbuy10: 0.0,
            mbuy50: 0.0,
            mbuy200: 0.0,
            mbuy500: 0.0,
            last_order_price: None,
        }
    }
    fn best_ask(&self) -> f64 {
        self.asks
            .iter()
            .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(p, _)| *p)
            .unwrap_or(f64::INFINITY)
    }
    fn best_bid(&self) -> f64 {
        self.bids
            .iter()
            .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(p, _)| *p)
            .unwrap_or(0.0)
    }
    fn update_mkt_buy(&mut self, desired: f64) -> f64 {
        let mut rem = desired;
        let mut cost = 0.0;
        let mut filled = 0.0;
        let mut asks = self.asks.clone();
        asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        for (p, s) in asks.into_iter() {
            if rem <= 0.0 {
                break;
            }
            let take = rem.min(s);
            if take > 0.0 {
                cost += take * p;
                filled += take;
                rem -= take;
            }
        }
        let avg = if filled > 0.0 { cost / filled } else { 0.0 };
        if (desired - 10.0).abs() < 1e-9 {
            self.mbuy10 = avg;
        } else if (desired - 50.0).abs() < 1e-9 {
            self.mbuy50 = avg;
        } else if (desired - 200.0).abs() < 1e-9 {
            self.mbuy200 = avg;
        } else if (desired - 500.0).abs() < 1e-9 {
            self.mbuy500 = avg;
        }
        avg
    }
}

#[derive(Clone)]
struct GroupStrategyRs {
    neg_risk_id: String,
    symbols: Arc<tokio::sync::RwLock<HashMap<String, SymbolState>>>,
    margin: f64,
    order_tx: Option<mpsc::Sender<OrderCommand>>, // 由 demo 注入
    debug_depth: bool,
}

impl GroupStrategyRs {
    fn log_full_order_book(&self, sym: &SymbolState) {
        // sort bids desc, asks asc for display
        let mut bids = sym.bids.clone();
        bids.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        let mut asks = sym.asks.clone();
        asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let bid_levels = bids.len();
        let ask_levels = asks.len();
        tracing::info!(target: "arb",
            token=%sym.token_id,
            name=%sym.name,
            bid_levels=%bid_levels,
            ask_levels=%ask_levels,
            "ORDER BOOK SNAPSHOT START");
        for (i, (p, s)) in bids.iter().enumerate() {
            tracing::info!(target: "arb", token=%sym.token_id, side="BID", level=i+1, price=%p, size=%s);
        }
        for (i, (p, s)) in asks.iter().enumerate() {
            tracing::info!(target: "arb", token=%sym.token_id, side="ASK", level=i+1, price=%p, size=%s);
        }
        tracing::info!(target: "arb", token=%sym.token_id, "ORDER BOOK SNAPSHOT END");
    }
}

#[derive(Clone)]
enum OrderCommand {
    Place(Vec<(OrderArgs, OrderType, CreateOrderOptions)>),
    Cancel(Vec<String>),
}

#[async_trait]
impl engine::strategy::Strategy for GroupStrategyRs {
    async fn on_start(&self) -> Vec<String> {
        let m = self.symbols.read().await;
        m.keys().cloned().collect()
    }

    async fn on_order_book(&self, ev: OrderBook) {
        if let Some(mut sym) = self.symbols.read().await.get(&ev.asset_id).cloned() {
            let mut bids: Vec<(f64, f64)> = ev
                .bids
                .into_iter()
                .map(|lvl| (lvl.price, lvl.size))
                .collect();
            let mut asks: Vec<(f64, f64)> = ev
                .asks
                .into_iter()
                .map(|lvl| (lvl.price, lvl.size))
                .collect();
            bids.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            sym.bids = bids;
            sym.asks = asks;
            self.symbols
                .write()
                .await
                .insert(ev.asset_id.clone(), sym.clone());
            // 日志：打印该 token 的完整订单簿
            self.log_full_order_book(&sym);
            // 简化策略：在 best_bid - tick_size 处挂一个 size=5 的限价买单（每个 token 只在价格变化时重挂）
            let mut w = self.symbols.write().await;
            if let Some(st) = w.get_mut(&ev.asset_id) {
                let best_bid = st.best_bid();
                let tick = if st.tick_size > 0.0 {
                    st.tick_size
                } else {
                    0.01
                };
                if best_bid > 0.0 && tick > 0.0 {
                    let mut target = ((best_bid - tick) / tick).floor() * tick;
                    if target <= 0.0 {
                        target = tick;
                    }
                    let need = match st.last_order_price {
                        Some(p) => (p - target).abs() > 1e-12,
                        None => true,
                    };
                    if need {
                        st.last_order_price = Some(target);
                        drop(w);
                        let args = OrderArgs {
                            token_id: ev.asset_id.clone(),
                            price: target,
                            size: 5.0,
                            side: Side::Buy,
                        };
                        let opts = CreateOrderOptions {
                            tick_size: Some(tick),
                            neg_risk: Some(true),
                        };
                        if let Some(tx) = &self.order_tx {
                            let _ = tx
                                .send(OrderCommand::Place(vec![(args, OrderType::Gtc, opts)]))
                                .await;
                        }
                    }
                }
            }
        } else {
            tracing::info!(target:"arb", token=%ev.asset_id, market=%ev.market, msg="on_order_book for unknown token (not in strategy map)");
        }
    }

    async fn on_depth_update(&self, event: DepthUpdate) {
        // apply deltas to local book, then attempt placement like on_order_book
        if self.debug_depth {
            // print first few asset_ids from changes
            let mut sample: Vec<String> = Vec::new();
            for ch in event.price_changes.iter() {
                if sample.len() >= 5 {
                    break;
                }
                sample.push(ch.asset_id.clone());
            }

            // check whether these assets exist in symbol map
            let map = self.symbols.read().await;
            let mut unknown: Vec<String> = Vec::new();
            for aid in sample.iter() {
                if !map.contains_key(aid) {
                    unknown.push(aid.clone());
                }
            }
            if !unknown.is_empty() {
                tracing::info!(target:"arb", unknown=?unknown, msg="asset_ids not found in strategy symbol map");
            }
            drop(map);
        }
        use std::collections::HashSet;
        let mut changed_assets: HashSet<String> = HashSet::new();
        {
            let mut map = self.symbols.write().await;
            for ch in event.price_changes.iter() {
                if let Some(sym) = map.get_mut(&ch.asset_id) {
                    let price = ch.price;
                    let size = ch.size;
                    let side_buy = matches!(ch.side, Side::Buy);
                    let book = if side_buy {
                        &mut sym.bids
                    } else {
                        &mut sym.asks
                    };
                    // remove any existing entry at this price
                    if let Some(idx) = book.iter().position(|(p, _)| (*p - price).abs() <= 1e-12) {
                        book.remove(idx);
                    }
                    if size > 1e-18 {
                        book.push((price, size));
                    }
                    changed_assets.insert(sym.token_id.clone());
                }
            }
        }

        // recompute averages for changed symbols，并打印完整订单簿
        {
            let mut map = self.symbols.write().await;
            for aid in changed_assets.iter() {
                if let Some(sym) = map.get_mut(aid) {
                    sym.update_mkt_buy(10.0);
                    sym.update_mkt_buy(50.0);
                    sym.update_mkt_buy(200.0);
                    sym.update_mkt_buy(500.0);
                    // 日志：打印该 token 的完整订单簿
                    // 为避免持有写锁过久，这里克隆后再打印
                    let snapshot = sym.clone();
                    drop(map);
                    self.log_full_order_book(&snapshot);
                    map = self.symbols.write().await;
                }
            }
        }

        // 简化策略：对 changed 的资产在 best_bid - tick_size 处挂 size=5 的限价买单（仅在价格变化时重挂）
        let mut batch: Vec<(OrderArgs, OrderType, CreateOrderOptions)> = Vec::new();
        let mut w = self.symbols.write().await;
        for aid in changed_assets.iter() {
            if let Some(st) = w.get_mut(aid) {
                let best_bid = st.best_bid();
                let tick = if st.tick_size > 0.0 {
                    st.tick_size
                } else {
                    0.01
                };
                if best_bid > 0.0 && tick > 0.0 {
                    let mut target = ((best_bid - tick) / tick).floor() * tick;
                    if target <= 0.0 {
                        target = tick;
                    }
                    let need = match st.last_order_price {
                        Some(p) => (p - target).abs() > 1e-12,
                        None => true,
                    };
                    if need {
                        st.last_order_price = Some(target);
                        let args = OrderArgs {
                            token_id: st.token_id.clone(),
                            price: target,
                            size: 5.0,
                            side: Side::Buy,
                        };
                        let opts = CreateOrderOptions {
                            tick_size: Some(tick),
                            neg_risk: Some(true),
                        };
                        batch.push((args, OrderType::Gtc, opts));
                    }
                }
            }
        }
        drop(w);
        if !batch.is_empty() {
            if let Some(tx) = &self.order_tx {
                let _ = tx.send(OrderCommand::Place(batch)).await;
            }
        }
    }
    async fn on_tick_size_change(&self, _event: TickSizeChange) {}
    async fn on_public_trade(&self, _event: PublicTrade) {}
    async fn on_order_update(&self, _event: OrderUpdate) {}
    async fn on_user_trade(&self, _event: UserTrade) {}
    async fn on_signal(&self, _signal: serde_json::Value) {}
}

#[tokio::main]
async fn main() -> Result<()> {
    fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();
    let cfg_text = fs::read_to_string("config.json")?;
    let full: FullConfig = serde_json::from_str(&cfg_text)?;

    // registry & metrics
    let registry = Registry::new();
    let metrics = Metrics::new(&registry);
    let eng_cfg = EngineConfig::default();
    let http_pool = HttpPool::new(&eng_cfg, &registry)?;

    // exchange
    let signer = parse_pk(&full.client_config.private_key)?;
    // derive or create API creds at startup (no need in config)
    let api = create_or_derive_api_creds(&http_pool, &signer, None).await?;
    let maker_override = std::env::var("SAFE_ADDRESS")
        .ok()
        .or_else(|| full.client_config.browser_address.clone());
    let exchange = PolymarketHttpExchange::new(
        Arc::new(http_pool.clone()),
        signer.clone(),
        api.clone(),
        full.client_config.chain_id.unwrap_or(137),
        maker_override,
    );

    // streams
    let market = MarketMultiWs::new("wss://ws-subscriptions-clob.polymarket.com/ws/market", 500)?;
    let user = UserWs::new("wss://ws-live-data.polymarket.com", api.clone());

    // engine
    let engine = Arc::new(Engine::new(
        eng_cfg,
        Arc::new(exchange),
        Arc::new(market),
        Arc::new(user),
        metrics,
    ));

    // build strategies per neg_risk group
    let mut groups: HashMap<String, Vec<SymbolCfg>> = HashMap::new();
    for s in full.symbols.iter().cloned() {
        if let Some(g) = s.neg_risk_id.clone() {
            groups.entry(g).or_default().push(s);
        }
    }

    // Create a composite dispatcher by layering multiple GroupStrategy via Engine::with_strategy per group
    // Here we aggregate into a single Strategy implementation that forwards to inner groups is complex;
    // Instead, we create one aggregated GroupStrategy with all symbols (neg_risk ignored for simplicity)
    let mut symbols_map = HashMap::new();
    for s in full.symbols.iter() {
        symbols_map.insert(s.token_id.clone(), SymbolState::new(s));
    }
    let debug_depth = std::env::var("ARB_DEBUG_DEPTH")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let mut group = GroupStrategyRs {
        neg_risk_id: "GLOBAL".into(),
        symbols: Arc::new(tokio::sync::RwLock::new(symbols_map)),
        margin: full
            .arbitrage
            .as_ref()
            .and_then(|a| a.margin)
            .unwrap_or(0.0),
        order_tx: None,
        debug_depth,
    };

    // order command channel and executor
    let (order_tx, mut order_rx) = mpsc::channel::<OrderCommand>(1000);
    group.order_tx = Some(order_tx.clone());
    // Use a single shared Arc instance for strategy, to ensure bootstrap/safety/engine see the same state
    let shared: Arc<GroupStrategyRs> = Arc::new(group);
    let strat_arc: Arc<dyn engine::strategy::Strategy> = shared.clone();

    // Cancel batcher
    let (cancel_tx, mut cancel_rx) = mpsc::channel::<String>(2000);
    let engine_cancel = engine.clone();
    tokio::spawn(async move {
        use std::collections::HashSet;
        let mut pending: HashSet<String> = HashSet::new();
        let mut interval = tokio::time::interval(Duration::from_millis(200));
        loop {
            tokio::select! {
                Some(id) = cancel_rx.recv() => { pending.insert(id); }
                _ = interval.tick() => {
                    if pending.is_empty() { continue; }
                    let mut batch = Vec::new();
                    for id in pending.drain() { batch.push(id); if batch.len()>=50 { break; } }
                    if !batch.is_empty() { let _ = engine_cancel.cancel_orders(batch).await; }
                }
            }
        }
    });

    // spawn executor that calls engine APIs for placements
    let engine_exec = engine.clone();
    tokio::spawn(async move {
        while let Some(cmd) = order_rx.recv().await {
            match cmd {
                OrderCommand::Place(batch) => {
                    if batch.is_empty() {
                        continue;
                    }
                    // 自动按 15 拆分，Engine 内部已支持
                    if let Err(e) = engine_exec.submit_orders_bulk(batch).await {
                        tracing::warn!(target: "arb", "submit batch failed: {}", e);
                    }
                }
                OrderCommand::Cancel(ids) => {
                    if let Err(e) = engine_exec.cancel_orders(ids).await {
                        tracing::warn!(target: "arb", "cancel failed: {}", e);
                    }
                }
            }
        }
    });

    // Safety checker: periodically recompute target (per neg_risk group) and enqueue cancels
    let engine_safety = engine.clone();
    let strat_safety = shared.clone();
    let cancel_tx2 = cancel_tx.clone();
    tokio::spawn(async move {
        loop {
            let symbols = strat_safety.symbols.read().await.clone();
            // Precompute targets
            let mut active: Vec<SymbolState> = symbols
                .values()
                .filter(|s| !s.asks.is_empty())
                .cloned()
                .collect();
            for s in active.iter_mut() {
                s.update_mkt_buy(10.0);
                s.update_mkt_buy(50.0);
            }
            for s in active.iter() {
                let mut ksum = 0.0;
                let mut k10 = 0.0;
                let mut m = 0;
                for t in active.iter() {
                    if t.token_id != s.token_id && t.mbuy50 > 0.0 {
                        if s.neg_risk_id.is_some() && t.neg_risk_id == s.neg_risk_id {
                            ksum += t.mbuy50;
                            k10 += t.mbuy10;
                            m += 1;
                        }
                    }
                }
                if m == 0 {
                    continue;
                }
                let tick = if s.tick_size > 0.0 { s.tick_size } else { 0.01 };
                let mut target = ((m as f64) - ksum) / tick;
                target = target.floor() * tick;
                if (ksum - k10).abs() <= 1e-12 {
                    target -= tick;
                }
                if target <= 0.0 {
                    continue;
                }
                // Compare with existing orders in OMS
                let orders = engine_safety.list_orders_by_token(&s.token_id);
                for o in orders.into_iter() {
                    let st = format!("{:?}", o.status); // debug string
                    let st_up = st.to_uppercase();
                    if st_up.contains("LIVE")
                        || st_up.contains("PENDING")
                        || st_up.contains("SUBMITTING")
                    {
                        if (o.price - target).abs() > 1e-9 {
                            let _ = cancel_tx2.send(o.id.clone()).await;
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });

    // REST Bootstrap: concurrent /books to inject initial books before WS fully warms up
    let http_pool_clone = http_pool.clone();
    let sym_ids: Vec<String> = shared.symbols.read().await.keys().cloned().collect();
    let strat_boot = shared.clone();
    tokio::spawn(async move {
        let batch = 200usize;
        let total = sym_ids.len();
        let mut tasks = Vec::new();
        for i in (0..total).step_by(batch) {
            let ids = sym_ids[i..(i + batch).min(total)].to_vec();
            let pool = http_pool_clone.clone();
            let s = strat_boot.clone();
            tasks.push(tokio::spawn(async move {
                let body = serde_json::json!(ids.iter().map(|t| serde_json::json!({"token_id": t})).collect::<Vec<_>>());
                match pool.post("/books", None, Some(&body)).await {
                    Ok(resp) => {
                        if let Some(val) = resp.json {
                            // 顶层既可能是数组，也可能是对象包含 data/books 数组
                            let items_opt = if let Some(arr) = val.as_array() { Some(arr.clone()) }
                                            else if let Some(arr) = val.get("data").and_then(|v| v.as_array()) { Some(arr.clone()) }
                                            else if let Some(arr) = val.get("books").and_then(|v| v.as_array()) { Some(arr.clone()) }
                                            else { None };
                            if let Some(arr) = items_opt {
                                for it in arr.iter() {
                                    // REST /books 可能使用 token_id 或 asset_id，这里做兼容
                                    let asset_id = it.get("asset_id").and_then(|v| v.as_str())
                                        .or_else(|| it.get("token_id").and_then(|v| v.as_str()))
                                        .unwrap_or("")
                                        .to_string();
                                    if asset_id.is_empty() { continue; }
                                    let market = it.get("market").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                    // 更健壮的 bids/asks 解析：兼容对象{price,size}或数组[price,size]，且接受字符串或数字
                                    let to_f64 = |v: &serde_json::Value| -> f64 {
                                        if let Some(s) = v.as_str() { s.parse::<f64>().unwrap_or(0.0) }
                                        else if let Some(n) = v.as_f64() { n }
                                        else if let Some(i) = v.as_i64() { i as f64 }
                                        else { 0.0 }
                                    };
                                    let bids = it.get("bids").and_then(|v| v.as_array()).cloned().unwrap_or_default().into_iter().filter_map(|e| {
                                        if let Some(o) = e.as_object() {
                                            let p = o.get("price").map(to_f64).unwrap_or(0.0);
                                            let s = o.get("size").map(to_f64).unwrap_or(0.0);
                                            Some(Level { price: p, size: s })
                                        } else if let Some(arr) = e.as_array() {
                                            if arr.len() >= 2 { Some(Level { price: to_f64(&arr[0]), size: to_f64(&arr[1]) }) } else { None }
                                        } else { None }
                                    }).collect::<Vec<Level>>();
                                    let asks = it.get("asks").and_then(|v| v.as_array()).cloned().unwrap_or_default().into_iter().filter_map(|e| {
                                        if let Some(o) = e.as_object() {
                                            let p = o.get("price").map(to_f64).unwrap_or(0.0);
                                            let s = o.get("size").map(to_f64).unwrap_or(0.0);
                                            Some(Level { price: p, size: s })
                                        } else if let Some(arr) = e.as_array() {
                                            if arr.len() >= 2 { Some(Level { price: to_f64(&arr[0]), size: to_f64(&arr[1]) }) } else { None }
                                        } else { None }
                                    }).collect::<Vec<Level>>();
                                    let ts = it.get("timestamp").and_then(|v| v.as_str()).and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
                                    let hash = it.get("hash").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                    // update tick_size from REST book if present
                                    if let Some(tk) = it.get("tick_size") {
                                        let tick_val = if let Some(ss) = tk.as_str() { ss.parse::<f64>().unwrap_or(0.01) } else { tk.as_f64().unwrap_or(0.01) };
                                        let mut w = s.symbols.write().await;
                                        if let Some(sym) = w.get_mut(&asset_id) { sym.tick_size = tick_val; }
                                    }
                                    let ob = OrderBook{ event_type: "book".into(), asset_id: asset_id.clone(), market, bids, asks, timestamp: ts, hash };
                                    s.on_order_book(ob).await;
                                    tracing::info!(target: "arb", token=%asset_id, msg="bootstrap: injected book via REST");
                                }
                            }
                        }
                    }
                    Err(e) => { tracing::warn!(target: "arb", "bootstrap /books error: {}", e); }
                }
            }));
        }
        for t in tasks {
            let _ = t.await;
        }
        tracing::info!(target: "arb", "Bootstrap REST books completed");
    });

    // run engine with strategy
    // set strategy then run engine (Arc<Self>)
    engine.set_strategy(strat_arc);
    // 周期性时间驱动下单：即使没有新的WS事件，也会触发一次m-k评估，防止“从未触发计算”的情况
    let strat_time = shared.clone();
    tokio::spawn(async move {
        let mut iv = tokio::time::interval(Duration::from_secs(5));
        loop {
            iv.tick().await;
            tracing::info!(target:"arb", msg="time-driven tick");
            // 读取活跃（asks非空）的symbols
            let active_map = strat_time.symbols.read().await;
            let mut active: Vec<SymbolState> = active_map
                .values()
                .filter(|s| !s.asks.is_empty())
                .cloned()
                .collect();
            drop(active_map);
            if active.len() < 2 {
                tracing::info!(target:"arb", active=%active.len(), msg="time-driven: not enough active symbols (asks empty or <2) skip");
                continue;
            }
            for s in active.iter_mut() {
                s.update_mkt_buy(10.0);
                s.update_mkt_buy(50.0);
            }
            let mut batch: Vec<(OrderArgs, OrderType, CreateOrderOptions)> = Vec::new();
            use std::collections::HashSet;
            let mut seen: HashSet<String> = HashSet::new();
            for s in active.iter() {
                let mut ksum = 0.0;
                let mut k10 = 0.0;
                let mut m = 0;
                for t in active.iter() {
                    if t.token_id != s.token_id && t.mbuy50 > 0.0 {
                        if t.neg_risk_id == s.neg_risk_id {
                            ksum += t.mbuy50;
                            k10 += t.mbuy10;
                            m += 1;
                        }
                    }
                }
                if m == 0 {
                    continue;
                }
                let tick = if s.tick_size > 0.0 { s.tick_size } else { 0.01 };
                let mut target = (m as f64) - ksum;
                let pre_floor = target;
                target = (target / tick).floor() * tick;
                if (ksum - k10).abs() <= 1e-12 {
                    target -= tick;
                }
                if target <= 0.0 {
                    continue;
                }
                let best_ask = s.best_ask();
                if best_ask.is_finite() && target >= best_ask {
                    let adjusted = (best_ask - tick).max(0.0);
                    target = (adjusted / tick).floor() * tick;
                    if target <= 0.0 {
                        continue;
                    }
                }
                if best_ask.is_finite() {
                    let min_ok = (best_ask - 6.0 * tick).max(0.0);
                    if target < min_ok {
                        continue;
                    }
                }
                if !seen.insert(s.token_id.clone()) {
                    continue;
                }
                let args = OrderArgs {
                    token_id: s.token_id.clone(),
                    price: target,
                    size: 10.0,
                    side: Side::Buy,
                };
                let opts = CreateOrderOptions {
                    tick_size: Some(tick),
                    neg_risk: Some(true),
                };
                batch.push((args, OrderType::Gtc, opts));
            }
            if !batch.is_empty() {
                tracing::info!(target:"arb", count=%batch.len(), msg="time-driven submitting batch");
                if let Some(tx) = &strat_time.order_tx {
                    let _ = tx.send(OrderCommand::Place(batch)).await;
                }
            } else {
                // 对齐用户疑问：明确记录本次评估的状态，便于确认“是否触发过计算”
                tracing::info!(target:"arb", msg="time-driven evaluation complete with no orders");
            }
        }
    });
    let engine_run = engine.clone();
    let run_res = engine_run.run().await;

    // Periodic sync of orders/positions
    let engine_sync = engine.clone();
    tokio::spawn(async move {
        loop {
            if let Err(e) = engine_sync.sync_orders_via_rest().await {
                tracing::warn!(target: "arb", "sync orders failed: {}", e);
            }
            if let Err(e) = engine_sync.sync_positions_via_rest().await {
                tracing::warn!(target: "arb", "sync positions failed: {}", e);
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });

    if let Err(e) = run_res {
        Err(e)
    } else {
        Ok(())
    }
}
