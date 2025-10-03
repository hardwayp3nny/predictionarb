use alloy_primitives::U256;
use alloy_signer_local::PrivateKeySigner;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use engine::{
    auth::{create_or_derive_api_creds, ApiCreds},
    config::EngineConfig,
    eip712_sign::parse_pk,
    http_exchange::PolymarketHttpExchange,
    http_pool::HttpPool,
    model::{
        BookSnapshot, CancelAck, DepthUpdate, Level, OpenOrder, OrderAck, OrderArgs, OrderBook,
        OrderStatus, OrderType, OrderUpdate, Side, UserEvent, UserTrade,
    },
    oms::OrderManager,
    order_builder::{prepare_signed_order, PreparedOrder},
    order_types::{CreateOrderOptionsRs, SigType},
    ports::{ExchangeClient, OrdersSnapshot, PositionsSnapshot},
    strategy::Strategy,
};
use futures::future::{join_all, try_join_all};
use prometheus::Registry;
use serde::Deserialize;
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    convert::TryFrom,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{fs, sync::RwLock};
use tracing::{error, info, warn};

const EPSILON: f64 = 1e-9;
const BOOKS_BATCH_LIMIT: usize = 200;
const MIN_ASK_SIZE: f64 = 10.0;
const MIN_BID_SIZE: f64 = 5.0;
const MAX_ORDER_SIZE: f64 = 200.0;
const POST_BATCH_SIZE: usize = 15;
const ORDER_LOOP_INTERVAL_SECS: u64 = 5;

fn floats_equal(a: f64, b: f64) -> bool {
    (a - b).abs() < EPSILON
}

fn floor_to_tick(value: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return value;
    }
    let scaled = (value / tick).floor();
    (scaled * tick).max(0.0)
}

fn order_is_active(order: &OpenOrder) -> bool {
    matches!(order.status, OrderStatus::PendingNew | OrderStatus::Live)
}

fn sort_levels(levels: &mut Vec<Level>, is_bid: bool) {
    if is_bid {
        levels.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(Ordering::Equal));
    } else {
        levels.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(Ordering::Equal));
    }
}

fn update_levels(levels: &mut Vec<Level>, price: f64, size: f64, is_bid: bool) {
    if let Some(idx) = levels.iter().position(|lvl| floats_equal(lvl.price, price)) {
        if size <= 0.0 {
            levels.remove(idx);
        } else {
            levels[idx].size = size;
        }
    } else if size > 0.0 {
        levels.push(Level { price, size });
    }
    sort_levels(levels, is_bid);
}

#[derive(Debug, Default, Clone)]
struct OrderBookAggregator {
    books: HashMap<String, OrderBook>,
}

impl OrderBookAggregator {
    fn new() -> Self {
        Self::default()
    }

    fn apply_snapshot(&mut self, snapshot: &OrderBook) -> OrderBook {
        let mut book = snapshot.clone();
        sort_levels(&mut book.bids, true);
        sort_levels(&mut book.asks, false);
        self.books.insert(book.asset_id.clone(), book.clone());
        book
    }

    fn apply_depth_update(&mut self, update: &DepthUpdate) -> Vec<OrderBook> {
        let mut touched: HashSet<String> = HashSet::new();
        for change in &update.price_changes {
            let entry = self
                .books
                .entry(change.asset_id.clone())
                .or_insert_with(|| OrderBook {
                    event_type: "book".into(),
                    asset_id: change.asset_id.clone(),
                    market: update.market.clone(),
                    bids: Vec::new(),
                    asks: Vec::new(),
                    timestamp: update.timestamp,
                    hash: change.hash.clone(),
                });
            entry.market = update.market.clone();
            if update.timestamp != 0 {
                entry.timestamp = update.timestamp;
            }
            if !change.hash.is_empty() {
                entry.hash = change.hash.clone();
            }
            match change.side {
                Side::Buy => update_levels(&mut entry.bids, change.price, change.size, true),
                Side::Sell => update_levels(&mut entry.asks, change.price, change.size, false),
            }
            touched.insert(change.asset_id.clone());
        }
        touched
            .into_iter()
            .filter_map(|asset| self.books.get(&asset).cloned())
            .collect()
    }

    fn current(&self, asset_id: &str) -> Option<&OrderBook> {
        self.books.get(asset_id)
    }
}

#[derive(Debug, Clone)]
pub struct ArbitrageToken {
    pub symbol: String,
    pub neg_risk_id: String,
    pub token_id: String,
    pub tick_size: f64,
    pub neg_risk: bool,
    pub min_order_size: Option<f64>,
    pub order_book: Arc<RwLock<Option<OrderBook>>>,
}

#[derive(Debug, Clone, Deserialize)]
struct ClientConfig {
    host: String,
    private_key: String,
    chain_id: u64,
    #[serde(default)]
    browser_address: Option<String>,
    #[serde(default)]
    rpc_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiCredsConfig {
    #[serde(rename = "api_key")]
    api_key: String,
    #[serde(rename = "api_secret")]
    api_secret: String,
    #[serde(rename = "api_passphrase")]
    api_passphrase: String,
}

impl From<ApiCredsConfig> for ApiCreds {
    fn from(value: ApiCredsConfig) -> Self {
        ApiCreds {
            api_key: value.api_key,
            secret: value.api_secret,
            passphrase: value.api_passphrase,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
struct PoolConfig {
    #[serde(default)]
    max_connections: Option<usize>,
    #[serde(default)]
    connection_timeout: Option<u64>,
}

impl PoolConfig {
    fn connection_timeout_ms(&self) -> Option<u64> {
        self.connection_timeout
            .map(|seconds| seconds.saturating_mul(1_000))
    }
}

#[derive(Debug, Clone, Deserialize)]
struct SymbolConfig {
    symbol: String,
    neg_risk_id: String,
    token_id: String,
    tick_size: String,
    #[serde(default)]
    neg_risk: bool,
}

impl TryFrom<SymbolConfig> for ArbitrageToken {
    type Error = anyhow::Error;

    fn try_from(value: SymbolConfig) -> Result<Self, Self::Error> {
        let tick_size = value
            .tick_size
            .parse::<f64>()
            .with_context(|| format!("invalid tick_size: {}", value.tick_size))?;
        Ok(Self {
            symbol: value.symbol,
            neg_risk_id: value.neg_risk_id,
            token_id: value.token_id,
            tick_size,
            neg_risk: value.neg_risk,
            min_order_size: None,
            order_book: Arc::new(RwLock::new(None)),
        })
    }
}

#[derive(Debug, Deserialize)]
struct AppConfig {
    #[serde(rename = "client_config")]
    client: ClientConfig,
    #[serde(rename = "api_creds")]
    #[serde(default)]
    api_creds: Option<ApiCredsConfig>,
    #[serde(rename = "pool_config")]
    #[serde(default)]
    pool: Option<PoolConfig>,
    #[serde(default)]
    symbols: Vec<SymbolConfig>,
}

struct StrategyRuntime {
    signer: Arc<PrivateKeySigner>,
    api_creds: Arc<ApiCreds>,
    tokens: Vec<ArbitrageToken>,
    combos: HashMap<String, Vec<ArbitrageToken>>,
    books: HashMap<String, Arc<RwLock<Option<OrderBook>>>>,
    aggregator: OrderBookAggregator,
    exchange: Arc<PolymarketHttpExchange>,
    order_managers: HashMap<String, OrderManager>,
    chain_id: u64,
    browser_address: Option<String>,
    rpc_url: Option<String>,
    _registry: Arc<Registry>,
}

impl StrategyRuntime {
    fn ensure_order_manager(&mut self, token_id: &str) -> OrderManager {
        self.order_managers
            .entry(token_id.to_string())
            .or_insert_with(OrderManager::new)
            .clone()
    }

    fn get_order_manager(&self, token_id: &str) -> Option<OrderManager> {
        self.order_managers.get(token_id).cloned()
    }
}

#[derive(Clone)]
pub struct ArbitrageStrategy {
    config_path: PathBuf,
    runtime: Arc<RwLock<Option<StrategyRuntime>>>,
    preloaded_api_creds: Arc<RwLock<Option<ApiCreds>>>,
    pending_orders: Arc<RwLock<HashMap<String, PreparedOrder>>>,
}

impl ArbitrageStrategy {
    pub fn new<P: Into<PathBuf>>(config_path: P) -> Self {
        Self {
            config_path: config_path.into(),
            runtime: Arc::new(RwLock::new(None)),
            preloaded_api_creds: Arc::new(RwLock::new(None)),
            pending_orders: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn set_api_creds(&self, creds: ApiCreds) {
        *self.preloaded_api_creds.write().await = Some(creds);
    }

    async fn initialize_runtime(&self) -> Result<Vec<String>> {
        let raw = fs::read(&self.config_path)
            .await
            .with_context(|| format!("read config file: {}", self.config_path.display()))?;
        let cfg: AppConfig = serde_json::from_slice(&raw).context("parse config json")?;

        let client_cfg = cfg.client.clone();
        let fallback_api_creds = cfg.api_creds.clone().map(ApiCreds::from);
        let pool_cfg = cfg.pool.clone();

        let signer = Arc::new(parse_pk(&client_cfg.private_key).context("parse private key")?);
        let mut engine_cfg = EngineConfig::default();
        engine_cfg.http.base_url = client_cfg.host.clone();
        if let Some(pool_cfg) = pool_cfg.as_ref() {
            if let Some(max_conn) = pool_cfg.max_connections {
                engine_cfg.http.max_connections = max_conn;
            }
            if let Some(timeout_ms) = pool_cfg.connection_timeout_ms() {
                engine_cfg.http.timeout_ms = timeout_ms;
            }
        }

        let registry = Arc::new(Registry::new());
        let http_pool =
            Arc::new(HttpPool::new(&engine_cfg, registry.as_ref()).context("create http pool")?);

        let preset_api_creds = {
            let guard = self.preloaded_api_creds.read().await;
            guard.clone()
        };

        let api_creds = if let Some(creds) = preset_api_creds {
            creds
        } else {
            let result =
                create_or_derive_api_creds(http_pool.as_ref(), signer.as_ref(), None).await;
            let creds = match result {
                Ok(creds) => creds,
                Err(err) => {
                    if let Some(fallback) = fallback_api_creds.clone() {
                        warn!(
                            ?err,
                            "failed to create API key from signer, falling back to config creds"
                        );
                        fallback
                    } else {
                        return Err(err.context("failed to create API creds"));
                    }
                }
            };
            {
                let mut guard = self.preloaded_api_creds.write().await;
                *guard = Some(creds.clone());
            }
            creds
        };

        let api_creds_arc = Arc::new(api_creds);

        let mut tokens: Vec<ArbitrageToken> = cfg
            .symbols
            .into_iter()
            .map(ArbitrageToken::try_from)
            .collect::<Result<Vec<_>>>()?;

        let exchange = Arc::new(PolymarketHttpExchange::new(
            Arc::clone(&http_pool),
            (*signer).clone(),
            (*api_creds_arc).clone(),
            client_cfg.chain_id,
            client_cfg.browser_address.clone(),
        ));

        let mut aggregator = OrderBookAggregator::new();
        let token_ids: Vec<String> = tokens.iter().map(|t| t.token_id.clone()).collect();
        let mut initial_books: HashMap<String, BookSnapshot> = HashMap::new();
        if !token_ids.is_empty() {
            let chunked_ids: Vec<Vec<String>> = token_ids
                .chunks(BOOKS_BATCH_LIMIT)
                .map(|chunk| chunk.to_vec())
                .collect();

            let fetches = chunked_ids.into_iter().map(|chunk| {
                let client = Arc::clone(&exchange);
                async move { client.fetch_books(chunk.as_slice()).await }
            });

            match try_join_all(fetches).await {
                Ok(batches) => {
                    for snapshots in batches {
                        for snapshot in snapshots {
                            aggregator.apply_snapshot(&snapshot.order_book);
                            initial_books.insert(snapshot.order_book.asset_id.clone(), snapshot);
                        }
                    }
                }
                Err(err) => {
                    warn!(?err, "failed to fetch initial books");
                }
            }
        }

        for token in tokens.iter_mut() {
            if let Some(snapshot) = initial_books.get(&token.token_id) {
                token.tick_size = snapshot.tick_size;
                if let Some(nr) = snapshot.neg_risk {
                    token.neg_risk = nr;
                }
                token.min_order_size = snapshot.min_order_size;
            }
        }

        let mut combos: HashMap<String, Vec<ArbitrageToken>> = HashMap::new();
        let mut books: HashMap<String, Arc<RwLock<Option<OrderBook>>>> = HashMap::new();
        for token in tokens.iter_mut() {
            let book_snapshot = initial_books
                .get(&token.token_id)
                .map(|snap| snap.order_book.clone());
            let lock = Arc::new(RwLock::new(book_snapshot));
            token.order_book = lock.clone();
            combos
                .entry(token.neg_risk_id.clone())
                .or_default()
                .push(token.clone());
            books.insert(token.token_id.clone(), lock);
        }

        let mut seen = HashSet::new();
        let mut asset_ids = Vec::new();
        for token in &tokens {
            if seen.insert(token.token_id.clone()) {
                asset_ids.push(token.token_id.clone());
            }
        }

        let mut order_managers: HashMap<String, OrderManager> = HashMap::new();
        for token in &tokens {
            order_managers
                .entry(token.token_id.clone())
                .or_insert_with(OrderManager::new);
        }
        let runtime = StrategyRuntime {
            signer,
            api_creds: Arc::clone(&api_creds_arc),
            tokens: tokens.clone(),
            combos,
            books,
            aggregator,
            exchange,
            order_managers,
            chain_id: client_cfg.chain_id,
            browser_address: client_cfg.browser_address,
            rpc_url: client_cfg.rpc_url,
            _registry: registry,
        };

        *self.runtime.write().await = Some(runtime);
        let sync_strategy = self.clone();
        tokio::spawn(async move {
            sync_strategy
                .sync_orders_positions_loop(Duration::from_secs(5))
                .await;
        });
        let trading_strategy = self.clone();
        tokio::spawn(async move {
            trading_strategy
                .trading_loop(Duration::from_secs(ORDER_LOOP_INTERVAL_SECS))
                .await;
        });
        Ok(asset_ids)
    }

    pub async fn tokens(&self) -> Vec<ArbitrageToken> {
        self.runtime
            .read()
            .await
            .as_ref()
            .map(|rt| rt.tokens.clone())
            .unwrap_or_default()
    }

    pub async fn api_creds(&self) -> Option<ApiCreds> {
        self.runtime
            .read()
            .await
            .as_ref()
            .map(|rt| rt.api_creds.as_ref().clone())
    }

    pub async fn handle_order_book(&self, snapshot: OrderBook) {
        let (lock, merged) = {
            let mut guard = self.runtime.write().await;
            if let Some(rt) = guard.as_mut() {
                let merged = rt.aggregator.apply_snapshot(&snapshot);
                let entry = rt
                    .books
                    .entry(merged.asset_id.clone())
                    .or_insert_with(|| Arc::new(RwLock::new(None)))
                    .clone();
                (Some(entry), Some(merged))
            } else {
                (None, None)
            }
        };
        if let (Some(lock), Some(book)) = (lock, merged) {
            let mut guard = lock.write().await;
            *guard = Some(book);
        }
    }

    pub async fn handle_depth_update(&self, update: DepthUpdate) {
        let locks = {
            let mut guard = self.runtime.write().await;
            if let Some(rt) = guard.as_mut() {
                let updates = rt.aggregator.apply_depth_update(&update);
                updates
                    .into_iter()
                    .map(|book| {
                        let entry = rt
                            .books
                            .entry(book.asset_id.clone())
                            .or_insert_with(|| Arc::new(RwLock::new(None)))
                            .clone();
                        (entry, book)
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            }
        };
        for (lock, book) in locks {
            let mut guard = lock.write().await;
            *guard = Some(book);
        }
    }

    pub async fn order_book_snapshot(&self, token_id: &str) -> Option<OrderBook> {
        let (lock, agg_snapshot) = {
            let guard = self.runtime.read().await;
            if let Some(rt) = guard.as_ref() {
                let lock = rt.books.get(token_id).cloned();
                let agg = rt.aggregator.current(token_id).cloned();
                (lock, agg)
            } else {
                (None, None)
            }
        };
        if let Some(book_lock) = lock {
            let book = book_lock.read().await;
            if let Some(snapshot) = book.as_ref() {
                return Some(snapshot.clone());
            }
        }
        agg_snapshot
    }

    async fn ensure_order_manager(&self, token_id: &str) -> Option<OrderManager> {
        let mut guard = self.runtime.write().await;
        guard.as_mut().map(|rt| rt.ensure_order_manager(token_id))
    }

    async fn get_order_manager(&self, token_id: &str) -> Option<OrderManager> {
        let guard = self.runtime.read().await;
        guard.as_ref().and_then(|rt| rt.get_order_manager(token_id))
    }

    async fn run_trading_cycle(&self) -> Result<()> {
        let combos = {
            let guard = self.runtime.read().await;
            match guard.as_ref() {
                Some(rt) => rt.combos.clone(),
                None => return Ok(()),
            }
        };

        let group_count = combos.len();
        info!(groups = group_count, "starting trading cycle");
        if combos.is_empty() {
            return Ok(());
        }

        #[derive(Clone)]
        struct GroupTokenInfo {
            token: ArbitrageToken,
            order_book: OrderBook,
            best_ask: Level,
        }

        let mut prepared_orders: Vec<(String, String)> = Vec::new();

        for (group_id, tokens) in combos {
            let mut infos: Vec<GroupTokenInfo> = Vec::new();
            for token in tokens {
                let maybe_book = { token.order_book.read().await.clone() };
                let book = match maybe_book {
                    Some(book) => book,
                    None => {
                        info!(
                            token_id = token.token_id.as_str(),
                            symbol = token.symbol.as_str(),
                            "no order book available"
                        );
                        continue;
                    }
                };
                let mut filtered_asks: Vec<Level> = book
                    .asks
                    .iter()
                    .filter(|lvl| lvl.size >= MIN_ASK_SIZE)
                    .cloned()
                    .collect();
                if filtered_asks.is_empty() {
                    info!(
                        token_id = token.token_id.as_str(),
                        symbol = token.symbol.as_str(),
                        "no qualifying asks after filtering"
                    );
                    continue;
                }
                filtered_asks
                    .sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(Ordering::Equal));
                let best_ask = filtered_asks[0].clone();
                infos.push(GroupTokenInfo {
                    token,
                    order_book: book,
                    best_ask,
                });
            }

            info!(
                group_id = group_id.as_str(),
                candidates = infos.len(),
                "processing token group"
            );
            if infos.len() < 2 {
                info!(
                    group_id = group_id.as_str(),
                    "not enough active tokens, skipping group"
                );
                continue;
            }

            let group_size = infos.len() as f64;

            for idx in 0..infos.len() {
                let token = infos[idx].token.clone();
                let order_book = infos[idx].order_book.clone();
                let other_best_asks: Vec<Level> = infos
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| *j != idx)
                    .map(|(_, ctx)| ctx.best_ask.clone())
                    .collect();
                if other_best_asks.is_empty() {
                    continue;
                }

                let sum_i: f64 = other_best_asks.iter().map(|lvl| lvl.price).sum();
                let minshares = other_best_asks
                    .iter()
                    .fold(f64::INFINITY, |acc, lvl| acc.min(lvl.size));
                if !minshares.is_finite() || minshares <= 0.0 {
                    continue;
                }

                let threshold_raw = group_size - sum_i - 1.0;
                let threshold = floor_to_tick(threshold_raw, token.tick_size);
                if threshold <= 0.0 {
                    continue;
                }

                let mut relevant_bids: Vec<Level> = order_book
                    .bids
                    .iter()
                    .filter(|lvl| lvl.price <= threshold + EPSILON)
                    .filter(|lvl| lvl.size >= MIN_BID_SIZE)
                    .cloned()
                    .collect();
                relevant_bids
                    .sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(Ordering::Equal));

                let mut target_price = threshold;
                if let Some(highest) = relevant_bids.first() {
                    if floats_equal(highest.price, threshold) {
                        if relevant_bids.len() > 1 {
                            target_price = relevant_bids[1].price + token.tick_size;
                        } else {
                            target_price = threshold;
                        }
                    } else {
                        target_price = highest.price + token.tick_size;
                    }
                }

                if target_price > threshold {
                    target_price = threshold;
                }
                target_price = floor_to_tick(target_price, token.tick_size);
                if target_price <= 0.0 {
                    continue;
                }

                let order_size = minshares.min(MAX_ORDER_SIZE).floor();
                if let Some(min_size) = token.min_order_size {
                    if order_size < min_size {
                        continue;
                    }
                }
                if order_size <= 0.0 {
                    continue;
                }

                let existing_orders = self.list_orders(&token.token_id).await;
                if existing_orders.iter().any(order_is_active) {
                    info!(
                        token_id = token.token_id.as_str(),
                        "skipping token due to active order"
                    );
                    continue;
                }

                info!(
                    token_id = token.token_id.as_str(),
                    symbol = token.symbol.as_str(),
                    threshold,
                    target_price,
                    order_size,
                    "preparing buy order"
                );
                match self
                    .build_limit_order(
                        &token.token_id,
                        Side::Buy,
                        target_price,
                        order_size,
                        OrderType::Gtc,
                        None,
                    )
                    .await
                {
                    Ok(order_id) => {
                        prepared_orders.push((order_id, token.token_id.clone()));
                    }
                    Err(err) => {
                        warn!(
                            ?err,
                            token_id = token.token_id.as_str(),
                            symbol = token.symbol.as_str(),
                            "failed to build order"
                        );
                    }
                }
            }
        }

        if prepared_orders.is_empty() {
            return Ok(());
        }

        for chunk in prepared_orders.chunks(POST_BATCH_SIZE) {
            info!(batch_size = chunk.len(), "submitting order batch");
            let results =
                join_all(chunk.iter().map(|(order_id, _)| self.post_order(order_id))).await;
            for ((order_id, token_id), result) in chunk.iter().zip(results.into_iter()) {
                match result {
                    Ok(ack) => {
                        if !ack.success {
                            warn!(
                                token_id = token_id.as_str(),
                                order_id = order_id.as_str(),
                                error = ?ack.error_message,
                                "order post failed"
                            );
                        }
                    }
                    Err(err) => {
                        warn!(
                            ?err,
                            token_id = token_id.as_str(),
                            order_id = order_id.as_str(),
                            "failed to post order"
                        );
                    }
                }
            }
            info!(batch_size = chunk.len(), "completed order batch submission");
        }

        Ok(())
    }

    async fn trading_loop(self, interval: Duration) {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            if let Err(err) = self.run_trading_cycle().await {
                warn!(?err, "trading cycle failed");
            }
        }
    }

    pub async fn build_limit_order(
        &self,
        token_identifier: &str,
        side: Side,
        price: f64,
        size: f64,
        order_type: OrderType,
        expiration: Option<u64>,
    ) -> Result<String> {
        let prepared = {
            let guard = self.runtime.read().await;
            let rt = guard
                .as_ref()
                .ok_or_else(|| anyhow!("strategy runtime not initialized"))?;
            let token = rt
                .tokens
                .iter()
                .find(|t| t.token_id == token_identifier || t.symbol == token_identifier)
                .cloned()
                .ok_or_else(|| anyhow!("unknown token: {}", token_identifier))?;
            let args = OrderArgs {
                token_id: token.token_id.clone(),
                price,
                size,
                side,
            };
            prepare_signed_order(
                rt.signer.as_ref(),
                rt.chain_id,
                args,
                order_type,
                CreateOrderOptionsRs {
                    tick_size: Some(token.tick_size),
                    neg_risk: Some(token.neg_risk),
                },
                expiration.unwrap_or(0),
                0,
                U256::from(0u8),
                "0x0000000000000000000000000000000000000000",
                rt.browser_address.as_deref(),
                SigType::PolyGnosisSafe,
            )?
        };

        let order_id = prepared.signed.order_id.clone();
        let mut guard = self.pending_orders.write().await;
        guard.insert(order_id.clone(), prepared);
        Ok(order_id)
    }

    pub async fn post_order(&self, order_id: &str) -> Result<OrderAck> {
        let prepared = {
            let guard = self.pending_orders.read().await;
            guard
                .get(order_id)
                .cloned()
                .ok_or_else(|| anyhow!("order {} not prepared", order_id))?
        };

        let (exchange, manager) = {
            let mut guard = self.runtime.write().await;
            let rt = guard
                .as_mut()
                .ok_or_else(|| anyhow!("strategy runtime not initialized"))?;
            let mgr = rt.ensure_order_manager(&prepared.args.token_id);
            (Arc::clone(&rt.exchange), mgr)
        };

        manager.upsert_pending(prepared.signed.order_id.clone(), &prepared.args);
        let ack = exchange.submit_prepared_order(&prepared).await?;
        manager.on_ack(&ack);

        if ack.success {
            let ack_id = ack.order_id.clone();
            let mut guard = self.pending_orders.write().await;
            guard.remove(order_id);
            if let Some(actual) = ack_id {
                if actual != order_id {
                    guard.remove(&actual);
                }
            }
        }

        Ok(ack)
    }

    pub async fn cancel_orders(&self, ids: Vec<String>) -> Result<CancelAck> {
        if ids.is_empty() {
            return Ok(CancelAck {
                canceled: vec![],
                not_canceled: vec![],
            });
        }
        let exchange = {
            let guard = self.runtime.read().await;
            let rt = guard
                .as_ref()
                .ok_or_else(|| anyhow!("strategy runtime not initialized"))?;
            Arc::clone(&rt.exchange)
        };
        let ack = exchange.cancel_orders(ids.clone()).await?;
        if !ack.canceled.is_empty() {
            let mut guard = self.pending_orders.write().await;
            for id in &ack.canceled {
                guard.remove(id);
            }
        }
        Ok(ack)
    }

    pub async fn list_orders(&self, token_id: &str) -> Vec<OpenOrder> {
        if let Some(manager) = self.get_order_manager(token_id).await {
            manager.list_by_asset(token_id)
        } else {
            Vec::new()
        }
    }

    async fn sync_orders_once(&self) -> Result<()> {
        let exchange = {
            let guard = self.runtime.read().await;
            match guard.as_ref() {
                Some(rt) => Arc::clone(&rt.exchange),
                None => return Ok(()),
            }
        };
        let orders = OrdersSnapshot::fetch_orders(exchange.as_ref()).await?;
        let mut guard = self.runtime.write().await;
        if let Some(rt) = guard.as_mut() {
            let known: HashSet<String> = rt.tokens.iter().map(|t| t.token_id.clone()).collect();
            for order in orders.into_iter() {
                if known.contains(&order.asset_id) {
                    let manager = rt.ensure_order_manager(&order.asset_id);
                    manager.upsert_snapshot(order);
                }
            }
        }
        Ok(())
    }

    async fn sync_positions_once(&self) -> Result<()> {
        let exchange = {
            let guard = self.runtime.read().await;
            match guard.as_ref() {
                Some(rt) => Arc::clone(&rt.exchange),
                None => return Ok(()),
            }
        };
        let positions = PositionsSnapshot::fetch_positions(exchange.as_ref()).await?;
        let mut guard = self.runtime.write().await;
        if let Some(rt) = guard.as_mut() {
            let known: HashSet<String> = rt.tokens.iter().map(|t| t.token_id.clone()).collect();
            for pos in positions.into_iter() {
                if known.contains(&pos.asset_id) {
                    let manager = rt.ensure_order_manager(&pos.asset_id);
                    manager.set_position(pos);
                }
            }
        }
        Ok(())
    }

    async fn sync_orders_positions_loop(self, interval: Duration) {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            if let Err(err) = self.sync_orders_once().await {
                warn!(?err, "sync orders via REST failed");
            }
            if let Err(err) = self.sync_positions_once().await {
                warn!(?err, "sync positions via REST failed");
            }
        }
    }

    async fn handle_order_update(&self, event: OrderUpdate) {
        if let Some(manager) = self.ensure_order_manager(&event.asset_id).await {
            let evt = UserEvent::Order(event.clone());
            manager.on_user_event(&evt);
        }
        let status = event.status.to_uppercase();
        if matches!(
            status.as_str(),
            "MATCHED" | "CANCELLED" | "CANCELED" | "REJECTED"
        ) {
            let mut guard = self.pending_orders.write().await;
            guard.remove(&event.id);
        }
    }

    async fn handle_user_trade(&self, event: UserTrade) {
        if let Some(manager) = self.ensure_order_manager(&event.asset_id).await {
            let evt = UserEvent::Trade(event);
            manager.on_user_event(&evt);
        }
    }
}

#[async_trait]
impl Strategy for ArbitrageStrategy {
    async fn on_start(&self) -> Vec<String> {
        match self.initialize_runtime().await {
            Ok(asset_ids) => asset_ids,
            Err(err) => {
                error!(?err, "arbitrage strategy initialization failed");
                Vec::new()
            }
        }
    }

    async fn on_order_book(&self, event: OrderBook) {
        self.handle_order_book(event).await;
    }

    async fn on_depth_update(&self, event: DepthUpdate) {
        self.handle_depth_update(event).await;
    }

    async fn on_order_update(&self, event: OrderUpdate) {
        self.handle_order_update(event).await;
    }

    async fn on_user_trade(&self, event: UserTrade) {
        self.handle_user_trade(event).await;
    }
}
