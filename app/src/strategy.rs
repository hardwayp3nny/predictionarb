use crate::config::load_config;
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
    math::{
        ceil_to_tick, floats_equal, floor_to_tick, has_active_ask, order_is_active,
        price_for_instant_buy, sort_levels, update_levels, vwap_for_market_buy, EPSILON,
    },
    model::{
        BookSnapshot, CancelAck, DepthUpdate, Level, OpenOrder, OrderAck, OrderArgs, OrderBook,
        OrderType, OrderUpdate, Side, UserEvent, UserTrade,
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
use serde_json::json;
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    convert::TryFrom,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{
    fs,
    fs::OpenOptions,
    io::AsyncWriteExt,
    sync::{Mutex, RwLock},
    time::{sleep, Instant},
};
use tracing::{error, info, warn};

const BOOKS_BATCH_LIMIT: usize = 200;
const MIN_ASK_SIZE: f64 = 10.0;
const MIN_BID_SIZE: f64 = 5.0;
const MAX_ORDER_SIZE: f64 = 200.0;
const POST_BATCH_SIZE: usize = 15;
const ORDER_LOOP_INTERVAL_SECS: u64 = 5;
const ORDER_HEALTH_INTERVAL_MS: u64 = 500;
const HEDGE_TIMEOUT_SECS: u64 = 3;
const GROUP_BLOCK_SECS: u64 = 60;

fn current_timestamp_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|dur| dur.as_millis() as i64)
        .unwrap_or(0)
}

struct LoggerState {
    file: Option<tokio::fs::File>,
    size: u64,
}

#[derive(Clone)]
struct PersistentLogger {
    dir: PathBuf,
    base_name: String,
    max_files: usize,
    max_bytes: u64,
    state: Arc<Mutex<LoggerState>>,
}

impl PersistentLogger {
    fn new(dir: PathBuf, base_name: String, max_files: usize, max_bytes: u64) -> Self {
        let normalized_max_files = max_files.max(1);
        Self {
            dir,
            base_name,
            max_files: normalized_max_files,
            max_bytes: max_bytes.max(1),
            state: Arc::new(Mutex::new(LoggerState {
                file: None,
                size: 0,
            })),
        }
    }

    async fn log_json(&self, value: serde_json::Value) -> Result<()> {
        let line = value.to_string();
        self.write_line(line.as_bytes()).await
    }

    async fn write_line(&self, bytes: &[u8]) -> Result<()> {
        fs::create_dir_all(&self.dir)
            .await
            .with_context(|| format!("create log dir: {}", self.dir.display()))?;

        let needed = bytes.len() as u64 + 1; // account for newline

        {
            let mut guard = self.state.lock().await;
            if guard.file.is_none() {
                let (file, size) = self.open_current_file().await?;
                guard.file = Some(file);
                guard.size = size;
            }

            if guard.size + needed > self.max_bytes {
                guard.file = None;
                guard.size = 0;
                drop(guard);
                self.rotate_files().await?;
                let mut guard = self.state.lock().await;
                let (file, size) = self.open_current_file().await?;
                guard.file = Some(file);
                guard.size = size;
                self.write_to_file(guard.file.as_mut().unwrap(), bytes)
                    .await?;
                guard.size += needed;
                return Ok(());
            } else {
                self.write_to_file(guard.file.as_mut().unwrap(), bytes)
                    .await?;
                guard.size += needed;
                return Ok(());
            }
        }
    }

    async fn write_to_file(&self, file: &mut tokio::fs::File, bytes: &[u8]) -> Result<()> {
        file.write_all(bytes).await.context("write log line")?;
        file.write_all(b"\n").await.context("write log newline")?;
        file.flush().await.context("flush log file")?;
        Ok(())
    }

    async fn open_current_file(&self) -> Result<(tokio::fs::File, u64)> {
        let path = self.base_file_path();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .with_context(|| format!("open log file: {}", path.display()))?;
        let size = file.metadata().await.map(|meta| meta.len()).unwrap_or(0);
        Ok((file, size))
    }

    async fn rotate_files(&self) -> Result<()> {
        let archives = self.max_files.saturating_sub(1);
        if archives == 0 {
            let _ = fs::remove_file(self.base_file_path()).await;
            return Ok(());
        }

        let oldest = self.rotated_path(archives);
        let _ = fs::remove_file(&oldest).await;

        for idx in (1..archives).rev() {
            let src = self.rotated_path(idx);
            let dst = self.rotated_path(idx + 1);
            if fs::metadata(&src).await.is_ok() {
                fs::rename(&src, &dst).await.with_context(|| {
                    format!("rotate log file {} -> {}", src.display(), dst.display())
                })?;
            }
        }

        let base = self.base_file_path();
        if fs::metadata(&base).await.is_ok() {
            fs::rename(&base, self.rotated_path(1))
                .await
                .with_context(|| format!("rotate base log file: {}", base.display()))?;
        }

        Ok(())
    }

    fn base_file_path(&self) -> PathBuf {
        self.dir.join(&self.base_name)
    }

    fn rotated_path(&self, index: usize) -> PathBuf {
        self.dir.join(format!("{}.{}", self.base_name, index))
    }
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
struct HedgePendingOrder {
    expected_fill: f64,
    filled: f64,
    deadline: Instant,
}

impl HedgePendingOrder {
    fn new(expected_fill: f64, deadline: Instant) -> Self {
        Self {
            expected_fill,
            filled: 0.0,
            deadline,
        }
    }

    fn remaining(&self) -> f64 {
        (self.expected_fill - self.filled).max(0.0)
    }
}

#[derive(Debug, Default, Clone)]
struct GroupExecutionState {
    blocked_until: Option<Instant>,
    pending: HashMap<String, HedgePendingOrder>,
}

impl GroupExecutionState {
    fn is_blocked(&self, now: Instant) -> bool {
        self.blocked_until
            .map(|deadline| deadline > now)
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone)]
struct HedgeOrderPlan {
    token: ArbitrageToken,
    price: f64,
    size: f64,
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
    #[serde(default)]
    health_path: Option<String>,
    #[serde(default)]
    health_interval_secs: Option<u64>,
}

impl PoolConfig {
    fn connection_timeout_ms(&self) -> Option<u64> {
        self.connection_timeout
            .map(|seconds| seconds.saturating_mul(1_000))
    }

    fn health_interval_ms(&self) -> Option<u64> {
        self.health_interval_secs
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
    order_fill_tracker: Arc<RwLock<HashMap<String, f64>>>,
    hedge_orders: Arc<RwLock<HashSet<String>>>,
    hedge_order_index: Arc<RwLock<HashMap<String, String>>>,
    group_states: Arc<RwLock<HashMap<String, GroupExecutionState>>>,
    logger: Arc<PersistentLogger>,
}

impl ArbitrageStrategy {
    pub fn new<P: Into<PathBuf>>(config_path: P) -> Self {
        let logs_dir = PathBuf::from("logs");
        let logger =
            PersistentLogger::new(logs_dir, "strategy.log".to_string(), 10, 10 * 1024 * 1024);
        Self {
            config_path: config_path.into(),
            runtime: Arc::new(RwLock::new(None)),
            preloaded_api_creds: Arc::new(RwLock::new(None)),
            pending_orders: Arc::new(RwLock::new(HashMap::new())),
            order_fill_tracker: Arc::new(RwLock::new(HashMap::new())),
            hedge_orders: Arc::new(RwLock::new(HashSet::new())),
            hedge_order_index: Arc::new(RwLock::new(HashMap::new())),
            group_states: Arc::new(RwLock::new(HashMap::new())),
            logger: Arc::new(logger),
        }
    }

    pub async fn set_api_creds(&self, creds: ApiCreds) {
        *self.preloaded_api_creds.write().await = Some(creds);
    }

    async fn initialize_runtime(&self) -> Result<Vec<String>> {
        let cfg_value = load_config(&self.config_path).await?;
        let cfg: AppConfig = serde_json::from_value(cfg_value).context("parse config json")?;

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
            if let Some(path) = pool_cfg.health_path.as_ref() {
                engine_cfg.http.health_path = path.clone();
            }
            if let Some(interval_ms) = pool_cfg.health_interval_ms() {
                engine_cfg.http.health_interval_ms = interval_ms;
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

        {
            let mut states = self.group_states.write().await;
            states.retain(|group_id, _| combos.contains_key(group_id));
            for group_id in combos.keys() {
                states
                    .entry(group_id.clone())
                    .or_insert_with(GroupExecutionState::default);
            }
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
        let health_strategy = self.clone();
        tokio::spawn(async move {
            health_strategy
                .order_health_loop(Duration::from_millis(ORDER_HEALTH_INTERVAL_MS))
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

        let blocked_groups = self.blocked_groups_snapshot().await;

        #[derive(Clone)]
        struct GroupTokenInfo {
            token: ArbitrageToken,
            order_book: OrderBook,
            best_ask: Level,
        }

        let mut prepared_orders: Vec<(String, String)> = Vec::new();

        for (group_id, tokens) in combos {
            if blocked_groups.contains(&group_id) {
                info!(
                    group_id = group_id.as_str(),
                    "group blocked, skipping trading cycle entry"
                );
                continue;
            }
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
            let results = join_all(
                chunk
                    .iter()
                    .map(|(order_id, _)| self.post_order(order_id, "primary")),
            )
            .await;
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

    async fn order_health_loop(self, interval: Duration) {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            if let Err(err) = self.check_limit_orders_health().await {
                warn!(?err, "order health check failed");
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

    pub async fn post_order(&self, order_id: &str, source: &str) -> Result<OrderAck> {
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

        self.log_order_submission_event(&prepared.args, &prepared.signed.order_id, source)
            .await;

        let submit_result = exchange.submit_prepared_order(&prepared).await;
        let ack = match submit_result {
            Ok(ack) => ack,
            Err(err) => {
                let mut guard = self.pending_orders.write().await;
                guard.remove(order_id);
                return Err(err);
            }
        };

        manager.on_ack(&ack);

        {
            // Always drop the prepared order after we attempt to submit it to avoid unbounded growth.
            let mut guard = self.pending_orders.write().await;
            guard.remove(order_id);
            if let Some(actual) = ack.order_id.as_ref() {
                if actual != order_id {
                    guard.remove(actual);
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

    async fn record_fill_delta(&self, order_id: &str, new_matched: f64) -> (f64, f64) {
        let mut guard = self.order_fill_tracker.write().await;
        let prev = guard.get(order_id).cloned().unwrap_or(0.0);
        let delta = if new_matched + EPSILON < prev {
            // Inconsistent update; reset baseline to avoid negative delta.
            guard.insert(order_id.to_string(), new_matched);
            0.0
        } else {
            let diff = new_matched - prev;
            guard.insert(order_id.to_string(), new_matched);
            if diff <= EPSILON {
                0.0
            } else {
                diff
            }
        };
        (delta, prev)
    }

    async fn override_fill_total(&self, order_id: &str, total_matched: f64) {
        let mut guard = self.order_fill_tracker.write().await;
        guard.insert(order_id.to_string(), total_matched);
    }

    async fn blocked_groups_snapshot(&self) -> HashSet<String> {
        let now = Instant::now();
        let guard = self.group_states.read().await;
        guard
            .iter()
            .filter_map(|(group_id, state)| {
                if state.is_blocked(now) {
                    Some(group_id.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    async fn is_group_blocked(&self, group_id: &str) -> bool {
        let now = Instant::now();
        let guard = self.group_states.read().await;
        guard
            .get(group_id)
            .map(|state| state.is_blocked(now))
            .unwrap_or(false)
    }

    async fn token_symbol(&self, token_id: &str) -> Option<String> {
        let guard = self.runtime.read().await;
        guard.as_ref().and_then(|rt| {
            rt.tokens
                .iter()
                .find(|token| token.token_id == token_id)
                .map(|token| token.symbol.clone())
        })
    }

    fn side_label(side: Side) -> &'static str {
        match side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }
    }

    async fn log_order_submission_event(&self, args: &OrderArgs, order_id: &str, source: &str) {
        let symbol = self
            .token_symbol(&args.token_id)
            .await
            .unwrap_or_else(|| args.token_id.clone());
        let entry = json!({
            "type": "order_submit",
            "timestamp_ms": current_timestamp_ms(),
            "order_id": order_id,
            "token_id": args.token_id,
            "symbol": symbol,
            "side": Self::side_label(args.side),
            "price": args.price,
            "size": args.size,
            "source": source,
        });
        if let Err(err) = self.logger.log_json(entry).await {
            warn!(
                ?err,
                order_id = order_id,
                token_id = args.token_id.as_str(),
                "failed to write order submission log"
            );
        }
    }

    async fn log_order_update_event(&self, event: &OrderUpdate) {
        let symbol = self
            .token_symbol(&event.asset_id)
            .await
            .unwrap_or_else(|| event.asset_id.clone());
        let entry = json!({
            "type": "order_update",
            "timestamp_ms": current_timestamp_ms(),
            "order_id": event.id,
            "token_id": event.asset_id,
            "symbol": symbol,
            "status": event.status,
            "side": Self::side_label(event.side),
            "price": event.price,
            "size_matched": event.size_matched,
        });
        if let Err(err) = self.logger.log_json(entry).await {
            warn!(
                ?err,
                order_id = event.id.as_str(),
                token_id = event.asset_id.as_str(),
                "failed to write order update log"
            );
        }
    }

    async fn handle_group_timeout(&self, group_id: &str) -> Result<()> {
        let now = Instant::now();
        let pending_orders: Vec<String> = {
            let mut guard = self.group_states.write().await;
            if let Some(state) = guard.get_mut(group_id) {
                let keys: Vec<String> = state.pending.keys().cloned().collect();
                state.pending.clear();
                state.blocked_until = Some(now + Duration::from_secs(GROUP_BLOCK_SECS));
                keys
            } else {
                Vec::new()
            }
        };

        if !pending_orders.is_empty() {
            let mut hedge_guard = self.hedge_orders.write().await;
            for order_id in &pending_orders {
                hedge_guard.remove(order_id);
            }
            drop(hedge_guard);

            let mut index_guard = self.hedge_order_index.write().await;
            for order_id in &pending_orders {
                index_guard.remove(order_id);
            }
        }

        {
            let mut tracker = self.order_fill_tracker.write().await;
            for order_id in &pending_orders {
                tracker.remove(order_id);
            }
        }

        let cancel_ids = self.collect_group_order_ids(group_id).await?;
        if !cancel_ids.is_empty() {
            match self.cancel_orders(cancel_ids.clone()).await {
                Ok(_) => info!(
                    group_id = group_id,
                    order_ids = ?cancel_ids,
                    "cancelled group orders after hedge timeout"
                ),
                Err(err) => warn!(
                    ?err,
                    group_id = group_id,
                    order_ids = ?cancel_ids,
                    "failed to cancel group orders after hedge timeout"
                ),
            }
        }

        info!(group_id = group_id, "group blocked due to hedge timeout");
        Ok(())
    }

    async fn maybe_timeout_group(&self, group_id: &str) -> Result<()> {
        let now = Instant::now();
        let expired = {
            let guard = self.group_states.read().await;
            guard.get(group_id).map_or(false, |state| {
                state
                    .pending
                    .values()
                    .any(|pending| pending.deadline <= now && pending.remaining() > EPSILON)
            })
        };

        if expired {
            self.handle_group_timeout(group_id).await?;
        }

        Ok(())
    }

    async fn collect_group_order_ids(&self, group_id: &str) -> Result<Vec<String>> {
        let token_ids = {
            let guard = self.runtime.read().await;
            let Some(rt) = guard.as_ref() else {
                return Ok(Vec::new());
            };
            let Some(tokens) = rt.combos.get(group_id) else {
                return Ok(Vec::new());
            };
            tokens
                .iter()
                .map(|token| token.token_id.clone())
                .collect::<Vec<_>>()
        };

        let mut ids: Vec<String> = Vec::new();
        for token_id in token_ids {
            let orders = self.list_orders(&token_id).await;
            for order in orders.into_iter() {
                if order_is_active(&order) {
                    ids.push(order.id.clone());
                }
            }
        }
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    async fn is_hedge_order(&self, order_id: &str) -> bool {
        let guard = self.hedge_orders.read().await;
        guard.contains(order_id)
    }

    async fn remove_hedge_tracking(&self, order_id: &str) {
        {
            let mut guard = self.hedge_orders.write().await;
            guard.remove(order_id);
        }
        let group_id = {
            let mut guard = self.hedge_order_index.write().await;
            guard.remove(order_id)
        };
        if let Some(group_id) = group_id {
            let mut state_guard = self.group_states.write().await;
            if let Some(state) = state_guard.get_mut(&group_id) {
                state.pending.remove(order_id);
            }
        }
    }

    async fn register_hedge_order(&self, group_id: &str, order_id: &str, expected_fill: f64) {
        {
            let mut guard = self.hedge_orders.write().await;
            guard.insert(order_id.to_string());
        }
        {
            let mut guard = self.hedge_order_index.write().await;
            guard.insert(order_id.to_string(), group_id.to_string());
        }
        {
            let mut guard = self.group_states.write().await;
            let state = guard
                .entry(group_id.to_string())
                .or_insert_with(GroupExecutionState::default);
            state.pending.insert(
                order_id.to_string(),
                HedgePendingOrder::new(
                    expected_fill,
                    Instant::now() + Duration::from_secs(HEDGE_TIMEOUT_SECS),
                ),
            );
        }

        let strategy = self.clone();
        let group_key = group_id.to_string();
        let order_key = order_id.to_string();
        tokio::spawn(async move {
            sleep(Duration::from_secs(HEDGE_TIMEOUT_SECS)).await;
            if let Err(err) = strategy.maybe_timeout_group(&group_key).await {
                warn!(
                    ?err,
                    group_id = group_key.as_str(),
                    order_id = order_key.as_str(),
                    "hedge timeout task failed"
                );
            }
        });
    }

    async fn handle_order_fill(&self, event: OrderUpdate, delta: f64) -> Result<()> {
        if self.is_hedge_order(&event.id).await {
            self.update_hedge_progress(&event, delta).await
        } else {
            self.handle_primary_order_fill(&event, delta).await
        }
    }

    async fn update_hedge_progress(&self, event: &OrderUpdate, delta: f64) -> Result<()> {
        let group_id = {
            let guard = self.hedge_order_index.read().await;
            guard.get(&event.id).cloned()
        };

        let Some(group_id) = group_id else {
            return Ok(());
        };

        let status = event.status.to_uppercase();
        let mut should_remove = false;
        let mut should_fail = false;

        {
            let mut guard = self.group_states.write().await;
            if let Some(state) = guard.get_mut(&group_id) {
                if let Some(pending) = state.pending.get_mut(&event.id) {
                    pending.filled += delta;
                    let remaining = pending.remaining();
                    if remaining <= EPSILON || status == "MATCHED" {
                        state.pending.remove(&event.id);
                        should_remove = true;
                    } else if matches!(status.as_str(), "CANCELLED" | "CANCELED" | "REJECTED") {
                        state.pending.remove(&event.id);
                        should_remove = true;
                        should_fail = true;
                    }
                } else {
                    should_remove = true;
                }
            } else {
                should_remove = true;
            }
        }

        if should_remove {
            self.remove_hedge_tracking(&event.id).await;
        }

        if should_fail {
            self.handle_group_timeout(&group_id).await?;
        }

        Ok(())
    }

    async fn handle_primary_order_fill(&self, event: &OrderUpdate, delta: f64) -> Result<()> {
        if delta <= EPSILON {
            return Ok(());
        }

        if event.side != Side::Buy {
            return Ok(());
        }

        let (trigger_token, group_tokens) = {
            let guard = self.runtime.read().await;
            let Some(rt) = guard.as_ref() else {
                return Ok(());
            };
            let Some(token) = rt
                .tokens
                .iter()
                .find(|token| token.token_id == event.asset_id)
                .cloned()
            else {
                return Ok(());
            };
            let Some(group) = rt.combos.get(&token.neg_risk_id).cloned() else {
                return Ok(());
            };
            (token, group)
        };

        if group_tokens.len() <= 1 {
            return Ok(());
        }

        // if self.is_group_blocked(&trigger_token.neg_risk_id).await {
        //     info!(
        //         group_id = trigger_token.neg_risk_id.as_str(),
        //         "group blocked, skipping hedge placement"
        //     );
        //     return Ok(());
        // }

        self.initiate_group_hedge(trigger_token, group_tokens, delta)
            .await
    }

    async fn initiate_group_hedge(
        &self,
        trigger_token: ArbitrageToken,
        group_tokens: Vec<ArbitrageToken>,
        delta: f64,
    ) -> Result<()> {
        if delta <= EPSILON {
            return Ok(());
        }

        let mut plans: Vec<HedgeOrderPlan> = Vec::new();

        for token in group_tokens.into_iter() {
            if token.token_id == trigger_token.token_id {
                continue;
            }

            let Some(book) = self.order_book_snapshot(&token.token_id).await else {
                continue;
            };

            if !has_active_ask(&book) {
                continue;
            }

            let Some((worst_price, _)) = price_for_instant_buy(&book, delta) else {
                warn!(
                    token_id = token.token_id.as_str(),
                    size = delta,
                    "insufficient ask liquidity for hedge"
                );
                return Ok(());
            };

            let mut limit_price = if token.tick_size > 0.0 {
                ceil_to_tick(worst_price, token.tick_size)
            } else {
                worst_price
            };

            if limit_price <= 0.0 {
                warn!(
                    token_id = token.token_id.as_str(),
                    "invalid hedge price computed"
                );
                return Ok(());
            }

            if limit_price * delta < 1.0 - EPSILON {
                let required_price = if delta > EPSILON {
                    1.0 / delta
                } else {
                    limit_price
                };
                let adjusted = if token.tick_size > 0.0 {
                    ceil_to_tick(required_price, token.tick_size)
                } else {
                    required_price
                };
                limit_price = limit_price.max(adjusted);
            }

            if limit_price * delta < 1.0 - EPSILON {
                warn!(
                    token_id = token.token_id.as_str(),
                    price = limit_price,
                    size = delta,
                    "unable to reach $1 notional for hedge"
                );
                return Ok(());
            }

            plans.push(HedgeOrderPlan {
                token,
                price: limit_price,
                size: delta,
            });
        }

        if plans.is_empty() {
            return Ok(());
        }

        self.execute_hedge_orders(&trigger_token.neg_risk_id, plans)
            .await
    }

    async fn execute_hedge_orders(&self, group_id: &str, plans: Vec<HedgeOrderPlan>) -> Result<()> {
        let mut registered: Vec<(String, String, f64)> = Vec::new();

        let mut built_orders: Vec<(String, HedgeOrderPlan)> = Vec::new();
        for plan in plans.iter() {
            match self
                .build_limit_order(
                    &plan.token.token_id,
                    Side::Buy,
                    plan.price,
                    plan.size,
                    OrderType::Gtc,
                    None,
                )
                .await
            {
                Ok(id) => built_orders.push((id, plan.clone())),
                Err(err) => {
                    warn!(
                        ?err,
                        token_id = plan.token.token_id.as_str(),
                        "failed to build hedge order"
                    );
                    return Ok(());
                }
            }
        }

        if built_orders.is_empty() {
            return Ok(());
        }

        let results = join_all(
            built_orders
                .iter()
                .map(|(order_id, _)| self.post_order(order_id, "hedge")),
        )
        .await;

        for ((order_id, plan), result) in built_orders.into_iter().zip(results.into_iter()) {
            match result {
                Ok(ack) => {
                    if !ack.success {
                        warn!(
                            token_id = plan.token.token_id.as_str(),
                            order_id = order_id.as_str(),
                            error = ?ack.error_message,
                            "hedge order rejected"
                        );
                        self.handle_group_timeout(group_id).await?;
                        return Ok(());
                    }
                    let actual_id = ack.order_id.clone().unwrap_or(order_id.clone());
                    registered.push((actual_id, plan.token.token_id.clone(), plan.size));
                }
                Err(err) => {
                    warn!(
                        ?err,
                        token_id = plan.token.token_id.as_str(),
                        order_id = order_id.as_str(),
                        "failed to post hedge order"
                    );
                    self.handle_group_timeout(group_id).await?;
                    return Ok(());
                }
            }
        }

        if registered.is_empty() {
            return Ok(());
        }

        for (order_id, _, expected) in &registered {
            self.register_hedge_order(group_id, order_id, *expected)
                .await;
        }

        info!(
            group_id = group_id,
            hedge_orders = registered.len(),
            "submitted hedge orders"
        );
        Ok(())
    }

    async fn check_limit_orders_health(&self) -> Result<()> {
        let (token_map, combos_index) = {
            let guard = self.runtime.read().await;
            let Some(rt) = guard.as_ref() else {
                return Ok(());
            };
            let token_map: HashMap<String, ArbitrageToken> = rt
                .tokens
                .iter()
                .cloned()
                .map(|token| (token.token_id.clone(), token))
                .collect();
            let combos_index: HashMap<String, Vec<String>> = rt
                .combos
                .iter()
                .map(|(neg_risk_id, tokens)| {
                    (
                        neg_risk_id.clone(),
                        tokens
                            .iter()
                            .map(|token| token.token_id.clone())
                            .collect::<Vec<_>>(),
                    )
                })
                .collect();
            (token_map, combos_index)
        };

        if token_map.is_empty() {
            return Ok(());
        }

        let mut to_cancel: HashSet<String> = HashSet::new();

        for (token_id, token) in token_map.iter() {
            let orders = self.list_orders(token_id).await;
            for order in orders.into_iter() {
                if !order_is_active(&order) || order.side != Side::Buy {
                    continue;
                }

                let remaining_size = (order.size - order.size_matched).max(0.0);
                if remaining_size <= EPSILON {
                    continue;
                }

                let combo_tokens = match combos_index.get(&token.neg_risk_id) {
                    Some(ids) => ids,
                    None => continue,
                };

                let mut active_count = 0usize;
                let mut vwap_sum = 0.0;
                let mut insufficient_liquidity = false;

                for combo_token_id in combo_tokens.iter() {
                    let Some(combo_token) = token_map.get(combo_token_id) else {
                        continue;
                    };
                    let book_opt = {
                        let guard = combo_token.order_book.read().await;
                        guard.clone()
                    };
                    let Some(book) = book_opt else {
                        continue;
                    };
                    if !has_active_ask(&book) {
                        continue;
                    }
                    active_count += 1;
                    if combo_token_id == &order.asset_id {
                        continue;
                    }
                    match vwap_for_market_buy(&book, remaining_size) {
                        Some(avg_price) => {
                            vwap_sum += avg_price;
                        }
                        None => {
                            insufficient_liquidity = true;
                            break;
                        }
                    }
                }

                if active_count <= 1 {
                    continue;
                }

                if insufficient_liquidity {
                    to_cancel.insert(order.id.clone());
                    continue;
                }

                let allowed = (active_count as f64 - 1.0) - (vwap_sum + order.price);
                if allowed < -EPSILON {
                    to_cancel.insert(order.id.clone());
                }
            }
        }

        if to_cancel.is_empty() {
            return Ok(());
        }

        let mut ids: Vec<String> = to_cancel.into_iter().collect();
        ids.sort();
        let orders_cancelled = ids.clone();
        match self.cancel_orders(ids).await {
            Ok(_) => {
                info!(order_ids = ?orders_cancelled, "cancelled unhealthy limit orders");
            }
            Err(err) => {
                warn!(
                    ?err,
                    order_ids = ?orders_cancelled,
                    "failed to cancel unhealthy limit orders"
                );
            }
        }

        Ok(())
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
        let manager = self.ensure_order_manager(&event.asset_id).await;
        if let Some(ref mgr) = manager {
            let evt = UserEvent::Order(event.clone());
            mgr.on_user_event(&evt);
        }

        self.log_order_update_event(&event).await;

        let (mut delta, prev_total) = self.record_fill_delta(&event.id, event.size_matched).await;
        let status = event.status.to_uppercase();

        if delta <= EPSILON && status == "MATCHED" {
            if let Some(ref mgr) = manager {
                if let Some(order_size) = mgr
                    .list_by_asset(&event.asset_id)
                    .into_iter()
                    .find(|order| order.id == event.id)
                    .map(|order| order.size)
                {
                    let inferred = (order_size - prev_total).max(0.0);
                    if inferred > EPSILON {
                        delta = inferred;
                        self.override_fill_total(&event.id, order_size).await;
                    }
                }
            }
        }

        if delta > EPSILON {
            if let Err(err) = self.handle_order_fill(event.clone(), delta).await {
                warn!(
                    ?err,
                    order_id = event.id.as_str(),
                    asset_id = event.asset_id.as_str(),
                    "failed to process order fill"
                );
            }
        }

        if matches!(
            status.as_str(),
            "MATCHED" | "CANCELLED" | "CANCELED" | "REJECTED"
        ) {
            let mut guard = self.pending_orders.write().await;
            guard.remove(&event.id);
            {
                let mut tracker = self.order_fill_tracker.write().await;
                tracker.remove(&event.id);
            }
            self.remove_hedge_tracking(&event.id).await;
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
