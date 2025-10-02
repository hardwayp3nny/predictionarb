use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};
use async_trait::async_trait;
use engine::{
    auth::{create_or_derive_api_creds, ApiCreds},
    config::EngineConfig,
    eip712_sign::parse_pk,
    http_exchange::PolymarketHttpExchange,
    http_pool::HttpPool,
    model::{BookSnapshot, DepthUpdate, Level, OrderBook, Side},
    strategy::Strategy,
};
use futures::future::try_join_all;
use prometheus::Registry;
use serde::Deserialize;
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    convert::TryFrom,
    path::PathBuf,
    sync::Arc,
};
use tokio::{fs, sync::RwLock};
use tracing::{error, warn};

const EPSILON: f64 = 1e-9;
const BOOKS_BATCH_LIMIT: usize = 200;

fn floats_equal(a: f64, b: f64) -> bool {
    (a - b).abs() < EPSILON
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

#[allow(dead_code)]
struct StrategyRuntime {
    signer: PrivateKeySigner,
    api_creds: ApiCreds,
    http_pool: Arc<HttpPool>,
    registry: Arc<Registry>,
    tokens: Vec<ArbitrageToken>,
    combos: HashMap<String, Vec<ArbitrageToken>>,
    books: HashMap<String, Arc<RwLock<Option<OrderBook>>>>,
    aggregator: OrderBookAggregator,
    chain_id: u64,
    browser_address: Option<String>,
    rpc_url: Option<String>,
}

#[derive(Clone)]
pub struct ArbitrageStrategy {
    config_path: PathBuf,
    runtime: Arc<RwLock<Option<StrategyRuntime>>>,
    preloaded_api_creds: Arc<RwLock<Option<ApiCreds>>>,
}

impl ArbitrageStrategy {
    pub fn new<P: Into<PathBuf>>(config_path: P) -> Self {
        Self {
            config_path: config_path.into(),
            runtime: Arc::new(RwLock::new(None)),
            preloaded_api_creds: Arc::new(RwLock::new(None)),
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

        let signer = parse_pk(&client_cfg.private_key).context("parse private key")?;
        let registry = Arc::new(Registry::new());
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

        let http_pool =
            Arc::new(HttpPool::new(&engine_cfg, registry.as_ref()).context("create http pool")?);

        let preset_api_creds = {
            let guard = self.preloaded_api_creds.read().await;
            guard.clone()
        };

        let api_creds = if let Some(creds) = preset_api_creds {
            creds
        } else {
            let result = create_or_derive_api_creds(http_pool.as_ref(), &signer, None).await;
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

        let mut tokens: Vec<ArbitrageToken> = cfg
            .symbols
            .into_iter()
            .map(ArbitrageToken::try_from)
            .collect::<Result<Vec<_>>>()?;

        let mut aggregator = OrderBookAggregator::new();
        let token_ids: Vec<String> = tokens.iter().map(|t| t.token_id.clone()).collect();
        let mut initial_books: HashMap<String, BookSnapshot> = HashMap::new();
        if !token_ids.is_empty() {
            let exchange_http = Arc::new(PolymarketHttpExchange::new(
                Arc::clone(&http_pool),
                signer.clone(),
                ApiCreds {
                    api_key: api_creds.api_key.clone(),
                    secret: api_creds.secret.clone(),
                    passphrase: api_creds.passphrase.clone(),
                },
                client_cfg.chain_id,
                client_cfg.browser_address.clone(),
            ));

            let chunked_ids: Vec<Vec<String>> = token_ids
                .chunks(BOOKS_BATCH_LIMIT)
                .map(|chunk| chunk.to_vec())
                .collect();

            let fetches = chunked_ids.into_iter().map(|chunk| {
                let client = Arc::clone(&exchange_http);
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
            combos
                .entry(token.neg_risk_id.clone())
                .or_default()
                .push(token.clone());
            let book_snapshot = initial_books
                .get(&token.token_id)
                .map(|snap| snap.order_book.clone());
            let lock = Arc::new(RwLock::new(book_snapshot));
            token.order_book = lock.clone();
            books.insert(token.token_id.clone(), lock);
        }

        let mut seen = HashSet::new();
        let mut asset_ids = Vec::new();
        for token in &tokens {
            if seen.insert(token.token_id.clone()) {
                asset_ids.push(token.token_id.clone());
            }
        }
        let runtime = StrategyRuntime {
            signer,
            api_creds: api_creds.clone(),
            http_pool,
            registry,
            tokens: tokens.clone(),
            combos,
            books,
            aggregator,
            chain_id: client_cfg.chain_id,
            browser_address: client_cfg.browser_address,
            rpc_url: client_cfg.rpc_url,
        };

        *self.runtime.write().await = Some(runtime);
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
            .map(|rt| rt.api_creds.clone())
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
}
