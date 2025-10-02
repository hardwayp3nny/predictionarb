use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpConfig {
    pub base_url: String,
    pub timeout_ms: u64,
    pub max_connections: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsConfig {
    pub market_ws_url: String,
    pub user_ws_url: String,
    pub max_assets_per_conn: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    pub http: HttpConfig,
    pub ws: WsConfig,
    pub worker_threads: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            http: HttpConfig {
                base_url: "https://clob.polymarket.com".to_string(),
                timeout_ms: 10_000,
                max_connections: 64,
            },
            ws: WsConfig {
                market_ws_url: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string(),
                user_ws_url: "wss://ws-live-data.polymarket.com".to_string(),
                max_assets_per_conn: 500,
            },
            worker_threads: num_cpus::get().max(2),
        }
    }
}
