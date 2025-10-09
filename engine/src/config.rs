use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpConfig {
    pub base_url: String,
    pub timeout_ms: u64,
    pub max_connections: usize,
    #[serde(default)]
    pub health_path: String,
    #[serde(default)]
    pub health_interval_ms: u64,
}

fn default_user_ws_url_v2() -> String {
    "wss://ws-subscriptions-clob.polymarket.com/ws/user".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsConfig {
    pub market_ws_url: String,
    pub user_ws_url: String,
    #[serde(default = "default_user_ws_url_v2")]
    pub user_ws_url_v2: String,
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
                max_connections: 50,
                health_path: "/".to_string(),
                health_interval_ms: 30_000,
            },
            ws: WsConfig {
                market_ws_url: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string(),
                user_ws_url: "wss://ws-live-data.polymarket.com".to_string(),
                user_ws_url_v2: default_user_ws_url_v2(),
                max_assets_per_conn: 500,
            },
            worker_threads: num_cpus::get().max(2),
        }
    }
}
