use std::{env, path::PathBuf, sync::Arc};

use anyhow::{anyhow, Context, Result};
use app::{config::load_config, ArbitrageStrategy};
use engine::{
    auth::{create_or_derive_api_creds, ApiCreds},
    config::EngineConfig,
    eip712_sign::parse_pk,
    http_exchange::PolymarketHttpExchange,
    http_pool::HttpPool,
    metrics::Metrics,
    strategy_runner::StrategyRunner,
    ws_market_multi::MarketMultiWs,
    ws_user::UserWs,
    ws_user2::UserWsV2,
    ws_user_combined::CombinedUserStream,
};
use engine_core::strategy::StrategyConfig;
use prometheus::Registry;
use serde_json::Value;
use tracing::info;
use tracing_subscriber::EnvFilter;

fn json_str<'a>(obj: &'a serde_json::Map<String, Value>, key: &str) -> Option<&'a str> {
    obj.get(key).and_then(|v| v.as_str())
}

fn json_u64(obj: &serde_json::Map<String, Value>, key: &str) -> Option<u64> {
    obj.get(key).and_then(|v| v.as_u64())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let config_arg = env::args()
        .nth(1)
        .unwrap_or_else(|| "config.json".to_string());
    let config_path = PathBuf::from(config_arg);

    let cfg: Value = load_config(&config_path).await?;

    let client_cfg = cfg
        .get("client_config")
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow!("missing client_config section"))?;
    let host = json_str(client_cfg, "host")
        .unwrap_or("https://clob.polymarket.com")
        .to_string();
    let chain_id = json_u64(client_cfg, "chain_id").unwrap_or(137);
    let private_key = json_str(client_cfg, "private_key")
        .ok_or_else(|| anyhow!("missing client_config.private_key"))?;
    let browser_address = json_str(client_cfg, "browser_address").map(|s| s.to_string());

    let mut engine_cfg = EngineConfig::default();
    engine_cfg.http.base_url = host.clone();
    if let Some(pool_cfg) = cfg.get("pool_config").and_then(|v| v.as_object()) {
        if let Some(max_conn) = json_u64(pool_cfg, "max_connections") {
            engine_cfg.http.max_connections = max_conn as usize;
        }
        if let Some(timeout_sec) = json_u64(pool_cfg, "connection_timeout") {
            engine_cfg.http.timeout_ms = timeout_sec.saturating_mul(1_000);
        }
        if let Some(path) = json_str(pool_cfg, "health_path") {
            engine_cfg.http.health_path = path.to_string();
        }
        if let Some(interval_sec) = json_u64(pool_cfg, "health_interval_secs") {
            engine_cfg.http.health_interval_ms = interval_sec.saturating_mul(1_000);
        }
    }

    let registry = Arc::new(Registry::new());
    
    let metrics_registry = Arc::clone(&registry);
    tokio::spawn(async move {
        use prometheus::{Encoder, TextEncoder};
        use warp::Filter;
        
        let metrics_route = warp::path!("metrics").map(move || {
            let encoder = TextEncoder::new();
            let metric_families = metrics_registry.gather();
            let mut buffer = vec![];
            encoder.encode(&metric_families, &mut buffer).unwrap();
            warp::reply::with_header(
                String::from_utf8(buffer).unwrap(),
                "content-type",
                "text/plain; version=0.0.4",
            )
        });
        
        info!("Prometheus metrics server listening on http://0.0.0.0:9090/metrics");
        warp::serve(metrics_route)
            .run(([0, 0, 0, 0], 9090))
            .await;
    });
    
    let http_pool =
        Arc::new(HttpPool::new(&engine_cfg, registry.as_ref()).context("create http pool")?);

    let signer = parse_pk(private_key).context("parse private key")?;

    let initial_creds = cfg
        .get("api_creds")
        .and_then(|v| v.as_object())
        .map(|obj| -> Result<ApiCreds> {
            let api_key =
                json_str(obj, "api_key").ok_or_else(|| anyhow!("missing api_creds.api_key"))?;
            let api_secret = json_str(obj, "api_secret")
                .ok_or_else(|| anyhow!("missing api_creds.api_secret"))?;
            let api_passphrase = json_str(obj, "api_passphrase")
                .ok_or_else(|| anyhow!("missing api_creds.api_passphrase"))?;
            Ok(ApiCreds {
                api_key: api_key.to_string(),
                secret: api_secret.to_string(),
                passphrase: api_passphrase.to_string(),
            })
        })
        .transpose()?;

    let api_creds = match initial_creds {
        Some(creds) => creds,
        None => {
            info!("creating API credentials using signer");
            create_or_derive_api_creds(http_pool.as_ref(), &signer, None)
                .await
                .context("create API credentials")?
        }
    };

    let exchange = Arc::new(PolymarketHttpExchange::new(
        http_pool.clone(),
        signer,
        api_creds.clone(),
        chain_id,
        browser_address.clone(),
    ));

    let market_ws = Arc::new(
        MarketMultiWs::new(
            &engine_cfg.ws.market_ws_url,
            engine_cfg.ws.max_assets_per_conn,
        )
        .context("create multi market websocket")?,
    );
    let user_ws_primary = Arc::new(UserWs::new(&engine_cfg.ws.user_ws_url, api_creds.clone()));
    let user_ws_secondary = Arc::new(UserWsV2::new(
        &engine_cfg.ws.user_ws_url_v2,
        api_creds.clone(),
    ));
    let user_ws = Arc::new(CombinedUserStream::new(
        Arc::clone(&user_ws_primary),
        Arc::clone(&user_ws_secondary),
    ));

    let metrics = Metrics::new(registry.as_ref());

    let runner = StrategyRunner::new(
        engine_cfg,
        exchange,
        market_ws,
        user_ws,
        Arc::clone(&metrics),
    );

    let strategy_config = StrategyConfig::new(serde_json::json!({
        "config_path": config_path.to_string_lossy(),
        "api_creds": {
            "api_key": api_creds.api_key,
            "api_secret": api_creds.secret,
            "api_passphrase": api_creds.passphrase,
        }
    }));

    runner.run::<ArbitrageStrategy>(strategy_config).await?;
    Ok(())
}
