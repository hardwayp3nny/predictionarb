use alloy_primitives::U256;
use anyhow::Result;
use async_trait::async_trait;
use engine::metrics::Metrics;
use engine::{
    auth::{create_l2_headers, create_or_derive_api_creds},
    eip712_sign::parse_pk,
    http_exchange::PolymarketHttpExchange,
    http_pool::HttpPool,
    model::*,
    order_builder::build_signed_order,
    order_types::{CreateOrderOptionsRs, SigType},
    ports::*,
    ws_market_multi::MarketMultiWs,
    ws_user::UserWs,
    Engine, EngineConfig,
};
use prometheus::Registry;
use serde_json::json;
use std::sync::Arc;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Clone)]
struct MockExchange;

#[async_trait]
impl ExchangeClient for MockExchange {
    async fn create_orders_batch(
        &self,
        batch: Vec<(OrderArgs, OrderType, CreateOrderOptions)>,
    ) -> Result<Vec<OrderAck>> {
        // mock: echo success with synthetic order_id per item
        let mut out = Vec::with_capacity(batch.len());
        for (args, _t, _o) in batch.into_iter() {
            out.push(OrderAck {
                success: true,
                error_message: None,
                order_id: Some(format!("mock-{}-{}", args.token_id, args.side as u8)),
            });
        }
        Ok(out)
    }
    async fn create_order(
        &self,
        args: OrderArgs,
        _typ: OrderType,
        _opts: CreateOrderOptions,
    ) -> Result<OrderAck> {
        let ack = OrderAck {
            success: true,
            error_message: None,
            order_id: Some(format!("mock-{}-{}", args.token_id, args.side as u8)),
        };
        Ok(ack)
    }
    async fn cancel_orders(&self, _ids: Vec<String>) -> Result<CancelAck> {
        Ok(CancelAck {
            canceled: vec![],
            not_canceled: vec![],
        })
    }
    async fn get_orders(&self) -> Result<Vec<OpenOrder>> {
        Ok(vec![])
    }
}

#[derive(Clone)]
struct MockMarket;

#[async_trait]
impl MarketStream for MockMarket {
    async fn subscribe(&self, _asset_ids: Vec<String>) -> Result<()> {
        Ok(())
    }
    async fn next(&self) -> Result<Option<MarketEvent>> {
        Ok(None)
    }
}

#[derive(Clone)]
struct MockUser;

#[async_trait]
impl UserStream for MockUser {
    async fn connect(&self) -> Result<()> {
        Ok(())
    }
    async fn next(&self) -> Result<Option<UserEvent>> {
        Ok(None)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    let cfg = EngineConfig::default();
    let registry = Registry::new();
    let metrics = Metrics::new(&registry);

    // 简单演示：用 HTTP 池访问 /time，验证 keep-alive + gzip/br/deflate 支持
    let http_pool = HttpPool::new(&cfg, &registry)?;
    // 准备 token id（复用到 /books 与订阅）
    let token_id =
        "72644755948825200035358029619849753313650897666061401891172445259217157804042".to_string();
    if let Ok(resp) = http_pool.get("/time", None).await {
        if let Some(js) = resp.json.as_ref() {
            tracing::info!(target: "app", "/time => {}", js);
        } else if let Some(txt) = resp.text.as_ref() {
            tracing::info!(target: "app", "/time => {}", txt);
        } else {
            tracing::info!(target: "app", "/time => {} bytes", resp.bytes.len());
        }
    }

    // 调用 /books 接口（POST），验证 chunked+gzip 与浏览器头
    let books_body = serde_json::json!([{ "token_id": token_id }]);
    match http_pool.post("/books", None, Some(&books_body)).await {
        Ok(resp) => {
            if let Some(js) = resp.json.as_ref() {
                tracing::info!(target: "app", "/books returned array with len={}", js.as_array().map(|a| a.len()).unwrap_or(0));
            } else if let Some(txt) = resp.text.as_ref() {
                tracing::info!(target: "app", "/books text len={} bytes", txt.len());
            } else {
                tracing::info!(target: "app", "/books raw bytes {}", resp.bytes.len());
            }
        }
        Err(e) => tracing::warn!(target: "app", "/books error: {}", e),
    }

    // 从环境变量读取私钥，创建或派生 API Key，然后发一笔测试订单
    let maybe_pk = std::env::var("PRIVATE_KEY");
    if let Ok(pk) = maybe_pk.clone() {
        let signer = parse_pk(&pk)?;
        let creds = create_or_derive_api_creds(&http_pool, &signer, None).await?;
        // 打印完整 API Creds，便于排查鉴权问题
        let creds_json = serde_json::json!({
            "api_creds": {
                "api_key": creds.api_key,
                "api_secret": creds.secret,
                "api_passphrase": creds.passphrase,
            }
        });
        if let Ok(pretty) = serde_json::to_string_pretty(&creds_json) {
            tracing::info!(target: "app", "{}", pretty);
        }

        // 查询市场参数，确保 payload 正确
        let tick_path = format!("/tick-size/{}", &token_id);
        let neg_path = format!("/neg-risk/{}", &token_id);
        let mut tick_size_val: f64 = 0.01;
        let mut neg_risk_val: bool = true;
        if let Ok(resp) = http_pool.get(&tick_path, None).await {
            if let Some(js) = resp.json.as_ref() {
                if let Some(ts) = js.get("minimum_tick_size").and_then(|v| v.as_str()) {
                    tick_size_val = ts.parse::<f64>().unwrap_or(tick_size_val);
                }
            }
        }
        if let Ok(resp) = http_pool.get(&neg_path, None).await {
            if let Some(js) = resp.json.as_ref() {
                if let Some(nr) = js.get("neg_risk").and_then(|v| v.as_bool()) {
                    neg_risk_val = nr;
                }
            }
        }
        tracing::info!(target: "app", "market params: tick_size={}, neg_risk={}", tick_size_val, neg_risk_val);

        // 初始化 Engine 与 Exchange（统一走批量下单接口）
        let maker_override = std::env::var("SAFE_ADDRESS")
            .ok()
            .or_else(|| std::env::var("BROWSER_ADDRESS").ok());
        let exchange = PolymarketHttpExchange::new(
            Arc::new(http_pool.clone()),
            signer.clone(),
            creds.clone(),
            137,
            maker_override,
        );
        let engine = Engine::new(
            cfg.clone(),
            Arc::new(exchange),
            Arc::new(MarketMultiWs::new(
                "wss://ws-subscriptions-clob.polymarket.com/ws/market",
                500,
            )?),
            Arc::new(UserWs::new(
                "wss://ws-live-data.polymarket.com",
                creds.clone(),
            )),
            metrics.clone(),
        );

        // 使用批量接口（即使是单笔也统一走批量）
        let args = OrderArgs {
            token_id: token_id.clone(),
            price: 0.01,
            size: 5.0,
            side: Side::Buy,
        };
        let opts = CreateOrderOptions {
            tick_size: Some(tick_size_val),
            neg_risk: Some(neg_risk_val),
        };
        let acks = engine
            .submit_orders_bulk(vec![(args, OrderType::Gtc, opts)])
            .await?;
        for ack in acks.into_iter() {
            tracing::info!(target: "app", "bulk order ack => success={} id={:?} err={:?}", ack.success, ack.order_id, ack.error_message);
        }
    } else {
        tracing::warn!(target: "app", "PRIVATE_KEY not set, skip order post demo");
    }

    // 使用 MarketMultiWs 与 UserWs 适配器（自动 500/连接 拆分与聚合）
    let market = MarketMultiWs::new("wss://ws-subscriptions-clob.polymarket.com/ws/market", 500)?;

    if let Ok(pk) = maybe_pk {
        let signer = parse_pk(&pk)?;
        let creds = create_or_derive_api_creds(&http_pool, &signer, None).await?;
        let user = UserWs::new("wss://ws-live-data.polymarket.com", creds.clone());

        let maker_override = std::env::var("SAFE_ADDRESS")
            .ok()
            .or_else(|| std::env::var("BROWSER_ADDRESS").ok());
        let exchange = PolymarketHttpExchange::new(
            Arc::new(http_pool.clone()),
            signer.clone(),
            creds.clone(),
            137,
            maker_override,
        );

        let engine = Arc::new(Engine::new(
            cfg,
            Arc::new(exchange),
            Arc::new(market.clone()),
            Arc::new(user),
            metrics,
        ));

        // 如需同步交易所订单，请在需要时调用 engine.sync_orders_via_rest()，此处不自动调用

        tracing::info!(target: "app", "subscribing to token_id={}", token_id);
        engine.subscribe_assets(vec![token_id]).await?;

        tracing::info!(target: "app", "engine starting...");
        engine.run().await?;
    } else {
        // 没有私钥时，退化成只跑市场流（用户流用 Mock）
        let user = MockUser;
        let engine = Arc::new(Engine::new(
            cfg,
            Arc::new(MockExchange),
            Arc::new(market.clone()),
            Arc::new(user),
            metrics,
        ));

        tracing::info!(target: "app", "subscribing to token_id={}", token_id);
        engine.subscribe_assets(vec![token_id]).await?;

        tracing::info!(target: "app", "engine starting (mock user stream)...");
        engine.run().await?;
    }
    Ok(())
}
