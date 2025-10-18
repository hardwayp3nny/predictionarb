use crate::{
    config::EngineConfig,
    counter::EngineCounter,
    metrics::Metrics,
    model::*,
    oms::Oms,
    ports::{
        BooksSnapshot, ExchangeClient, MarketStream, OrdersSnapshot, PositionsSnapshot, UserStream,
    },
};
use anyhow::Result;
use dashmap::DashMap;
use engine_core::strategy::{
    PrimingRequest, StartAction, Strategy, StrategyConfig, StrategyContext,
};
use std::sync::Arc;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{info, warn};

pub struct StrategyRunner<R, M, U>
where
    R: ExchangeClient + BooksSnapshot + OrdersSnapshot + Send + Sync + 'static,
    M: MarketStream + Send + Sync + 'static,
    U: UserStream + Send + Sync + 'static,
{
    cfg: EngineConfig,
    exchange: Arc<R>,
    market: Arc<M>,
    user: Arc<U>,
    metrics: Arc<Metrics>,
    oms_by_token: Arc<DashMap<String, Oms<R>>>,
}

impl<R, M, U> StrategyRunner<R, M, U>
where
    R: ExchangeClient + BooksSnapshot + OrdersSnapshot + PositionsSnapshot + Send + Sync + 'static,
    M: MarketStream + Send + Sync + 'static,
    U: UserStream + Send + Sync + 'static,
{
    pub fn new(
        cfg: EngineConfig,
        exchange: Arc<R>,
        market: Arc<M>,
        user: Arc<U>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            cfg,
            exchange,
            market,
            user,
            metrics,
            oms_by_token: Arc::new(DashMap::new()),
        }
    }

    pub async fn run<S>(&self, config: StrategyConfig) -> Result<()>
    where
        S: Strategy,
    {
        let mut strategy = S::new(config)?;
        let counter = Arc::new(EngineCounter::new(
            self.exchange.clone(),
            self.metrics.clone(),
            self.oms_by_token.clone(),
        ));

        let ctx = StrategyContext::new(counter.clone());
        let start_action = strategy.on_start(&ctx).await?;
        self.apply_start_action(&start_action).await?;

        let mut enable_user_stream = start_action.subscribe_user_stream;

        let (ev_tx, mut ev_rx) = mpsc::channel::<MarketEvent>(10_000);
        let (usr_tx, mut usr_rx) = mpsc::channel::<UserEvent>(10_000);

        let metrics = self.metrics.clone();
        let market = self.market.clone();
        let market_loop: JoinHandle<Result<()>> = tokio::spawn(async move {
            info!(target: "runner", "market stream started");
            loop {
                match market.next().await? {
                    Some(ev) => {
                        let _ = ev_tx.send(ev).await;
                        metrics.ws_events.inc();
                    }
                    None => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
                }
            }
        });

        let user_loop: Option<JoinHandle<Result<()>>> = if enable_user_stream {
            let user = self.user.clone();
            let metrics_u = self.metrics.clone();
            Some(tokio::spawn(async move {
                info!(target: "runner", "user stream started");
                user.connect().await?;
                loop {
                    match user.next().await? {
                        Some(ev) => {
                            let _ = usr_tx.send(ev).await;
                            metrics_u.ws_events.inc();
                        }
                        None => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
                    }
                }
                #[allow(unreachable_code)]
                Ok::<(), anyhow::Error>(())
            }))
        } else {
            None
        };

        let mut stats_iv = tokio::time::interval(std::time::Duration::from_secs(10));
        let mut stat_books = 0u64;
        let mut stat_depth = 0u64;
        let mut stat_ticks = 0u64;
        let mut stat_trades = 0u64;

        let mut running = true;
        while running {
            tokio::select! {
                result = ev_rx.recv() => {
                    match result {
                        Some(ev) => match ev {
                            MarketEvent::OrderBook(ref ob) => {
                                stat_books += 1;
                                strategy.on_order_book(ob);
                            }
                            MarketEvent::DepthUpdate(ref depth) => {
                                stat_depth += 1;
                                strategy.on_depth_update(depth);
                            }
                            MarketEvent::TickSizeChange(ref tick) => {
                                stat_ticks += 1;
                                strategy.on_tick_size_update(tick);
                            }
                            MarketEvent::PublicTrade(ref trade) => {
                                stat_trades += 1;
                                strategy.on_public_trade(trade);
                            }
                        },
                        None => running = false,
                    }
                }
                result = usr_rx.recv(), if enable_user_stream => {
                    match result {
                        Some(user_ev) => match &user_ev {
                            UserEvent::Order(update) => {
                                let token = update.asset_id.clone();
                                self.ensure_oms(token).store().on_user_event(&user_ev);
                                strategy.on_order_update(update);
                            }
                            UserEvent::Trade(trade) => {
                                let token = trade.asset_id.clone();
                                self.ensure_oms(token).store().on_user_event(&user_ev);
                                strategy.on_user_trade(trade);
                            }
                        },
                        None => running = false,
                    }
                }
                _ = stats_iv.tick() => {
                    info!(target: "runner", books=%stat_books, depth=%stat_depth, ticks=%stat_ticks, trades=%stat_trades, "market stats (10s)");
                    stat_books = 0;
                    stat_depth = 0;
                    stat_ticks = 0;
                    stat_trades = 0;
                }
            }
        }

        drop(counter);
        let _ = market_loop.abort();
        if let Some(h) = user_loop {
            let _ = h.abort();
        }

        Ok(())
    }

    async fn apply_start_action(&self, action: &StartAction) -> Result<()> {
        if !action.subscribe_tokens.is_empty() {
            self.market
                .subscribe(action.subscribe_tokens.clone())
                .await?;
        }
        for request in &action.priming_requests {
            match request {
                PrimingRequest::SyncOpenOrders => {
                    if let Err(err) = self.sync_orders_via_rest().await {
                        warn!(target: "runner", ?err, "failed to sync orders during start");
                    }
                }
                PrimingRequest::SyncPositions => {
                    if let Err(err) = self.sync_positions_via_rest().await {
                        warn!(target: "runner", ?err, "failed to sync positions during start");
                    }
                }
                PrimingRequest::Custom(name) => {
                    warn!(target: "runner", request=%name, "custom priming request unhandled");
                }
            }
        }
        Ok(())
    }

    async fn sync_orders_via_rest(&self) -> Result<usize> {
        let orders = self.exchange.fetch_orders().await?;
        let mut n = 0usize;
        for order in orders.into_iter() {
            let token = order.asset_id.clone();
            let oms = self.ensure_oms(token);
            oms.store().upsert_snapshot(order);
            n += 1;
        }
        Ok(n)
    }

    async fn sync_positions_via_rest(&self) -> Result<usize> {
        let positions = self.exchange.fetch_positions().await?;
        let mut n = 0usize;
        for pos in positions.into_iter() {
            let token = pos.asset_id.clone();
            let oms = self.ensure_oms(token);
            oms.store().set_position(pos);
            n += 1;
        }
        Ok(n)
    }

    fn ensure_oms(&self, token_id: String) -> Oms<R> {
        if let Some(oms) = self.oms_by_token.get(&token_id) {
            return oms.clone();
        }
        let oms = Oms::new_for_token(self.exchange.clone(), token_id.clone());
        self.oms_by_token.insert(token_id, oms.clone());
        oms
    }
}
