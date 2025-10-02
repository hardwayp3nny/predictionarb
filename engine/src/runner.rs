use crate::{
    config::EngineConfig,
    metrics::Metrics,
    model::*,
    oms::{Oms, OmsApi},
    ports::*,
    strategy::Strategy,
};
use anyhow::Result;
use dashmap::DashMap;
use futures::future::join3;
use parking_lot::RwLock;
use std::cmp::min;
use std::sync::Arc;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::info;

pub struct Engine<R: ExchangeClient + 'static, M: MarketStream + 'static, U: UserStream + 'static> {
    cfg: EngineConfig,
    exchange: Arc<R>,
    market: Arc<M>,
    user: Arc<U>,
    oms_by_token: DashMap<String, Oms<R>>, // 每个 token 一个 OMS
    metrics: Arc<Metrics>,
    strategy: Arc<RwLock<Option<Arc<dyn Strategy>>>>, // 可选策略（可运行期设置）
}

impl<R: ExchangeClient, M: MarketStream, U: UserStream> Engine<R, M, U> {
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
            oms_by_token: DashMap::new(),
            metrics,
            strategy: Arc::new(RwLock::new(None)),
        }
    }

    pub fn with_strategy(mut self, strategy: Arc<dyn Strategy>) -> Self {
        *self.strategy.write() = Some(strategy);
        self
    }

    pub fn set_strategy(&self, strategy: Arc<dyn Strategy>) {
        *self.strategy.write() = Some(strategy);
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!(target: "engine", "runner starting");
        // 若配置了策略，先获取订阅列表并订阅
        if let Some(strat) = self.strategy.read().as_ref().cloned() {
            let subs = strat.on_start().await;
            if !subs.is_empty() {
                self.subscribe_assets(subs).await?;
            }
        }
        let (ev_tx, mut ev_rx) = mpsc::channel::<MarketEvent>(10_000);
        let (usr_tx, mut usr_rx) = mpsc::channel::<UserEvent>(10_000);

        let market = self.market.clone();
        let user = self.user.clone();
        let metrics = self.metrics.clone();
        let h_market: JoinHandle<Result<()>> = tokio::spawn(async move {
            tracing::info!(target: "engine", "market loop started");
            loop {
                match market.next().await? {
                    Some(ev) => {
                        let _ = ev_tx.send(ev).await;
                        metrics.ws_events.inc();
                    }
                    None => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
                }
            }
        });

        let metrics_u = self.metrics.clone();
        let h_user: JoinHandle<Result<()>> = tokio::spawn(async move {
            tracing::info!(target: "engine", "user loop started");
            user.connect().await?;
            loop {
                match user.next().await? {
                    Some(ev) => {
                        let _ = usr_tx.send(ev).await;
                        metrics_u.ws_events.inc();
                    }
                    None => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
                }
            }
        });

        let exchange = self.exchange.clone();
        let oms_map = self.oms_by_token.clone();
        let metrics_e = self.metrics.clone();
        let strategy = self.strategy.clone();
        let h_core: JoinHandle<Result<()>> = tokio::spawn(async move {
            // market event stats (10s window)
            let mut stat_books: u64 = 0;
            let mut stat_depth: u64 = 0;
            let mut stat_ticks: u64 = 0;
            let mut stat_trades: u64 = 0;
            let mut stats_iv = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                tokio::select! {
                    Some(ev) = ev_rx.recv() => {
                        match &ev {
                            MarketEvent::OrderBook(ob) => {
                                stat_books += 1;
                                if let Some(strat) = strategy.read().as_ref().cloned() { let e = ob.clone(); tokio::spawn(async move { strat.on_order_book(e).await; }); }
                            }
                            MarketEvent::DepthUpdate(d) => {
                                stat_depth += 1;
                                if let Some(strat) = strategy.read().as_ref().cloned() { let e = d.clone(); tokio::spawn(async move { strat.on_depth_update(e).await; }); }
                            },
                            MarketEvent::TickSizeChange(t) => {
                                stat_ticks += 1;
                                if let Some(strat) = strategy.read().as_ref().cloned() { let e = t.clone(); tokio::spawn(async move { strat.on_tick_size_change(e).await; }); }
                            },
                            MarketEvent::PublicTrade(tr) => {
                                stat_trades += 1;
                                if let Some(strat) = strategy.read().as_ref().cloned() { let e = tr.clone(); tokio::spawn(async move { strat.on_public_trade(e).await; }); }
                            },
                        }
                    }
                    _ = stats_iv.tick() => {
                        info!(target: "engine", "Market WSS stats (last 10s): books={}, depth_updates={}, tick_changes={}, public_trades={}", stat_books, stat_depth, stat_ticks, stat_trades);
                        stat_books = 0; stat_depth = 0; stat_ticks = 0; stat_trades = 0;
                    }
                    Some(uev) = usr_rx.recv() => {
                        // 路由用户事件到对应 token 的 OMS
                        match &uev {
                            UserEvent::Order(upd) => {
                                let token = upd.asset_id.clone();
                                let oms = ensure_oms_for_token(&oms_map, exchange.clone(), token);
                                oms.on_user_event(&uev);
                                if let Some(strat) = strategy.read().as_ref().cloned() { let e = upd.clone(); tokio::spawn(async move { strat.on_order_update(e).await; }); }
                            }
                            UserEvent::Trade(tr) => {
                                let token = tr.asset_id.clone();
                                let oms = ensure_oms_for_token(&oms_map, exchange.clone(), token);
                                oms.on_user_event(&uev);
                                if let Some(strat) = strategy.read().as_ref().cloned() { let e = tr.clone(); tokio::spawn(async move { strat.on_user_trade(e).await; }); }
                            }
                        }
                    }
                    else => { break; }
                }
            }
            Ok(())
        });

        let _ = join3(h_market, h_user, h_core).await;
        Ok(())
    }

    pub async fn submit_orders_bulk(
        &self,
        batch: Vec<(OrderArgs, OrderType, CreateOrderOptions)>,
    ) -> Result<Vec<OrderAck>> {
        self.metrics.orders_sent.inc_by(batch.len() as u64);
        for _ in 0..batch.len() {
            self.metrics.inflight_orders.inc();
        }
        let mut acks: Vec<OrderAck> = Vec::new();
        let mut idx = 0usize;
        while idx < batch.len() {
            let end = min(idx + 15, batch.len());
            let chunk = batch[idx..end].to_vec();
            // 调用底层批量接口（1次请求 ≤ 15）
            let res = self.exchange.create_orders_batch(chunk.clone()).await?;
            // 将 ack 写入对应 token 的 OMS
            for (i, ack) in res.iter().enumerate() {
                if let Some(order_id) = ack.order_id.clone() {
                    let (args, _t, _o) = &batch[idx + i];
                    let oms = self.ensure_oms(args.token_id.clone());
                    oms.store().upsert_pending(order_id.clone(), args);
                    oms.store().on_ack(ack);
                }
                // 打印提交结果：逐条 ACK（便于排查）
                if let Some((args, _t, _o)) = batch.get(idx + i) {
                    tracing::info!(
                        target: "engine",
                        token=%args.token_id,
                        price=%args.price,
                        size=%args.size,
                        side=?args.side,
                        success=%ack.success,
                        order_id=?ack.order_id,
                        error=?ack.error_message,
                        "submit_orders_bulk ack"
                    );
                } else {
                    tracing::info!(target: "engine", success=%ack.success, order_id=?ack.order_id, error=?ack.error_message, "submit_orders_bulk ack (index out of range)");
                }
            }
            // 批次小结
            let succ = res.iter().filter(|a| a.success).count();
            let fail = res.len().saturating_sub(succ);
            tracing::info!(target: "engine", batch_start=%idx, batch_end=%end, success=%succ, failed=%fail, "submit_orders_bulk chunk result");
            acks.extend(res);
            idx = end;
        }
        for _ in 0..acks.len() {
            self.metrics.inflight_orders.dec();
        }
        Ok(acks)
    }

    pub async fn cancel_orders(&self, ids: Vec<String>) -> Result<CancelAck> {
        self.exchange.cancel_orders(ids).await
    }

    pub async fn subscribe_assets(&self, asset_ids: Vec<String>) -> Result<()> {
        self.market.subscribe(asset_ids).await
    }

    // 通过 REST 同步交易所订单并写入各 token 的 OMS
    pub async fn sync_orders_via_rest(&self) -> Result<usize>
    where
        R: OrdersSnapshot,
    {
        let orders = self.exchange.fetch_orders().await?;
        let mut n = 0usize;
        for o in orders.into_iter() {
            let token = o.asset_id.clone();
            let oms = self.ensure_oms(token);
            oms.store().upsert_snapshot(o);
            n += 1;
        }
        Ok(n)
    }

    // 通过 REST 同步仓位并写入各 token 的 OMS
    pub async fn sync_positions_via_rest(&self) -> Result<usize>
    where
        R: PositionsSnapshot,
    {
        let poss = self.exchange.fetch_positions().await?;
        let mut n = 0usize;
        for p in poss.into_iter() {
            let token = p.asset_id.clone();
            let oms = self.ensure_oms(token);
            oms.store().set_position(p);
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

    pub fn list_orders_by_token(&self, token_id: &str) -> Vec<OpenOrder> {
        if let Some(oms) = self.oms_by_token.get(token_id) {
            return oms.store().list_by_asset(token_id);
        }
        Vec::new()
    }
}

fn ensure_oms_for_token<R: ExchangeClient + 'static>(
    map: &DashMap<String, Oms<R>>,
    ex: Arc<R>,
    token: String,
) -> Oms<R> {
    if let Some(oms) = map.get(&token) {
        return oms.clone();
    }
    let oms = Oms::new_for_token(ex.clone(), token.clone());
    map.insert(token, oms.clone());
    oms
}
