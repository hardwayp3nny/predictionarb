use prometheus::{IntCounter, IntGauge, Opts, Registry};
use std::sync::Arc;

#[derive(Clone)]
pub struct Metrics {
    pub orders_sent: IntCounter,
    pub orders_succeeded: IntCounter,
    pub orders_failed: IntCounter,
    pub ws_events: IntCounter,
    pub inflight_orders: IntGauge,
}

impl Metrics {
    pub fn new(registry: &Registry) -> Arc<Self> {
        let orders_sent =
            IntCounter::with_opts(Opts::new("orders_sent", "Total orders sent")).unwrap();
        let orders_succeeded =
            IntCounter::with_opts(Opts::new("orders_succeeded", "Orders succeeded")).unwrap();
        let orders_failed =
            IntCounter::with_opts(Opts::new("orders_failed", "Orders failed")).unwrap();
        let ws_events = IntCounter::with_opts(Opts::new("ws_events", "Total ws events")).unwrap();
        let inflight_orders =
            IntGauge::with_opts(Opts::new("inflight_orders", "Inflight orders")).unwrap();
        registry.register(Box::new(orders_sent.clone())).ok();
        registry.register(Box::new(orders_succeeded.clone())).ok();
        registry.register(Box::new(orders_failed.clone())).ok();
        registry.register(Box::new(ws_events.clone())).ok();
        registry.register(Box::new(inflight_orders.clone())).ok();
        Arc::new(Self {
            orders_sent,
            orders_succeeded,
            orders_failed,
            ws_events,
            inflight_orders,
        })
    }
}
