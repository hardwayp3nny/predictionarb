use std::time::Instant;

#[derive(Clone, Default)]
pub struct BookSide {
    pub levels: Vec<(f64, f64)>, // (price, size)
}

impl BookSide {
    pub fn sort_bid_desc(&mut self) {
        self.levels
            .sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    }
    pub fn sort_ask_asc(&mut self) {
        self.levels
            .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    }
}

#[derive(Clone, Default)]
pub struct OrderBookState {
    pub asset_id: String,
    pub best_bid: f64,
    pub best_ask: f64,
    pub bids: BookSide,
    pub asks: BookSide,
    pub last_update: Option<Instant>,
}

impl OrderBookState {
    pub fn new(asset_id: String) -> Self {
        Self {
            asset_id,
            ..Default::default()
        }
    }
    pub fn apply_snapshot(&mut self, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        self.bids.levels = bids.to_vec();
        self.asks.levels = asks.to_vec();
        self.bids.sort_bid_desc();
        self.asks.sort_ask_asc();
        self.best_bid = self.bids.levels.first().map(|x| x.0).unwrap_or(0.0);
        self.best_ask = self
            .asks
            .levels
            .first()
            .map(|x| x.0)
            .unwrap_or(f64::INFINITY);
        self.last_update = Some(Instant::now());
    }
    pub fn apply_delta(&mut self, price: f64, size: f64, is_bid: bool) {
        let side = if is_bid {
            &mut self.bids.levels
        } else {
            &mut self.asks.levels
        };
        if let Some(idx) = side.iter().position(|(p, _s)| (*p - price).abs() <= 1e-12) {
            if size <= 1e-18 {
                side.remove(idx);
            } else {
                side[idx] = (price, size);
            }
        } else if size > 1e-18 {
            side.push((price, size));
        }
        if is_bid {
            self.bids.sort_bid_desc();
        } else {
            self.asks.sort_ask_asc();
        }
        self.best_bid = self.bids.levels.first().map(|x| x.0).unwrap_or(0.0);
        self.best_ask = self
            .asks
            .levels
            .first()
            .map(|x| x.0)
            .unwrap_or(f64::INFINITY);
        self.last_update = Some(Instant::now());
    }
}
