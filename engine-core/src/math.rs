use std::cmp::Ordering;

use crate::model::{Level, OpenOrder, OrderBook, OrderStatus};

pub const EPSILON: f64 = 1e-9;

pub fn floats_equal(a: f64, b: f64) -> bool {
    (a - b).abs() < EPSILON
}

pub fn floor_to_tick(value: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return value;
    }
    let scaled = (value / tick).floor();
    (scaled * tick).max(0.0)
}

pub fn ceil_to_tick(value: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return value;
    }
    let scaled = (value / tick).ceil();
    (scaled * tick).max(0.0)
}

pub fn order_is_active(order: &OpenOrder) -> bool {
    matches!(order.status, OrderStatus::PendingNew | OrderStatus::Live)
}

pub fn sort_levels(levels: &mut Vec<Level>, is_bid: bool) {
    if is_bid {
        levels.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(Ordering::Equal));
    } else {
        levels.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(Ordering::Equal));
    }
}

pub fn update_levels(levels: &mut Vec<Level>, price: f64, size: f64, is_bid: bool) {
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

pub fn has_active_ask(book: &OrderBook) -> bool {
    book.asks.iter().any(|lvl| lvl.size > EPSILON)
}

pub fn vwap_for_market_buy(book: &OrderBook, target_size: f64) -> Option<f64> {
    if target_size <= EPSILON {
        return None;
    }
    let mut remaining = target_size;
    let mut notional = 0.0;
    for level in &book.asks {
        if level.size <= EPSILON {
            continue;
        }
        let traded = remaining.min(level.size);
        notional += traded * level.price;
        remaining -= traded;
        if remaining <= EPSILON {
            break;
        }
    }
    if remaining > EPSILON {
        None
    } else {
        Some(notional / target_size)
    }
}

pub fn price_for_instant_buy(book: &OrderBook, target_size: f64) -> Option<(f64, f64)> {
    if target_size <= EPSILON {
        return None;
    }
    let mut remaining = target_size;
    let mut worst_price = 0.0;
    let mut notional = 0.0;
    for level in &book.asks {
        if level.size <= EPSILON {
            continue;
        }
        let traded = remaining.min(level.size);
        notional += traded * level.price;
        if level.price > worst_price {
            worst_price = level.price;
        }
        remaining -= traded;
        if remaining <= EPSILON {
            break;
        }
    }
    if remaining > EPSILON {
        None
    } else {
        Some((worst_price, notional))
    }
}
