use rust_decimal::prelude::*;
use rust_decimal::Decimal;

use super::rest::{BookLevelJson, OrderBookResponse};

#[derive(Debug, Clone)]
pub struct ParsedBook {
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub spread: Option<f64>,
    pub midpoint: Option<f64>,
    pub bid_depth_1pct: f64,
    pub ask_depth_1pct: f64,
    pub bid_depth_5pct: f64,
    pub ask_depth_5pct: f64,
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
}

pub fn parse_book(book: &OrderBookResponse) -> ParsedBook {
    let bids = parse_levels(book.bids.as_deref().unwrap_or(&[]));
    let asks = parse_levels(book.asks.as_deref().unwrap_or(&[]));
    let best_bid = bids.first().map(|(p, _)| *p);
    let best_ask = asks.first().map(|(p, _)| *p);
    let spread = match (best_bid, best_ask) {
        (Some(bid), Some(ask)) => Some(ask - bid),
        _ => None,
    };
    let midpoint = match (best_bid, best_ask) {
        (Some(bid), Some(ask)) => Some((bid + ask) / 2.0),
        _ => None,
    };
    let mid = midpoint.unwrap_or(0.5);
    ParsedBook {
        best_bid,
        best_ask,
        spread,
        midpoint,
        bid_depth_1pct: depth_within_pct(&bids, mid, 0.01, true),
        ask_depth_1pct: depth_within_pct(&asks, mid, 0.01, false),
        bid_depth_5pct: depth_within_pct(&bids, mid, 0.05, true),
        ask_depth_5pct: depth_within_pct(&asks, mid, 0.05, false),
        bids,
        asks,
    }
}

fn parse_levels(levels: &[BookLevelJson]) -> Vec<(f64, f64)> {
    let mut parsed: Vec<(f64, f64)> = levels
        .iter()
        .filter_map(|level| {
            let price = level.price.parse::<Decimal>().ok()?.to_f64()?;
            let size = level.size.parse::<Decimal>().ok()?.to_f64()?;
            Some((price, size))
        })
        .collect();
    parsed.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    parsed
}

fn depth_within_pct(levels: &[(f64, f64)], mid: f64, pct: f64, is_bid: bool) -> f64 {
    levels
        .iter()
        .filter(|(price, _)| {
            if is_bid {
                *price >= mid - pct
            } else {
                *price <= mid + pct
            }
        })
        .map(|(_, size)| size)
        .sum()
}

pub fn slippage(levels: &[(f64, f64)], notional: f64, is_buy: bool) -> Option<f64> {
    let mut remaining = notional;
    let mut filled = 0.0;
    let mut cost = 0.0;
    let walk = if is_buy {
        levels.iter().rev().collect::<Vec<_>>()
    } else {
        levels.iter().collect()
    };
    for (price, size) in walk {
        let level_notional = price * size;
        if level_notional >= remaining {
            filled += remaining / price;
            cost += remaining;
            break;
        }
        filled += size;
        cost += level_notional;
        remaining -= level_notional;
    }
    if filled <= 0.0 {
        return None;
    }
    let avg = cost / filled;
    let ref_price = if is_buy {
        levels.last().map(|(p, _)| *p)?
    } else {
        levels.first().map(|(p, _)| *p)?
    };
    Some((avg - ref_price).abs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spread_from_levels() {
        let book = OrderBookResponse {
            hash: None,
            market: None,
            asset_id: None,
            timestamp: None,
            bids: Some(vec![BookLevelJson {
                price: "0.48".into(),
                size: "100".into(),
            }]),
            asks: Some(vec![BookLevelJson {
                price: "0.52".into(),
                size: "100".into(),
            }]),
            min_order_size: None,
            tick_size: None,
            neg_risk: None,
        };
        let parsed = parse_book(&book);
        assert!((parsed.spread.unwrap() - 0.04).abs() < 1e-9);
        assert!((parsed.midpoint.unwrap() - 0.5).abs() < 1e-9);
    }
}
