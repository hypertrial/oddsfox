use serde::{Deserialize, Serialize};

fn de_opt_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Number(number)) => number
            .as_f64()
            .ok_or_else(|| serde::de::Error::custom("invalid number"))
            .map(Some),
        Some(serde_json::Value::String(raw)) if raw.trim().is_empty() => Ok(None),
        Some(serde_json::Value::String(raw)) => raw
            .parse::<f64>()
            .map(Some)
            .map_err(serde::de::Error::custom),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected number or string, got {other}"
        ))),
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct KalshiMarketResponse {
    #[serde(default)]
    pub markets: Vec<KalshiMarket>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct KalshiEventEnvelope {
    pub event: Option<KalshiEvent>,
    #[serde(default)]
    pub markets: Vec<KalshiMarket>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct KalshiEvent {
    pub event_ticker: Option<String>,
    pub series_ticker: Option<String>,
    pub ticker: Option<String>,
    pub title: Option<String>,
    pub sub_title: Option<String>,
    pub category: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct KalshiMarket {
    pub ticker: String,
    pub event_ticker: Option<String>,
    pub series_ticker: Option<String>,
    pub title: Option<String>,
    pub subtitle: Option<String>,
    pub sub_title: Option<String>,
    pub status: Option<String>,
    pub yes_sub_title: Option<String>,
    pub no_sub_title: Option<String>,
    pub open_time: Option<String>,
    pub close_time: Option<String>,
    pub expiration_time: Option<String>,
    pub settlement_timer_seconds: Option<i64>,
    pub settlement_time: Option<String>,
    pub settlement_ts: Option<i64>,
    pub result: Option<String>,
    pub volume: Option<f64>,
    pub volume_24h: Option<f64>,
    pub liquidity: Option<f64>,
    pub open_interest: Option<f64>,
    pub yes_bid: Option<f64>,
    pub yes_ask: Option<f64>,
    pub no_bid: Option<f64>,
    pub no_ask: Option<f64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct KalshiCandlestickResponse {
    #[serde(default)]
    pub candlesticks: Vec<KalshiCandlestick>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct KalshiCandlestick {
    pub end_period_ts: Option<i64>,
    pub start_period_ts: Option<i64>,
    pub price: Option<KalshiCandlePrice>,
    pub yes_bid: Option<KalshiCandlePrice>,
    pub yes_ask: Option<KalshiCandlePrice>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct KalshiCandlePrice {
    pub close: Option<f64>,
    pub close_dollars: Option<f64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct KalshiTradesResponse {
    #[serde(default)]
    pub trades: Vec<KalshiTrade>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct KalshiFillsResponse {
    #[serde(default)]
    pub fills: Vec<KalshiFill>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct KalshiFill {
    pub trade_id: Option<String>,
    pub fill_id: Option<String>,
    pub order_id: Option<String>,
    pub ticker: Option<String>,
    pub market_ticker: Option<String>,
    pub action: Option<String>,
    pub side: Option<String>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub yes_price: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub yes_price_dollars: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub count: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub count_fp: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub fee: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub fee_dollars: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub fee_cost: Option<f64>,
    pub created_time: Option<String>,
    pub created_ts: Option<i64>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct KalshiPositionsResponse {
    #[serde(default)]
    pub market_positions: Vec<KalshiPosition>,
    #[serde(default)]
    pub positions: Vec<KalshiPosition>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct KalshiPosition {
    pub ticker: Option<String>,
    pub market_ticker: Option<String>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub position: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub position_fp: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub yes_count: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub yes_count_fp: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub no_count: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub no_count_fp: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub market_exposure: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub market_exposure_dollars: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub realized_pnl: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub realized_pnl_dollars: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub total_traded: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub resting_order_count: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub fees_paid: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub fees_paid_dollars: Option<f64>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct KalshiTrade {
    pub trade_id: Option<String>,
    pub ticker: Option<String>,
    pub market_ticker: Option<String>,
    pub created_time: Option<String>,
    pub created_ts: Option<i64>,
    pub yes_price: Option<f64>,
    pub yes_price_dollars: Option<f64>,
    pub count: Option<f64>,
    pub count_fp: Option<f64>,
    pub taker_side: Option<String>,
    pub is_block_trade: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct KalshiOrderbookEnvelope {
    pub orderbook: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct HistoricalCutoff {
    pub cutoff_ts: Option<i64>,
    pub cutoff_time: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_market_event_trade_candle_and_cutoff() {
        let markets: KalshiMarketResponse = serde_json::from_value(serde_json::json!({
            "markets": [{
                "ticker": "KXTEST-26",
                "event_ticker": "KXTEST",
                "series_ticker": "KX",
                "title": "Will it happen?",
                "status": "open",
                "volume": 12,
                "yes_bid": 48,
                "yes_ask": 52
            }],
            "cursor": "next"
        }))
        .unwrap();
        assert_eq!(markets.markets[0].ticker, "KXTEST-26");

        let event: KalshiEventEnvelope = serde_json::from_value(serde_json::json!({
            "event": {"event_ticker": "KXTEST", "title": "Event"},
            "markets": [{"ticker": "KXTEST-26"}]
        }))
        .unwrap();
        assert_eq!(event.markets.len(), 1);

        let trades: KalshiTradesResponse = serde_json::from_value(serde_json::json!({
            "trades": [{
                "trade_id": "t1",
                "ticker": "KXTEST-26",
                "yes_price_dollars": 0.61,
                "count_fp": 2.5,
                "created_time": "2026-01-01T00:00:00Z",
                "is_block_trade": false
            }]
        }))
        .unwrap();
        assert_eq!(trades.trades[0].trade_id.as_deref(), Some("t1"));

        let candles: KalshiCandlestickResponse = serde_json::from_value(serde_json::json!({
            "candlesticks": [{
                "end_period_ts": 1700000000,
                "price": {"close_dollars": 0.4}
            }]
        }))
        .unwrap();
        assert_eq!(candles.candlesticks[0].end_period_ts, Some(1700000000));

        let cutoff: HistoricalCutoff =
            serde_json::from_value(serde_json::json!({"cutoff_ts": 1700000000})).unwrap();
        assert_eq!(cutoff.cutoff_ts, Some(1700000000));
    }

    #[test]
    fn parses_portfolio_string_decimals() {
        let fills: KalshiFillsResponse = serde_json::from_value(serde_json::json!({
            "fills": [{
                "fill_id": "f1",
                "ticker": "KXTEST-26",
                "count_fp": "2.5",
                "fee_cost": "0.01",
                "yes_price_dollars": "0.61"
            }]
        }))
        .unwrap();
        assert_eq!(fills.fills[0].count_fp, Some(2.5));
        assert_eq!(fills.fills[0].fee_cost, Some(0.01));

        let positions: KalshiPositionsResponse = serde_json::from_value(serde_json::json!({
            "market_positions": [{
                "ticker": "KXTEST-26",
                "position_fp": "3.0",
                "market_exposure_dollars": "1.25",
                "realized_pnl_dollars": "0.12",
                "fees_paid_dollars": "0.02"
            }]
        }))
        .unwrap();
        assert_eq!(positions.market_positions[0].position_fp, Some(3.0));
        assert_eq!(
            positions.market_positions[0].market_exposure_dollars,
            Some(1.25)
        );
        assert_eq!(
            positions.market_positions[0].realized_pnl_dollars,
            Some(0.12)
        );
    }
}
