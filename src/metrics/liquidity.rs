use std::sync::Arc;

use arrow::array::{
    Float64Builder, Int32Builder, RecordBatch, StringBuilder, TimestampMillisecondBuilder,
};
use chrono::Utc;

use crate::config::Table;
use crate::duckdb_engine::{open_connection, read_parquet_sql};
use crate::error::Result;
use crate::paths::LakePaths;
use crate::schema::metrics as metrics_schema;

pub fn compute_liquidity_metrics(out: &std::path::Path, active_only: bool) -> Result<i64> {
    let paths = LakePaths::new(out);
    let glob = paths.duckdb_parquet_glob(Table::Orderbooks);
    let source = read_parquet_sql(&glob);
    let conn = open_connection(None)?;
    let filter = if active_only {
        let markets_source = read_parquet_sql(&paths.duckdb_parquet_glob(Table::Markets));
        format!(
            "WHERE ob.market_id IN (SELECT market_id FROM {markets_source} WHERE active = true)"
        )
    } else {
        String::new()
    };
    let sql = format!(
        "SELECT snapshot_id, token_id, market_id, spread, midpoint, bid_depth_1pct, ask_depth_1pct
         FROM {source} ob {filter}"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<f64>>(3)?,
            row.get::<_, Option<f64>>(4)?,
            row.get::<_, Option<f64>>(5)?,
            row.get::<_, Option<f64>>(6)?,
        ))
    })?;

    let schema = metrics_schema::schema();
    let mut metric_name = StringBuilder::new();
    let mut market_id = StringBuilder::new();
    let mut token_id = StringBuilder::new();
    let mut ts = TimestampMillisecondBuilder::new();
    let mut value = Float64Builder::new();
    let mut window_seconds = Int32Builder::new();
    let mut source_version = StringBuilder::new();
    let now = Utc::now().timestamp_millis();

    for row in rows {
        let row = row?;
        let (_snapshot, token, market, spread, midpoint, bid_depth, ask_depth) = row;
        append_metric(
            &mut metric_name,
            &mut market_id,
            &mut token_id,
            &mut ts,
            &mut value,
            &mut window_seconds,
            &mut source_version,
            "spread",
            market.as_deref(),
            Some(&token),
            now,
            spread,
        );
        if let (Some(sp), Some(mid)) = (spread, midpoint) {
            if mid > 0.0 {
                append_metric(
                    &mut metric_name,
                    &mut market_id,
                    &mut token_id,
                    &mut ts,
                    &mut value,
                    &mut window_seconds,
                    &mut source_version,
                    "relative_spread",
                    market.as_deref(),
                    Some(&token),
                    now,
                    Some(sp / mid),
                );
            }
        }
        append_metric(
            &mut metric_name,
            &mut market_id,
            &mut token_id,
            &mut ts,
            &mut value,
            &mut window_seconds,
            &mut source_version,
            "bid_depth_1pct",
            market.as_deref(),
            Some(&token),
            now,
            bid_depth,
        );
        append_metric(
            &mut metric_name,
            &mut market_id,
            &mut token_id,
            &mut ts,
            &mut value,
            &mut window_seconds,
            &mut source_version,
            "ask_depth_1pct",
            market.as_deref(),
            Some(&token),
            now,
            ask_depth,
        );
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(metric_name.finish()),
            Arc::new(market_id.finish()),
            Arc::new(token_id.finish()),
            Arc::new(ts.finish()),
            Arc::new(value.finish()),
            Arc::new(window_seconds.finish()),
            Arc::new(source_version.finish()),
        ],
    )?;
    let rows = batch.num_rows() as i64;
    if rows > 0 {
        crate::parquet::write_gold(&paths, "metric_points", "liquidity", &[batch])?;
    }
    Ok(rows)
}

#[allow(clippy::too_many_arguments)]
fn append_metric(
    metric_name: &mut StringBuilder,
    market_id: &mut StringBuilder,
    token_id: &mut StringBuilder,
    ts: &mut TimestampMillisecondBuilder,
    value: &mut Float64Builder,
    window_seconds: &mut Int32Builder,
    source_version: &mut StringBuilder,
    name: &str,
    market: Option<&str>,
    token: Option<&str>,
    now: i64,
    metric_value: Option<f64>,
) {
    if metric_value.is_none() {
        return;
    }
    metric_name.append_value(name);
    market_id.append_option(market);
    token_id.append_option(token);
    ts.append_value(now);
    value.append_value(metric_value.unwrap());
    window_seconds.append_null();
    source_version.append_value(crate::schema::schema_version());
}

#[cfg(test)]
mod tests {
    use crate::clob::book::{parse_book, slippage};
    use crate::clob::rest::{BookLevelJson, OrderBookResponse};

    #[test]
    fn slippage_positive_for_deep_book() {
        let book = OrderBookResponse {
            hash: None,
            market: None,
            asset_id: None,
            timestamp: None,
            bids: Some(vec![BookLevelJson {
                price: "0.49".into(),
                size: "1000".into(),
            }]),
            asks: Some(vec![BookLevelJson {
                price: "0.51".into(),
                size: "1000".into(),
            }]),
            min_order_size: None,
            tick_size: None,
            neg_risk: None,
        };
        let parsed = parse_book(&book);
        assert!(slippage(&parsed.asks, 100.0, true).is_some());
    }
}
