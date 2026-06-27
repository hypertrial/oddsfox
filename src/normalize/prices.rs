use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Builder, Int32Builder, RecordBatch, StringBuilder,
    TimestampMillisecondBuilder,
};
use chrono::Utc;

use crate::clob::rest::PriceHistoryPoint;
use crate::error::Result;
use crate::schema::prices as prices_schema;

pub fn prices_batch(
    token_id: &str,
    market_id: Option<&str>,
    points: &[PriceHistoryPoint],
    source: &str,
    fidelity_seconds: Option<i32>,
    run_id: &str,
) -> Result<RecordBatch> {
    let schema = prices_schema::schema();
    let mut token_col = StringBuilder::new();
    let mut market_col = StringBuilder::new();
    let mut ts = TimestampMillisecondBuilder::new();
    let mut price = Float64Builder::new();
    let mut source_col = StringBuilder::new();
    let mut fidelity = Int32Builder::new();
    let mut ingested_at = TimestampMillisecondBuilder::new();
    let mut raw_url = StringBuilder::new();
    let mut raw_sha = StringBuilder::new();
    let mut run_id_col = StringBuilder::new();
    let now = Utc::now().timestamp_millis();

    for point in points {
        token_col.append_value(token_id);
        market_col.append_option(market_id);
        let millis = if point.t > 1_000_000_000_000 {
            point.t
        } else {
            point.t * 1000
        };
        ts.append_value(millis);
        price.append_value(point.p);
        source_col.append_value(source);
        if let Some(f) = fidelity_seconds {
            fidelity.append_value(f);
        } else {
            fidelity.append_null();
        }
        ingested_at.append_value(now);
        raw_url.append_null();
        raw_sha.append_null();
        run_id_col.append_value(run_id);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(token_col.finish()),
        Arc::new(market_col.finish()),
        Arc::new(ts.finish()),
        Arc::new(price.finish()),
        Arc::new(source_col.finish()),
        Arc::new(fidelity.finish()),
        Arc::new(ingested_at.finish()),
        Arc::new(raw_url.finish()),
        Arc::new(raw_sha.finish()),
        Arc::new(run_id_col.finish()),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}
