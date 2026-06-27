use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Builder, Int32Builder, RecordBatch, StringBuilder,
    TimestampMillisecondBuilder,
};
use chrono::Utc;
use uuid::Uuid;

use crate::clob::book::ParsedBook;
use crate::clob::rest::OrderBookResponse;
use crate::error::Result;
use crate::schema::{book_levels as book_levels_schema, orderbooks as orderbooks_schema};

pub struct SnapshotRecord {
    pub snapshot_id: String,
    pub token_id: String,
    pub market_id: Option<String>,
    pub book: OrderBookResponse,
    pub parsed: ParsedBook,
}

pub fn orderbooks_batch(records: &[SnapshotRecord], source: &str, run_id: &str) -> Result<RecordBatch> {
    let schema = orderbooks_schema::schema();
    let mut snapshot_id = StringBuilder::new();
    let mut token_id = StringBuilder::new();
    let mut market_id = StringBuilder::new();
    let mut ts = TimestampMillisecondBuilder::new();
    let mut book_hash = StringBuilder::new();
    let mut best_bid = Float64Builder::new();
    let mut best_ask = Float64Builder::new();
    let mut spread = Float64Builder::new();
    let mut midpoint = Float64Builder::new();
    let mut bid_depth_1 = Float64Builder::new();
    let mut ask_depth_1 = Float64Builder::new();
    let mut bid_depth_5 = Float64Builder::new();
    let mut ask_depth_5 = Float64Builder::new();
    let mut raw_json = StringBuilder::new();
    let mut source_col = StringBuilder::new();
    let mut raw_url = StringBuilder::new();
    let mut raw_sha = StringBuilder::new();
    let mut ingested_at = TimestampMillisecondBuilder::new();
    let mut run_id_col = StringBuilder::new();
    let now = Utc::now().timestamp_millis();

    for record in records {
        snapshot_id.append_value(&record.snapshot_id);
        token_id.append_value(&record.token_id);
        market_id.append_option(record.market_id.as_deref());
        ts.append_value(now);
        book_hash.append_option(record.book.hash.as_deref());
        append_opt_f64(&mut best_bid, record.parsed.best_bid);
        append_opt_f64(&mut best_ask, record.parsed.best_ask);
        append_opt_f64(&mut spread, record.parsed.spread);
        append_opt_f64(&mut midpoint, record.parsed.midpoint);
        bid_depth_1.append_value(record.parsed.bid_depth_1pct);
        ask_depth_1.append_value(record.parsed.ask_depth_1pct);
        bid_depth_5.append_value(record.parsed.bid_depth_5pct);
        ask_depth_5.append_value(record.parsed.ask_depth_5pct);
        raw_json.append_value(serde_json::to_string(&record.book).unwrap_or_else(|_| "{}".into()));
        source_col.append_value(source);
        raw_url.append_null();
        raw_sha.append_null();
        ingested_at.append_value(now);
        run_id_col.append_value(run_id);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(snapshot_id.finish()),
        Arc::new(token_id.finish()),
        Arc::new(market_id.finish()),
        Arc::new(ts.finish()),
        Arc::new(book_hash.finish()),
        Arc::new(best_bid.finish()),
        Arc::new(best_ask.finish()),
        Arc::new(spread.finish()),
        Arc::new(midpoint.finish()),
        Arc::new(bid_depth_1.finish()),
        Arc::new(ask_depth_1.finish()),
        Arc::new(bid_depth_5.finish()),
        Arc::new(ask_depth_5.finish()),
        Arc::new(raw_json.finish()),
        Arc::new(source_col.finish()),
        Arc::new(raw_url.finish()),
        Arc::new(raw_sha.finish()),
        Arc::new(ingested_at.finish()),
        Arc::new(run_id_col.finish()),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}

pub fn book_levels_batch(records: &[SnapshotRecord], source: &str, run_id: &str) -> Result<RecordBatch> {
    let schema = book_levels_schema::schema();
    let mut snapshot_id = StringBuilder::new();
    let mut side = StringBuilder::new();
    let mut price = Float64Builder::new();
    let mut size = Float64Builder::new();
    let mut level_index = Int32Builder::new();
    let mut source_col = StringBuilder::new();
    let mut raw_url = StringBuilder::new();
    let mut raw_sha = StringBuilder::new();
    let mut ingested_at = TimestampMillisecondBuilder::new();
    let mut run_id_col = StringBuilder::new();
    let now = Utc::now().timestamp_millis();

    for record in records {
        for (idx, (p, s)) in record.parsed.bids.iter().enumerate() {
            snapshot_id.append_value(&record.snapshot_id);
            side.append_value("bid");
            price.append_value(*p);
            size.append_value(*s);
            level_index.append_value(idx as i32);
            source_col.append_value(source);
            raw_url.append_null();
            raw_sha.append_null();
            ingested_at.append_value(now);
            run_id_col.append_value(run_id);
        }
        for (idx, (p, s)) in record.parsed.asks.iter().enumerate() {
            snapshot_id.append_value(&record.snapshot_id);
            side.append_value("ask");
            price.append_value(*p);
            size.append_value(*s);
            level_index.append_value(idx as i32);
            source_col.append_value(source);
            raw_url.append_null();
            raw_sha.append_null();
            ingested_at.append_value(now);
            run_id_col.append_value(run_id);
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(snapshot_id.finish()),
        Arc::new(side.finish()),
        Arc::new(price.finish()),
        Arc::new(size.finish()),
        Arc::new(level_index.finish()),
        Arc::new(source_col.finish()),
        Arc::new(raw_url.finish()),
        Arc::new(raw_sha.finish()),
        Arc::new(ingested_at.finish()),
        Arc::new(run_id_col.finish()),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}

pub fn new_snapshot_id() -> String {
    Uuid::new_v4().to_string()
}

fn append_opt_f64(builder: &mut Float64Builder, value: Option<f64>) {
    if let Some(v) = value {
        builder.append_value(v);
    } else {
        builder.append_null();
    }
}
