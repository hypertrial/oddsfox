use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Builder, RecordBatch, StringBuilder, TimestampMillisecondBuilder,
};
use chrono::Utc;

use crate::data::DataTrade;
use crate::error::Result;
use crate::schema::trades as trades_schema;

pub fn trades_batch(
    trades: &[DataTrade],
    source: &str,
    run_id: &str,
) -> Result<RecordBatch> {
    let schema = trades_schema::schema();
    let mut trade_id = StringBuilder::new();
    let mut market_id = StringBuilder::new();
    let mut token_id = StringBuilder::new();
    let mut ts = TimestampMillisecondBuilder::new();
    let mut price = Float64Builder::new();
    let mut size = Float64Builder::new();
    let mut side = StringBuilder::new();
    let mut tx_hash = StringBuilder::new();
    let mut maker = StringBuilder::new();
    let mut taker = StringBuilder::new();
    let mut raw_json = StringBuilder::new();
    let mut source_col = StringBuilder::new();
    let mut raw_url = StringBuilder::new();
    let mut raw_sha = StringBuilder::new();
    let mut ingested_at = TimestampMillisecondBuilder::new();
    let mut run_id_col = StringBuilder::new();
    let now = Utc::now().timestamp_millis();

    for trade in trades {
        trade_id.append_option(trade.id.as_deref().or(Some("unknown")));
        market_id.append_option(trade.market.as_deref());
        token_id.append_option(trade.asset_id.as_deref());
        let millis = trade.timestamp.unwrap_or(now / 1000) * 1000;
        ts.append_value(millis);
        if let Some(p) = trade.price {
            price.append_value(p);
        } else {
            price.append_null();
        }
        if let Some(s) = trade.size {
            size.append_value(s);
        } else {
            size.append_null();
        }
        side.append_option(trade.side.as_deref());
        tx_hash.append_option(trade.transaction_hash.as_deref());
        maker.append_option(trade.maker_address.as_deref());
        taker.append_option(trade.taker_address.as_deref());
        raw_json.append_value(serde_json::to_string(trade).unwrap_or_else(|_| "{}".into()));
        source_col.append_value(source);
        raw_url.append_null();
        raw_sha.append_null();
        ingested_at.append_value(now);
        run_id_col.append_value(run_id);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(trade_id.finish()),
        Arc::new(market_id.finish()),
        Arc::new(token_id.finish()),
        Arc::new(ts.finish()),
        Arc::new(price.finish()),
        Arc::new(size.finish()),
        Arc::new(side.finish()),
        Arc::new(tx_hash.finish()),
        Arc::new(maker.finish()),
        Arc::new(taker.finish()),
        Arc::new(raw_json.finish()),
        Arc::new(source_col.finish()),
        Arc::new(raw_url.finish()),
        Arc::new(raw_sha.finish()),
        Arc::new(ingested_at.finish()),
        Arc::new(run_id_col.finish()),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}
