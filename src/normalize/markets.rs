use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, RecordBatch, StringBuilder,
    TimestampMillisecondBuilder,
};
use chrono::Utc;

use crate::error::Result;
use crate::gamma::GammaMarket;
use crate::schema::markets as markets_schema;

pub fn markets_batch(
    markets: &[GammaMarket],
    source: &str,
    raw_url: &str,
    raw_sha256: &str,
    run_id: &str,
) -> Result<RecordBatch> {
    let schema = markets_schema::schema();
    let mut market_id = StringBuilder::new();
    let mut event_id = StringBuilder::new();
    let mut condition_id = StringBuilder::new();
    let mut question_id = StringBuilder::new();
    let mut slug = StringBuilder::new();
    let mut question = StringBuilder::new();
    let mut description = StringBuilder::new();
    let mut active = BooleanBuilder::new();
    let mut closed = BooleanBuilder::new();
    let mut resolved = BooleanBuilder::new();
    let mut enable_order_book = BooleanBuilder::new();
    let mut neg_risk = BooleanBuilder::new();
    let mut liquidity = Float64Builder::new();
    let mut volume = Float64Builder::new();
    let mut volume_24h = Float64Builder::new();
    let mut open_interest = Float64Builder::new();
    let mut close_time = TimestampMillisecondBuilder::new();
    let mut resolution_time = TimestampMillisecondBuilder::new();
    let mut resolution_source = StringBuilder::new();
    let mut raw_json = StringBuilder::new();
    let mut source_col = StringBuilder::new();
    let mut raw_url_col = StringBuilder::new();
    let mut raw_sha_col = StringBuilder::new();
    let mut ingested_at = TimestampMillisecondBuilder::new();
    let mut run_id_col = StringBuilder::new();
    let now = Utc::now().timestamp_millis();

    for market in markets {
        market_id.append_value(&market.id);
        event_id.append_option(market.event_id.as_deref());
        condition_id.append_option(market.conditionId.as_deref());
        question_id.append_option(market.questionID.as_deref());
        slug.append_option(market.slug.as_deref());
        question.append_option(market.question.as_deref());
        description.append_option(market.description.as_deref());
        active.append_option(market.active);
        closed.append_option(market.closed);
        resolved.append_option(market.resolved);
        enable_order_book.append_option(market.enableOrderBook);
        neg_risk.append_option(market.negRisk);
        append_f64(&mut liquidity, super::parse_f64(market.liquidity.as_deref()));
        append_f64(&mut volume, super::parse_f64(market.volume.as_deref()));
        append_f64(&mut volume_24h, super::parse_f64(market.volume24hr.as_deref()));
        append_f64(&mut open_interest, super::parse_f64(market.openInterest.as_deref()));
        append_ts(&mut close_time, super::parse_ts(market.endDate.as_deref()));
        append_ts(&mut resolution_time, super::parse_ts(market.resolutionTime.as_deref()));
        resolution_source.append_option(market.resolutionSource.as_deref());
        raw_json.append_value(serde_json::to_string(market).unwrap_or_else(|_| "{}".into()));
        source_col.append_value(source);
        raw_url_col.append_value(raw_url);
        raw_sha_col.append_value(raw_sha256);
        ingested_at.append_value(now);
        run_id_col.append_value(run_id);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(market_id.finish()),
        Arc::new(event_id.finish()),
        Arc::new(condition_id.finish()),
        Arc::new(question_id.finish()),
        Arc::new(slug.finish()),
        Arc::new(question.finish()),
        Arc::new(description.finish()),
        Arc::new(active.finish()),
        Arc::new(closed.finish()),
        Arc::new(resolved.finish()),
        Arc::new(enable_order_book.finish()),
        Arc::new(neg_risk.finish()),
        Arc::new(liquidity.finish()),
        Arc::new(volume.finish()),
        Arc::new(volume_24h.finish()),
        Arc::new(open_interest.finish()),
        Arc::new(close_time.finish()),
        Arc::new(resolution_time.finish()),
        Arc::new(resolution_source.finish()),
        Arc::new(raw_json.finish()),
        Arc::new(source_col.finish()),
        Arc::new(raw_url_col.finish()),
        Arc::new(raw_sha_col.finish()),
        Arc::new(ingested_at.finish()),
        Arc::new(run_id_col.finish()),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn append_f64(builder: &mut Float64Builder, value: Option<f64>) {
    if let Some(v) = value {
        builder.append_value(v);
    } else {
        builder.append_null();
    }
}

fn append_ts(builder: &mut TimestampMillisecondBuilder, value: Option<chrono::DateTime<Utc>>) {
    if let Some(ts) = value {
        builder.append_value(ts.timestamp_millis());
    } else {
        builder.append_null();
    }
}
