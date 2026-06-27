use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringBuilder, TimestampMillisecondBuilder};
use chrono::Utc;

use crate::error::Result;
use crate::gamma::GammaMarket;
use crate::schema::resolutions as resolutions_schema;

pub fn resolutions_batch(
    markets: &[GammaMarket],
    source: &str,
    raw_url: &str,
    raw_sha256: &str,
    run_id: &str,
) -> Result<RecordBatch> {
    let schema = resolutions_schema::schema();
    let mut market_id = StringBuilder::new();
    let mut resolved_at = TimestampMillisecondBuilder::new();
    let mut winning_token_id = StringBuilder::new();
    let mut winning_outcome = StringBuilder::new();
    let mut resolution_source = StringBuilder::new();
    let mut resolution_status = StringBuilder::new();
    let mut raw_json = StringBuilder::new();
    let mut source_col = StringBuilder::new();
    let mut raw_url_col = StringBuilder::new();
    let mut raw_sha_col = StringBuilder::new();
    let mut ingested_at = TimestampMillisecondBuilder::new();
    let mut run_id_col = StringBuilder::new();
    let now = Utc::now().timestamp_millis();

    for market in markets.iter().filter(|m| m.resolved.unwrap_or(false)) {
        market_id.append_value(&market.id);
        append_ts(&mut resolved_at, super::parse_ts(market.resolutionTime.as_deref()));
        let outcomes = market.parsed_outcomes();
        let winner = market
            .winningOutcomeIndex
            .and_then(|idx| outcomes.iter().find(|(i, _, _)| *i == idx))
            .map(|(_, name, token)| (name.clone(), token.clone()));
        winning_outcome.append_option(winner.as_ref().map(|(name, _)| name.as_str()));
        winning_token_id.append_option(winner.as_ref().and_then(|(_, token)| token.as_deref()));
        resolution_source.append_option(market.resolutionSource.as_deref());
        resolution_status.append_value("resolved");
        raw_json.append_value(serde_json::to_string(market).unwrap_or_else(|_| "{}".into()));
        source_col.append_value(source);
        raw_url_col.append_value(raw_url);
        raw_sha_col.append_value(raw_sha256);
        ingested_at.append_value(now);
        run_id_col.append_value(run_id);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(market_id.finish()),
        Arc::new(resolved_at.finish()),
        Arc::new(winning_token_id.finish()),
        Arc::new(winning_outcome.finish()),
        Arc::new(resolution_source.finish()),
        Arc::new(resolution_status.finish()),
        Arc::new(raw_json.finish()),
        Arc::new(source_col.finish()),
        Arc::new(raw_url_col.finish()),
        Arc::new(raw_sha_col.finish()),
        Arc::new(ingested_at.finish()),
        Arc::new(run_id_col.finish()),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn append_ts(builder: &mut TimestampMillisecondBuilder, value: Option<chrono::DateTime<Utc>>) {
    if let Some(ts) = value {
        builder.append_value(ts.timestamp_millis());
    } else {
        builder.append_null();
    }
}
