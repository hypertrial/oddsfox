use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Int32Builder, RecordBatch, StringBuilder,
    TimestampMillisecondBuilder,
};
use chrono::Utc;

use crate::error::Result;
use crate::gamma::GammaMarket;
use crate::schema::outcomes as outcomes_schema;

pub fn outcomes_batch(
    markets: &[GammaMarket],
    source: &str,
    raw_url: &str,
    raw_sha256: &str,
    run_id: &str,
) -> Result<RecordBatch> {
    let schema = outcomes_schema::schema();
    let mut market_id = StringBuilder::new();
    let mut outcome_index = Int32Builder::new();
    let mut outcome_name = StringBuilder::new();
    let mut token_id = StringBuilder::new();
    let mut is_winner = BooleanBuilder::new();
    let mut source_col = StringBuilder::new();
    let mut raw_url_col = StringBuilder::new();
    let mut raw_sha_col = StringBuilder::new();
    let mut ingested_at = TimestampMillisecondBuilder::new();
    let mut run_id_col = StringBuilder::new();
    let now = Utc::now().timestamp_millis();

    for market in markets {
        for (idx, name, token) in market.parsed_outcomes() {
            market_id.append_value(&market.id);
            outcome_index.append_value(idx);
            outcome_name.append_value(name);
            token_id.append_option(token.as_deref());
            let winner = market
                .winningOutcomeIndex
                .map(|win| win == idx)
                .unwrap_or(false);
            is_winner.append_value(winner);
            source_col.append_value(source);
            raw_url_col.append_value(raw_url);
            raw_sha_col.append_value(raw_sha256);
            ingested_at.append_value(now);
            run_id_col.append_value(run_id);
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(market_id.finish()),
        Arc::new(outcome_index.finish()),
        Arc::new(outcome_name.finish()),
        Arc::new(token_id.finish()),
        Arc::new(is_winner.finish()),
        Arc::new(source_col.finish()),
        Arc::new(raw_url_col.finish()),
        Arc::new(raw_sha_col.finish()),
        Arc::new(ingested_at.finish()),
        Arc::new(run_id_col.finish()),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}
