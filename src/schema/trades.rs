use std::sync::Arc;

use arrow::datatypes::Schema;

use super::{ingest_meta_fields, string_field, float64_field, timestamp_field};

pub fn schema() -> Arc<Schema> {
    let mut fields = vec![
        string_field("trade_id", false),
        string_field("market_id", true),
        string_field("token_id", true),
        timestamp_field("ts", false),
        float64_field("price", true),
        float64_field("size", true),
        string_field("side", true),
        string_field("tx_hash", true),
        string_field("maker", true),
        string_field("taker", true),
        string_field("raw_json", true),
    ];
    fields.extend(ingest_meta_fields());
    Arc::new(Schema::new(fields))
}
