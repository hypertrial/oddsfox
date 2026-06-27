use std::sync::Arc;

use arrow::datatypes::Schema;

use super::{ingest_meta_fields, string_field, float64_field, timestamp_field};

pub fn schema() -> Arc<Schema> {
    let mut fields = vec![
        string_field("snapshot_id", false),
        string_field("token_id", false),
        string_field("market_id", true),
        timestamp_field("ts", false),
        string_field("book_hash", true),
        float64_field("best_bid", true),
        float64_field("best_ask", true),
        float64_field("spread", true),
        float64_field("midpoint", true),
        float64_field("bid_depth_1pct", true),
        float64_field("ask_depth_1pct", true),
        float64_field("bid_depth_5pct", true),
        float64_field("ask_depth_5pct", true),
        string_field("raw_json", true),
    ];
    fields.extend(ingest_meta_fields());
    Arc::new(Schema::new(fields))
}
