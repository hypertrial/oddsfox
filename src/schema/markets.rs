use std::sync::Arc;

use arrow::datatypes::Schema;

use super::{ingest_meta_fields, string_field, bool_field, float64_field, timestamp_field};

pub fn schema() -> Arc<Schema> {
    let mut fields = vec![
        string_field("market_id", false),
        string_field("event_id", true),
        string_field("condition_id", true),
        string_field("question_id", true),
        string_field("slug", true),
        string_field("question", true),
        string_field("description", true),
        bool_field("active", true),
        bool_field("closed", true),
        bool_field("resolved", true),
        bool_field("enable_order_book", true),
        bool_field("neg_risk", true),
        float64_field("liquidity", true),
        float64_field("volume", true),
        float64_field("volume_24h", true),
        float64_field("open_interest", true),
        timestamp_field("close_time", true),
        timestamp_field("resolution_time", true),
        string_field("resolution_source", true),
        string_field("raw_json", true),
    ];
    fields.extend(ingest_meta_fields());
    Arc::new(Schema::new(fields))
}
