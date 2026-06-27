use std::sync::Arc;

use arrow::datatypes::Schema;

use super::{ingest_meta_fields, string_field, timestamp_field};

pub fn schema() -> Arc<Schema> {
    let mut fields = vec![
        string_field("market_id", false),
        timestamp_field("resolved_at", true),
        string_field("winning_token_id", true),
        string_field("winning_outcome", true),
        string_field("resolution_source", true),
        string_field("resolution_status", true),
        string_field("raw_json", true),
    ];
    fields.extend(ingest_meta_fields());
    Arc::new(Schema::new(fields))
}
