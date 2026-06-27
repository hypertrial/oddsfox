use std::sync::Arc;

use arrow::datatypes::Schema;

use super::{ingest_meta_fields, string_field, float64_field, int32_field, timestamp_field};

pub fn schema() -> Arc<Schema> {
    let mut fields = vec![
        string_field("token_id", false),
        string_field("market_id", true),
        timestamp_field("ts", false),
        float64_field("price", true),
        string_field("source", false),
        int32_field("fidelity_seconds", true),
        timestamp_field("ingested_at", false),
    ];
    fields.extend(ingest_meta_fields());
    Arc::new(Schema::new(fields))
}
