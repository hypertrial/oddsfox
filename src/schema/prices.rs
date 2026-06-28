use std::sync::Arc;

use arrow::datatypes::Schema;

use super::{float64_field, ingest_meta_fields, int32_field, string_field, timestamp_field};

pub fn schema() -> Arc<Schema> {
    let mut fields = vec![
        string_field("token_id", false),
        string_field("market_id", true),
        timestamp_field("ts", false),
        float64_field("price", true),
        int32_field("fidelity_minutes", true),
    ];
    fields.extend(ingest_meta_fields());
    Arc::new(Schema::new(fields))
}
