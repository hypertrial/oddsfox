use std::sync::Arc;

use arrow::datatypes::Schema;

use super::{float64_field, ingest_meta_fields, string_field, timestamp_field};

pub fn schema() -> Arc<Schema> {
    let mut fields = vec![
        string_field("position_id", false),
        string_field("user_id", false),
        string_field("market_id", true),
        string_field("token_id", true),
        timestamp_field("as_of", false),
        float64_field("size", true),
        float64_field("avg_price", true),
        float64_field("mark_price", true),
        float64_field("mark_value", true),
        float64_field("unrealized_pnl", true),
        float64_field("realized_pnl", true),
        float64_field("total_pnl", true),
        string_field("status", true),
        string_field("raw_json", true),
    ];
    fields.extend(ingest_meta_fields());
    Arc::new(Schema::new(fields))
}
