use std::sync::Arc;

use arrow::datatypes::Schema;

use super::{float64_field, int32_field, string_field, timestamp_field};

pub fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        string_field("metric_name", false),
        string_field("market_id", true),
        string_field("token_id", true),
        timestamp_field("ts", false),
        float64_field("value", true),
        int32_field("window_seconds", true),
        string_field("source_version", true),
    ]))
}
