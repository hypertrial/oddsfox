use std::sync::Arc;

use arrow::datatypes::Schema;

use super::{ingest_meta_fields, string_field, bool_field, timestamp_field};

pub fn schema() -> Arc<Schema> {
    let mut fields = vec![
        string_field("event_id", false),
        string_field("slug", true),
        string_field("title", true),
        string_field("description", true),
        string_field("category", true),
        string_field("tags", true),
        bool_field("active", true),
        bool_field("closed", true),
        timestamp_field("start_time", true),
        timestamp_field("end_time", true),
        timestamp_field("created_at", true),
        timestamp_field("updated_at", true),
        string_field("raw_json", true),
    ];
    fields.extend(ingest_meta_fields());
    Arc::new(Schema::new(fields))
}
