use std::sync::Arc;

use arrow::datatypes::Schema;

use super::{float64_field, ingest_meta_fields, int32_field, string_field};

pub fn schema() -> Arc<Schema> {
    let mut fields = vec![
        string_field("snapshot_id", false),
        string_field("side", false),
        float64_field("price", true),
        float64_field("size", true),
        int32_field("level_index", false),
    ];
    fields.extend(ingest_meta_fields());
    Arc::new(Schema::new(fields))
}
