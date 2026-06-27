use std::sync::Arc;

use arrow::datatypes::Schema;

use super::{ingest_meta_fields, string_field, bool_field, int32_field};

pub fn schema() -> Arc<Schema> {
    let mut fields = vec![
        string_field("market_id", false),
        int32_field("outcome_index", false),
        string_field("outcome_name", true),
        string_field("token_id", true),
        bool_field("is_winner", true),
    ];
    fields.extend(ingest_meta_fields());
    Arc::new(Schema::new(fields))
}
