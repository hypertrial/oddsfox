use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use crate::config::Table;

pub mod book_levels;
pub mod events;
pub mod markets;
pub mod metrics;
pub mod orderbooks;
pub mod outcomes;
pub mod prices;
pub mod resolutions;
pub mod trades;

pub fn arrow_schema(table: Table) -> Arc<Schema> {
    match table {
        Table::Events => events::schema(),
        Table::Markets => markets::schema(),
        Table::Outcomes => outcomes::schema(),
        Table::Prices => prices::schema(),
        Table::Orderbooks => orderbooks::schema(),
        Table::BookLevels => book_levels::schema(),
        Table::Trades => trades::schema(),
        Table::Resolutions => resolutions::schema(),
    }
}

pub fn schema_version() -> &'static str {
    "prediction-market-v2"
}

pub fn lake_layout_version() -> &'static str {
    crate::paths::LAKE_LAYOUT_VERSION
}

pub fn print_schema(table: Table) {
    let schema = arrow_schema(table);
    println!(
        "{} schema ({} columns):",
        table.as_str(),
        schema.fields().len()
    );
    for field in schema.fields() {
        println!("  {}: {:?}", field.name(), field.data_type());
    }
}

pub(crate) fn string_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Utf8, nullable)
}

pub(crate) fn bool_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Boolean, nullable)
}

pub(crate) fn int32_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Int32, nullable)
}

#[allow(dead_code)]
pub fn int64_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Int64, nullable)
}

pub(crate) fn float64_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Float64, nullable)
}

pub(crate) fn timestamp_field(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Millisecond, None),
        nullable,
    )
}

pub(crate) fn ingest_meta_fields() -> Vec<Field> {
    vec![
        string_field("source", false),
        string_field("raw_url", true),
        string_field("raw_sha256", true),
        timestamp_field("ingested_at", false),
        string_field("run_id", false),
    ]
}
