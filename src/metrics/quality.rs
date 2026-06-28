use chrono::Utc;

use crate::config::Table;
use crate::duckdb_engine::{open_connection, read_parquet_sql};
use crate::error::Result;
use crate::manifest::DataQualityRecord;
use crate::paths::LakePaths;

pub fn run_quality_checks(out: &std::path::Path) -> Result<Vec<DataQualityRecord>> {
    let paths = LakePaths::new(out);
    let markets_glob = paths.duckdb_parquet_glob(Table::Markets);
    let outcomes_glob = paths.duckdb_parquet_glob(Table::Outcomes);
    let markets_source = read_parquet_sql(&markets_glob);
    let outcomes_source = read_parquet_sql(&outcomes_glob);
    let conn = open_connection(None)?;
    let mut records = Vec::new();
    let now = Utc::now();

    let missing_tokens_sql =
        format!("SELECT market_id FROM {outcomes_source} WHERE token_id IS NULL");
    if let Ok(mut stmt) = conn.prepare(&missing_tokens_sql) {
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        for market_id in rows {
            let market_id = market_id?;
            records.push(DataQualityRecord {
                check_name: "missing_token_ids".into(),
                entity_type: "market".into(),
                entity_id: market_id,
                severity: "warning".into(),
                message: "market outcome missing token_id".into(),
                checked_at: now,
            });
        }
    }

    let unresolved_sql =
        format!("SELECT market_id FROM {markets_source} WHERE closed = true AND resolved = false");
    if let Ok(mut stmt) = conn.prepare(&unresolved_sql) {
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        for market_id in rows {
            let market_id = market_id?;
            records.push(DataQualityRecord {
                check_name: "missing_resolution".into(),
                entity_type: "market".into(),
                entity_id: market_id,
                severity: "info".into(),
                message: "closed market without resolution".into(),
                checked_at: now,
            });
        }
    }

    records.push(DataQualityRecord {
        check_name: "off_chain_on_chain_caveat".into(),
        entity_type: "lake".into(),
        entity_id: out.display().to_string(),
        severity: "info".into(),
        message: "public order-book direction may disagree with on-chain ground truth".into(),
        checked_at: now,
    });

    if !records.is_empty() {
        let path = paths.data_quality_manifest();
        std::fs::write(&path, serde_json::to_string_pretty(&records)?)?;
    }
    Ok(records)
}
