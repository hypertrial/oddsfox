use std::path::{Path, PathBuf};

use duckdb::Connection;

use crate::config::Table;
use crate::error::{OddsfoxError, Result};
use crate::manifest::completed_run_ids_from_lake;
use crate::paths::LakePaths;

pub const GOLD_TABLES: [&str; 5] = [
    "metric_points",
    "calibration",
    "liquidity_rollup",
    "accuracy",
    "user_pnl",
];

pub fn lake_db_path(lake: &LakePaths) -> PathBuf {
    lake.catalog_db()
}

pub fn map_duckdb<T>(result: std::result::Result<T, duckdb::Error>) -> Result<T> {
    result.map_err(|err| OddsfoxError::DuckDb(err.to_string()))
}

pub fn open_connection(db_path: Option<&Path>) -> Result<Connection> {
    match db_path {
        Some(path) => map_duckdb(Connection::open(path)),
        None => map_duckdb(Connection::open_in_memory()),
    }
}

pub(crate) fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

pub(crate) fn read_parquet_sql(glob: &str) -> String {
    format!("read_parquet('{}')", escape_sql_string(glob))
}

pub fn bronze_source_sql(lake: &LakePaths, table: Table) -> String {
    let source = read_parquet_sql(&lake.duckdb_parquet_glob(table));
    if !table.is_run_partitioned() {
        return source;
    }
    let ids = completed_run_ids_from_lake(lake.root.clone());
    if ids.is_empty() {
        return format!("(SELECT * FROM {source} WHERE false)");
    }
    let ids = ids
        .into_iter()
        .map(|id| format!("'{}'", escape_sql_string(&id)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("(SELECT * FROM {source} WHERE run_id IN ({ids}))")
}

pub fn register_layer_views(conn: &Connection, lake: &LakePaths) -> Result<usize> {
    let mut created = 0;
    for table in crate::config::Table::all() {
        let glob = lake.duckdb_parquet_glob(*table);
        if glob_exists(&glob) {
            let view = format!("bronze_{}", table.as_str());
            let source = bronze_source_sql(lake, *table);
            let sql = format!("CREATE OR REPLACE VIEW {view} AS SELECT * FROM {source}");
            map_duckdb(conn.execute(&sql, []))?;
            created += 1;
        }
    }
    for name in GOLD_TABLES {
        let glob = lake.layer_parquet_glob("gold", name);
        if glob_exists(&glob) {
            let view = format!("gold_{name}");
            let source = read_parquet_sql(&glob);
            let sql = format!("CREATE OR REPLACE VIEW {view} AS SELECT * FROM {source}");
            map_duckdb(conn.execute(&sql, []))?;
            created += 1;
        }
    }
    Ok(created)
}

pub(crate) fn glob_exists(glob_pattern: &str) -> bool {
    let root = glob_pattern
        .trim_end_matches("**/*.parquet")
        .trim_end_matches('/');
    let root = std::path::Path::new(root);
    if !root.is_dir() {
        return false;
    }
    has_parquet_recursive(root)
}

fn has_parquet_recursive(dir: &Path) -> bool {
    let Ok(read_dir) = std::fs::read_dir(dir) else {
        return false;
    };
    for entry in read_dir.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if has_parquet_recursive(&path) {
                return true;
            }
        } else if path.extension().is_some_and(|ext| ext == "parquet") {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Table;
    use crate::paths::LakePaths;

    #[test]
    fn glob_exists_finds_nested_parquet() {
        let dir = tempfile::tempdir().unwrap();
        let paths = LakePaths::new(dir.path());
        paths.scaffold_dirs().unwrap();
        let part = paths.snapshot_partition_file(Table::Events, "run-1");
        paths.ensure_parent(&part).unwrap();
        std::fs::write(part.with_extension("parquet.tmp"), b"not-real").unwrap();
        std::fs::rename(part.with_extension("parquet.tmp"), &part).unwrap();
        let glob = paths.duckdb_parquet_glob(Table::Events);
        assert!(glob_exists(&glob));
    }

    #[test]
    fn read_parquet_sql_escapes_apostrophes() {
        let sql = read_parquet_sql("/tmp/lake's/bronze/events/**/*.parquet");
        assert_eq!(
            sql,
            "read_parquet('/tmp/lake''s/bronze/events/**/*.parquet')"
        );
    }

    #[test]
    fn bronze_source_filters_run_partitioned_tables() {
        let dir = tempfile::tempdir().unwrap();
        let paths = LakePaths::new(dir.path());
        let sql = bronze_source_sql(&paths, Table::Events);
        assert!(sql.contains("WHERE false"));

        let price_sql = bronze_source_sql(&paths, Table::Prices);
        assert!(!price_sql.contains("run_id IN"));
    }

    #[test]
    fn bronze_source_hides_uncommitted_runs() {
        let dir = tempfile::tempdir().unwrap();
        let paths = LakePaths::new(dir.path());
        paths.scaffold_dirs().unwrap();
        let raw = r#"[{"id":"e1","title":"Event","markets":[]}]"#;
        let events: Vec<crate::gamma::GammaEvent> = serde_json::from_str(raw).unwrap();
        let batch =
            crate::normalize::events_batch(&events, "gamma", "url", "sha", "run-1").unwrap();
        crate::parquet::write_snapshot(&paths, Table::Events, "run-1", &[batch]).unwrap();

        let conn = open_connection(None).unwrap();
        let source = bronze_source_sql(&paths, Table::Events);
        let count: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {source}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0);

        crate::manifest::ManifestStore::open(dir.path())
            .unwrap()
            .append_completed_run("test", "run-1", chrono::Utc::now(), 1)
            .unwrap();
        let source = bronze_source_sql(&paths, Table::Events);
        let count: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {source}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 1);
    }
}
