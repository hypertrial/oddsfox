use std::path::{Path, PathBuf};

use duckdb::Connection;

use crate::error::{OddsfoxError, Result};
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

pub fn register_layer_views(conn: &Connection, lake: &LakePaths) -> Result<usize> {
    let mut created = 0;
    for table in crate::config::Table::all() {
        let glob = lake.duckdb_parquet_glob(*table);
        if glob_exists(&glob) {
            let view = format!("bronze_{}", table.as_str());
            let source = read_parquet_sql(&glob);
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
}
