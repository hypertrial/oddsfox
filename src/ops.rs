use std::path::Path;

use crate::config::Table;
use crate::error::Result;
use crate::paths::LakePaths;

pub fn stats(out: &Path) -> Result<()> {
    let paths = LakePaths::new(out);
    let conn = crate::duckdb_engine::open_connection(None)?;
    for table in Table::all() {
        let glob = paths.duckdb_parquet_glob(*table);
        let sql = format!("SELECT COUNT(*) FROM read_parquet('{glob}')");
        if let Ok(mut stmt) = conn.prepare(&sql) {
            if let Ok(count) = stmt.query_row([], |row| row.get::<_, i64>(0)) {
                println!("{}: {count} rows", table.as_str());
            }
        }
    }
    Ok(())
}
