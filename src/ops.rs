use std::path::Path;

use crate::config::Table;
use crate::duckdb_engine::{bronze_source_sql, open_connection};
use crate::error::Result;
use crate::paths::LakePaths;

pub fn stats(out: &Path) -> Result<()> {
    let paths = LakePaths::new(out);
    let conn = open_connection(None)?;
    for table in Table::all() {
        let source = bronze_source_sql(&paths, *table);
        let sql = format!("SELECT COUNT(*) FROM {source}");
        if let Ok(mut stmt) = conn.prepare(&sql) {
            if let Ok(count) = stmt.query_row([], |row| row.get::<_, i64>(0)) {
                println!("{}: {count} rows", table.as_str());
            }
        }
    }
    Ok(())
}
