use std::path::Path;

use crate::error::Result;

pub fn run_adhoc(out: &Path, db: &Path, query: &str) -> Result<()> {
    let conn = if db.exists() {
        crate::duckdb_engine::open_connection(Some(db))?
    } else {
        let options = crate::config::DuckDbOptions {
            out: out.to_path_buf(),
            db: db.to_path_buf(),
        };
        crate::duckdb::run(&options)?;
        crate::duckdb_engine::open_connection(Some(db))?
    };
    let mut stmt = conn.prepare(query)?;
    let mut rows = stmt.query([])?;
    let mut count = 0;
    while let Some(row) = rows.next()? {
        let value = row.get::<_, String>(0).unwrap_or_else(|_| "?".into());
        println!("{value}");
        count += 1;
        if count >= 100 {
            break;
        }
    }
    Ok(())
}
