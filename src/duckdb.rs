use crate::config::DuckDbOptions;
use crate::duckdb_engine::{lake_db_path, open_connection, register_layer_views};
use crate::error::{OddsfoxError, Result};
use crate::paths::LakePaths;

pub fn default_db_for_lake(lake: &LakePaths) -> std::path::PathBuf {
    lake_db_path(lake)
}

pub fn run(options: &DuckDbOptions) -> Result<()> {
    let lake = LakePaths::new(&options.out);
    let db_path = options.db.clone();
    let conn = open_connection(Some(&db_path))?;
    let created = register_layer_views(&conn, &lake)?;

    if created == 0 {
        return Err(OddsfoxError::DuckDb(
            "no parquet files found in lake; run sync first".into(),
        ));
    }

    println!(
        "created {created} DuckDB view(s) in `{}` for lake `{}`",
        db_path.display(),
        lake.root.display()
    );
    Ok(())
}
