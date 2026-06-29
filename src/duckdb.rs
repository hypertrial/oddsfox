use crate::config::DuckDbOptions;
use crate::duckdb_engine::{lake_db_path, open_connection, register_layer_views};
use crate::error::{OddsfoxError, Result};
use crate::paths::LakePaths;
use crate::progress_log::log_progress;

pub fn default_db_for_lake(lake: &LakePaths) -> std::path::PathBuf {
    lake_db_path(lake)
}

pub fn run(options: &DuckDbOptions) -> Result<()> {
    let lake = LakePaths::new(&options.out);
    let db_path = options.db.clone();
    log_progress(format!(
        "duckdb: building catalog at `{}` for lake `{}`",
        db_path.display(),
        lake.root.display()
    ));
    let conn = open_connection(Some(&db_path))?;
    let created = register_layer_views(&conn, &lake)?;

    if created == 0 {
        return Err(OddsfoxError::DuckDb(
            "no parquet files found in lake; run sync first".into(),
        ));
    }

    log_progress(format!(
        "duckdb: registered {created} view(s) in `{}`",
        db_path.display()
    ));
    Ok(())
}
