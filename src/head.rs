use std::path::{Path, PathBuf};

use duckdb::types::ValueRef;
use duckdb::Connection;

use crate::config::Table;
use crate::duckdb_engine::{
    bronze_source_sql, escape_sql_string, glob_exists, map_duckdb, open_connection,
    read_parquet_sql, GOLD_TABLES,
};
use crate::error::Result;
use crate::paths::LakePaths;
use crate::schema;

#[derive(Debug, Clone)]
pub struct HeadOptions {
    pub out: PathBuf,
    pub export_dir: PathBuf,
    pub limit: usize,
}

pub fn default_export_dir(lake_root: &Path) -> PathBuf {
    lake_root.join("_exports").join("heads")
}

pub fn run(options: &HeadOptions) -> Result<()> {
    let paths = LakePaths::new(&options.out);
    std::fs::create_dir_all(&options.export_dir)?;
    let conn = open_connection(None)?;

    for table in Table::all() {
        let label = format!("bronze_{}", table.as_str());
        let glob = paths.duckdb_parquet_glob(*table);
        let source = bronze_source_sql(&paths, *table);
        let csv_path = csv_file_path(&options.export_dir, &label);
        process_table(
            &conn,
            &label,
            &glob,
            &source,
            &csv_path,
            options.limit,
            Some(*table),
        )?;
    }

    for name in GOLD_TABLES {
        let label = format!("gold_{name}");
        let glob = paths.layer_parquet_glob("gold", name);
        let source = read_parquet_sql(&glob);
        let csv_path = csv_file_path(&options.export_dir, &label);
        process_table(
            &conn,
            &label,
            &glob,
            &source,
            &csv_path,
            options.limit,
            None,
        )?;
    }

    Ok(())
}

fn csv_file_path(export_dir: &Path, label: &str) -> PathBuf {
    export_dir.join(format!("{label}_head.csv"))
}

fn process_table(
    conn: &Connection,
    label: &str,
    glob: &str,
    source: &str,
    csv_path: &Path,
    limit: usize,
    bronze_table: Option<Table>,
) -> Result<()> {
    println!("=== {label} (limit {limit}) ===");
    if glob_exists(glob) {
        let row_count = print_preview(conn, source, limit)?;
        export_populated_table(conn, source, csv_path, limit)?;
        println!("wrote {} ({row_count} rows)", csv_path.display());
    } else {
        println!("(empty — no parquet files)");
        write_header_only_csv(csv_path, bronze_table)?;
        println!("wrote {} (0 rows, header only)", csv_path.display());
    }
    println!();
    Ok(())
}

fn print_preview(conn: &Connection, source: &str, limit: usize) -> Result<usize> {
    let sql = format!("SELECT * FROM {source} LIMIT {limit}");
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    let column_names = rows
        .as_ref()
        .expect("query should expose statement metadata")
        .column_names();
    println!("{}", column_names.join("\t"));

    let mut row_count = 0;
    while let Some(row) = rows.next()? {
        let mut values = Vec::with_capacity(column_names.len());
        for idx in 0..column_names.len() {
            values.push(format_value(row.get_ref(idx)?));
        }
        println!("{}", values.join("\t"));
        row_count += 1;
    }
    Ok(row_count)
}

fn export_populated_table(
    conn: &Connection,
    source: &str,
    csv_path: &Path,
    limit: usize,
) -> Result<()> {
    let csv_path = escape_sql_string(&csv_path.to_string_lossy());
    let sql = format!(
        "COPY (SELECT * FROM {source} LIMIT {limit}) \
         TO '{csv_path}' (HEADER, DELIMITER ',')"
    );
    map_duckdb(conn.execute(&sql, []))?;
    Ok(())
}

fn write_header_only_csv(csv_path: &Path, bronze_table: Option<Table>) -> Result<()> {
    let schema = match bronze_table {
        Some(table) => schema::arrow_schema(table),
        None => schema::metrics::schema(),
    };
    let header = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>()
        .join(",");
    std::fs::write(csv_path, format!("{header}\n"))?;
    Ok(())
}

fn format_value(value: ValueRef<'_>) -> String {
    match value {
        ValueRef::Null => String::new(),
        ValueRef::Boolean(v) => v.to_string(),
        ValueRef::TinyInt(v) => v.to_string(),
        ValueRef::SmallInt(v) => v.to_string(),
        ValueRef::Int(v) => v.to_string(),
        ValueRef::BigInt(v) => v.to_string(),
        ValueRef::HugeInt(v) => v.to_string(),
        ValueRef::UTinyInt(v) => v.to_string(),
        ValueRef::USmallInt(v) => v.to_string(),
        ValueRef::UInt(v) => v.to_string(),
        ValueRef::UBigInt(v) => v.to_string(),
        ValueRef::Float(v) => v.to_string(),
        ValueRef::Double(v) => v.to_string(),
        ValueRef::Text(v) => String::from_utf8_lossy(v).into_owned(),
        ValueRef::Blob(v) => format!("<blob {} bytes>", v.len()),
        ValueRef::Timestamp(_, v) => v.to_string(),
        ValueRef::Date32(v) => v.to_string(),
        ValueRef::Time64(_, v) => v.to_string(),
        ValueRef::Interval { .. } => "?".into(),
        ValueRef::Decimal(v) => v.to_string(),
        _ => "?".into(),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::gamma::GammaEvent;
    use crate::normalize::events_batch;
    use crate::parquet::write_snapshot;

    use super::*;

    #[test]
    fn head_exports_populated_and_empty_tables() {
        let dir = tempfile::tempdir().unwrap();
        let paths = LakePaths::new(dir.path());
        paths.scaffold_dirs().unwrap();

        let raw = fs::read_to_string("tests/fixtures/gamma_event_response.json").unwrap();
        let events: Vec<GammaEvent> = serde_json::from_str(&raw).unwrap();
        let events_data =
            events_batch(&events, "gamma", "http://test/events", "sha", "run-1").unwrap();
        write_snapshot(&paths, Table::Events, "run-1", &[events_data]).unwrap();
        crate::manifest::ManifestStore::open(dir.path())
            .unwrap()
            .append_completed_run("test", "run-1", chrono::Utc::now(), 1)
            .unwrap();

        let export_dir = dir.path().join("heads");
        run(&HeadOptions {
            out: dir.path().to_path_buf(),
            export_dir: export_dir.clone(),
            limit: 5,
        })
        .unwrap();

        let events_csv = export_dir.join("bronze_events_head.csv");
        assert!(events_csv.exists());
        let events_content = fs::read_to_string(&events_csv).unwrap();
        assert!(events_content.contains("event_id"));
        assert!(events_content.lines().count() >= 2);

        let trades_csv = export_dir.join("bronze_trades_head.csv");
        assert!(trades_csv.exists());
        let trades_content = fs::read_to_string(&trades_csv).unwrap();
        assert!(trades_content.contains("trade_id"));
        assert_eq!(trades_content.lines().count(), 1);
    }
}
