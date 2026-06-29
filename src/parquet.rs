use std::fs::File;
use std::io::ErrorKind;
use std::path::Path;

use arrow::array::RecordBatch;
use parquet::arrow::ArrowWriter;

use crate::config::Table;
use crate::error::{OddsfoxError, Result};
use crate::parquet_props::data_writer_properties;
use crate::paths::LakePaths;

pub fn write_snapshot(
    lake: &LakePaths,
    table: Table,
    run_id: &str,
    batches: &[RecordBatch],
) -> Result<std::path::PathBuf> {
    let path = lake.snapshot_partition_file(table, run_id);
    write_batches(&path, batches)
}

pub fn write_time_series(
    lake: &LakePaths,
    table: Table,
    date: chrono::NaiveDate,
    part: &str,
    batches: &[RecordBatch],
) -> Result<std::path::PathBuf> {
    let path = lake.time_series_file(table, date, part);
    write_batches(&path, batches)
}

pub fn write_token_series(
    lake: &LakePaths,
    table: Table,
    token_id: &str,
    batches: &[RecordBatch],
) -> Result<std::path::PathBuf> {
    let path = lake.token_partition_file(table, token_id);
    write_batches(&path, batches)
}

pub fn write_hourly_price_window(
    lake: &LakePaths,
    source: &str,
    token_id: &str,
    start_ts: i64,
    batches: &[RecordBatch],
) -> Result<std::path::PathBuf> {
    let path = lake.hourly_price_window_file(source, token_id, start_ts);
    write_batches(&path, batches)
}

pub fn write_batches(path: &Path, batches: &[RecordBatch]) -> Result<std::path::PathBuf> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temp = path.with_extension("parquet.tmp");
    {
        let file = File::create(&temp)?;
        let schema = batches
            .first()
            .ok_or_else(|| OddsfoxError::ParquetWrite("no batches to write".into()))?
            .schema();
        let props = data_writer_properties(Table::Events);
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.close()?;
    }
    replace_file(&temp, path)?;
    Ok(path.to_path_buf())
}

fn replace_file(temp: &Path, dest: &Path) -> Result<()> {
    match std::fs::rename(temp, dest) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::AlreadyExists => {
            std::fs::remove_file(dest)?;
            std::fs::rename(temp, dest)?;
            Ok(())
        }
        Err(err) => Err(err.into()),
    }
}

pub fn parquet_row_count(path: &Path) -> Result<i64> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    if !path.exists() {
        return Ok(0);
    }
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    Ok(builder.metadata().file_metadata().num_rows())
}

pub fn read_all_batches(path: &Path) -> Result<Vec<RecordBatch>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

pub fn write_gold(
    lake: &LakePaths,
    name: &str,
    run_id: &str,
    batches: &[RecordBatch],
) -> Result<std::path::PathBuf> {
    let path = lake
        .gold_dir(name)
        .join(format!("run={run_id}/part.parquet"));
    write_batches(&path, batches)
}
