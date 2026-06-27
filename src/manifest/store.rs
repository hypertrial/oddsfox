use std::fs::{File, OpenOptions};
use std::path::PathBuf;

use chrono::Utc;
use fs2::FileExt;
use uuid::Uuid;

use crate::config::Table;
use crate::error::{OddsfoxError, Result};
use crate::paths::LakePaths;
use crate::schema;

use super::records::{RunRecord, SyncStateRecord, VersionRecord};

pub fn new_run_id() -> String {
    Uuid::new_v4().to_string()
}

pub struct ManifestStore {
    paths: LakePaths,
    _lake_lock: File,
}

fn acquire_lake_lock_exclusive(paths: &LakePaths) -> Result<File> {
    paths.ensure_parent(&paths.lake_lock_file())?;
    let lock_file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(paths.lake_lock_file())
        .map_err(OddsfoxError::Io)?;
    FileExt::try_lock_exclusive(&lock_file).map_err(|err| {
        if err.kind() == std::io::ErrorKind::WouldBlock {
            OddsfoxError::LakeLocked(paths.lake_lock_file())
        } else {
            OddsfoxError::Io(err)
        }
    })?;
    Ok(lock_file)
}

fn acquire_lake_lock_shared(paths: &LakePaths) -> Result<File> {
    paths.ensure_parent(&paths.lake_lock_file())?;
    let lock_file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(paths.lake_lock_file())
        .map_err(OddsfoxError::Io)?;
    FileExt::try_lock_shared(&lock_file).map_err(|err| {
        if err.kind() == std::io::ErrorKind::WouldBlock {
            OddsfoxError::LakeLocked(paths.lake_lock_file())
        } else {
            OddsfoxError::Io(err)
        }
    })?;
    Ok(lock_file)
}

impl ManifestStore {
    pub fn open(lake_root: impl Into<PathBuf>) -> Result<Self> {
        let paths = LakePaths::new(lake_root);
        let lock = acquire_lake_lock_exclusive(&paths)?;
        Ok(Self {
            paths,
            _lake_lock: lock,
        })
    }

    pub fn open_read_only(lake_root: impl Into<PathBuf>) -> Result<Self> {
        let paths = LakePaths::new(lake_root);
        let lock = acquire_lake_lock_shared(&paths)?;
        Ok(Self {
            paths,
            _lake_lock: lock,
        })
    }

    pub fn paths(&self) -> &LakePaths {
        &self.paths
    }

    pub fn write_version(&self) -> Result<()> {
        let now = Utc::now();
        let record = VersionRecord {
            oddsfox_version: env!("CARGO_PKG_VERSION").to_string(),
            schema_version: schema::schema_version().to_string(),
            lake_layout_version: schema::lake_layout_version().to_string(),
            created_at: now,
            updated_at: now,
        };
        let path = self.paths.version_manifest();
        write_json(&path, &record)
    }

    pub fn append_run(&self, run: RunRecord) -> Result<()> {
        let path = self.paths.runs_manifest();
        append_json_line(&path, &run)
    }

    pub fn upsert_sync_state(&self, record: SyncStateRecord) -> Result<()> {
        let path = self.paths.sync_state_manifest();
        let mut records: Vec<SyncStateRecord> = read_json_lines(&path);
        if let Some(existing) = records
            .iter_mut()
            .find(|r| r.source == record.source && r.cursor_key == record.cursor_key)
        {
            *existing = record;
        } else {
            records.push(record);
        }
        write_json(&path, &records)
    }

    pub fn sync_state(&self, source: &str, cursor_key: &str) -> Option<SyncStateRecord> {
        let path = self.paths.sync_state_manifest();
        read_json_lines::<SyncStateRecord>(&path)
            .into_iter()
            .find(|r| r.source == source && r.cursor_key == cursor_key)
    }

    pub fn write_schema_records(&self) -> Result<()> {
        let records: Vec<crate::manifest::records::SchemaRecord> = Table::all()
            .iter()
            .map(|table| {
                let schema = schema::arrow_schema(*table);
                let columns: Vec<(&str, String)> = schema
                    .fields()
                    .iter()
                    .map(|f| (f.name().as_str(), format!("{:?}", f.data_type())))
                    .collect();
                crate::manifest::records::SchemaRecord {
                    table: table.as_str().to_string(),
                    schema_version: schema::schema_version().to_string(),
                    column_count: schema.fields().len() as i32,
                    columns_json: serde_json::to_string(&columns).unwrap(),
                    updated_at: Utc::now(),
                }
            })
            .collect();
        write_json(&self.paths.schemas_manifest(), &records)
    }
}

fn write_json<T: serde::Serialize>(path: &std::path::Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temp = path.with_extension("json.tmp");
    let contents = serde_json::to_string_pretty(value)?;
    std::fs::write(&temp, contents)?;
    std::fs::rename(&temp, path)?;
    Ok(())
}

fn append_json_line<T: serde::Serialize>(path: &std::path::Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    use std::io::Write;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    let line = serde_json::to_string(value)?;
    writeln!(file, "{line}")?;
    Ok(())
}

fn read_json_lines<T: for<'de> serde::Deserialize<'de>>(path: &std::path::Path) -> Vec<T> {
    if !path.exists() {
        return Vec::new();
    }
    let contents = std::fs::read_to_string(path).unwrap_or_default();
    if contents.trim().starts_with('[') {
        return serde_json::from_str(&contents).unwrap_or_default();
    }
    contents
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}
