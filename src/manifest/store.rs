use std::collections::{BTreeMap, BTreeSet};
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

pub const RUN_STARTED: &str = "started";
pub const RUN_COMPLETE: &str = "complete";
pub const RUN_FAILED: &str = "failed";

pub fn new_run_id() -> String {
    Uuid::new_v4().to_string()
}

pub struct ManifestStore {
    paths: LakePaths,
    _lake_lock: File,
}

pub struct RunGuard<'a> {
    store: &'a ManifestStore,
    command: String,
    run_id: String,
    started_at: chrono::DateTime<Utc>,
    completed: bool,
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

    pub fn append_started_run(
        &self,
        command: impl Into<String>,
        run_id: &str,
        started_at: chrono::DateTime<Utc>,
    ) -> Result<()> {
        self.append_run(RunRecord {
            run_id: run_id.to_string(),
            command: command.into(),
            started_at,
            finished_at: None,
            status: RUN_STARTED.into(),
            rows_written: 0,
            oddsfox_version: env!("CARGO_PKG_VERSION").into(),
        })
    }

    pub fn start_run(
        &self,
        command: impl Into<String>,
        run_id: &str,
        started_at: chrono::DateTime<Utc>,
    ) -> Result<RunGuard<'_>> {
        let command = command.into();
        self.append_started_run(command.clone(), run_id, started_at)?;
        Ok(RunGuard {
            store: self,
            command,
            run_id: run_id.to_string(),
            started_at,
            completed: false,
        })
    }

    pub fn append_completed_run(
        &self,
        command: impl Into<String>,
        run_id: &str,
        started_at: chrono::DateTime<Utc>,
        rows_written: i64,
    ) -> Result<()> {
        self.append_run(RunRecord {
            run_id: run_id.to_string(),
            command: command.into(),
            started_at,
            finished_at: Some(Utc::now()),
            status: RUN_COMPLETE.into(),
            rows_written,
            oddsfox_version: env!("CARGO_PKG_VERSION").into(),
        })
    }

    pub fn append_failed_run(
        &self,
        command: impl Into<String>,
        run_id: &str,
        started_at: chrono::DateTime<Utc>,
        _error: impl std::fmt::Display,
    ) -> Result<()> {
        self.append_run(RunRecord {
            run_id: run_id.to_string(),
            command: command.into(),
            started_at,
            finished_at: Some(Utc::now()),
            status: RUN_FAILED.into(),
            rows_written: 0,
            oddsfox_version: env!("CARGO_PKG_VERSION").into(),
        })
    }

    pub fn run_records(&self) -> Vec<RunRecord> {
        read_json_lines(&self.paths.runs_manifest())
    }

    pub fn completed_run_ids(&self) -> BTreeSet<String> {
        completed_run_ids_from_records(self.run_records())
    }

    pub fn incomplete_run_ids(&self) -> BTreeSet<String> {
        latest_run_statuses(self.run_records())
            .into_iter()
            .filter_map(|(run_id, status)| (status != RUN_COMPLETE).then_some(run_id))
            .collect()
    }

    pub fn sync_state_records(&self) -> Vec<SyncStateRecord> {
        read_json_lines(&self.paths.sync_state_manifest())
    }

    pub fn upsert_sync_state(&self, record: SyncStateRecord) -> Result<()> {
        self.upsert_sync_states(std::slice::from_ref(&record))
    }

    pub fn upsert_sync_states(&self, updates: &[SyncStateRecord]) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }
        let path = self.paths.sync_state_manifest();
        let mut records: Vec<SyncStateRecord> = read_json_lines(&path);
        for record in updates {
            if let Some(existing) = records
                .iter_mut()
                .find(|r| r.source == record.source && r.cursor_key == record.cursor_key)
            {
                *existing = record.clone();
            } else {
                records.push(record.clone());
            }
        }
        write_json(&path, &records)
    }

    pub fn sync_state(&self, source: &str, cursor_key: &str) -> Option<SyncStateRecord> {
        self.sync_state_records()
            .into_iter()
            .find(|r| r.source == source && r.cursor_key == cursor_key)
    }

    pub fn remove_sync_states_where(
        &self,
        pred: impl Fn(&SyncStateRecord) -> bool,
    ) -> Result<usize> {
        let path = self.paths.sync_state_manifest();
        let records: Vec<SyncStateRecord> = read_json_lines(&path);
        let (removed, kept): (Vec<_>, Vec<_>) = records.into_iter().partition(&pred);
        write_json(&path, &kept)?;
        Ok(removed.len())
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

impl RunGuard<'_> {
    pub fn complete(mut self, rows_written: i64) -> Result<()> {
        let result = self.store.append_completed_run(
            self.command.clone(),
            &self.run_id,
            self.started_at,
            rows_written,
        );
        if result.is_ok() {
            self.completed = true;
        }
        result
    }
}

impl Drop for RunGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self.store.append_failed_run(
                self.command.clone(),
                &self.run_id,
                self.started_at,
                "run exited before completion",
            );
        }
    }
}

pub fn completed_run_ids_from_lake(lake_root: impl Into<PathBuf>) -> BTreeSet<String> {
    let paths = LakePaths::new(lake_root);
    completed_run_ids_from_records(read_json_lines(&paths.runs_manifest()))
}

fn completed_run_ids_from_records(records: Vec<RunRecord>) -> BTreeSet<String> {
    latest_run_statuses(records)
        .into_iter()
        .filter_map(|(run_id, status)| (status == RUN_COMPLETE).then_some(run_id))
        .collect()
}

fn latest_run_statuses(records: Vec<RunRecord>) -> BTreeMap<String, String> {
    let mut latest = BTreeMap::new();
    for record in records {
        latest.insert(record.run_id, record.status);
    }
    latest
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
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_completed_run_writes_complete_record() {
        let dir = tempfile::tempdir().unwrap();
        let store = ManifestStore::open(dir.path()).unwrap();
        let started = Utc::now();
        store
            .append_completed_run("sync test", "run-1", started, 7)
            .unwrap();

        let raw = std::fs::read_to_string(store.paths().runs_manifest()).unwrap();
        let run: RunRecord = serde_json::from_str(raw.trim()).unwrap();
        assert_eq!(run.run_id, "run-1");
        assert_eq!(run.command, "sync test");
        assert_eq!(run.status, RUN_COMPLETE);
        assert_eq!(run.rows_written, 7);
        assert!(run.finished_at.is_some());
    }

    #[test]
    fn completed_run_ids_use_latest_status() {
        let dir = tempfile::tempdir().unwrap();
        let store = ManifestStore::open(dir.path()).unwrap();
        let started = Utc::now();
        store
            .append_started_run("sync test", "run-1", started)
            .unwrap();
        store
            .append_completed_run("sync test", "run-2", started, 1)
            .unwrap();
        store
            .append_failed_run("sync test", "run-2", started, "boom")
            .unwrap();
        store
            .append_completed_run("sync test", "run-3", started, 1)
            .unwrap();

        let ids = store.completed_run_ids();
        assert_eq!(ids.into_iter().collect::<Vec<_>>(), vec!["run-3"]);
        assert_eq!(
            store.incomplete_run_ids().into_iter().collect::<Vec<_>>(),
            vec!["run-1", "run-2"]
        );
    }

    #[test]
    fn upsert_sync_states_batch_updates_in_one_write() {
        let dir = tempfile::tempdir().unwrap();
        let store = ManifestStore::open(dir.path()).unwrap();
        let now = Utc::now();
        store
            .upsert_sync_states(&[
                SyncStateRecord {
                    source: "polymarket".into(),
                    cursor_key: "prices:polymarket:tok-1".into(),
                    cursor_value: r#"{"start_ts":1}"#.into(),
                    last_ts: None,
                    updated_at: now,
                },
                SyncStateRecord {
                    source: "polymarket".into(),
                    cursor_key: "prices:polymarket:tok-2".into(),
                    cursor_value: r#"{"start_ts":2}"#.into(),
                    last_ts: None,
                    updated_at: now,
                },
            ])
            .unwrap();
        assert!(store.sync_state("polymarket", "prices:polymarket:tok-1").is_some());
        assert!(store.sync_state("polymarket", "prices:polymarket:tok-2").is_some());
    }

    #[test]
    fn remove_sync_states_where_drops_matching_rows() {
        let dir = tempfile::tempdir().unwrap();
        let store = ManifestStore::open(dir.path()).unwrap();
        let now = Utc::now();
        store
            .upsert_sync_state(SyncStateRecord {
                source: "collect".into(),
                cursor_key: "collect:hourly:polymarket:config".into(),
                cursor_value: "1".into(),
                last_ts: None,
                updated_at: now,
            })
            .unwrap();
        store
            .upsert_sync_state(SyncStateRecord {
                source: "collect".into(),
                cursor_key: "collect:hourly:polymarket:tok-1".into(),
                cursor_value: "{}".into(),
                last_ts: None,
                updated_at: now,
            })
            .unwrap();

        let removed = store
            .remove_sync_states_where(|record| record.cursor_key.contains(":tok-"))
            .unwrap();
        assert_eq!(removed, 1);
        assert!(store.sync_state("collect", "collect:hourly:polymarket:config").is_some());
        assert!(store.sync_state("collect", "collect:hourly:polymarket:tok-1").is_none());
    }
}
