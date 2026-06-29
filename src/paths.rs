use std::path::{Path, PathBuf};

use chrono::NaiveDate;

use crate::config::Table;

pub const LAKE_LAYOUT_VERSION: &str = "medallion-v2";

#[derive(Clone)]
pub struct LakePaths {
    pub root: PathBuf,
}

impl LakePaths {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn config_file(&self) -> PathBuf {
        self.root.join("oddsfox.toml")
    }

    pub fn catalog_db(&self) -> PathBuf {
        self.root.join("catalog.duckdb")
    }

    pub fn metadata_dir(&self) -> PathBuf {
        self.root.join("_metadata")
    }

    pub fn sync_state_manifest(&self) -> PathBuf {
        self.metadata_dir().join("sync_state.parquet")
    }

    pub fn runs_manifest(&self) -> PathBuf {
        self.metadata_dir().join("runs.parquet")
    }

    pub fn schemas_manifest(&self) -> PathBuf {
        self.metadata_dir().join("schemas.parquet")
    }

    pub fn data_quality_manifest(&self) -> PathBuf {
        self.metadata_dir().join("data_quality.parquet")
    }

    pub fn version_manifest(&self) -> PathBuf {
        self.metadata_dir().join("version.parquet")
    }

    pub fn contract_manifest(&self) -> PathBuf {
        self.metadata_dir().join("contract.json")
    }

    pub fn lake_lock_file(&self) -> PathBuf {
        self.metadata_dir().join(".oddsfox.lock")
    }

    pub fn logs_dir(&self) -> PathBuf {
        self.root.join("logs")
    }

    pub fn bronze_dir(&self) -> PathBuf {
        self.root.join("bronze")
    }

    pub fn bronze_table_dir(&self, table: Table) -> PathBuf {
        self.bronze_dir().join(table.as_str())
    }

    pub fn silver_dir(&self, name: &str) -> PathBuf {
        self.root.join("silver").join(name)
    }

    pub fn gold_dir(&self, name: &str) -> PathBuf {
        self.root.join("gold").join(name)
    }

    pub fn raw_dir(&self, source: &str) -> PathBuf {
        self.root.join("_raw").join(source)
    }

    pub fn raw_file(&self, source: &str, filename: &str) -> PathBuf {
        self.raw_dir(source).join(filename)
    }

    pub fn date_partition_dir(&self, table: Table, date: NaiveDate) -> PathBuf {
        self.bronze_table_dir(table).join(format!("date={date}"))
    }

    pub fn snapshot_partition_file(&self, table: Table, run_id: &str) -> PathBuf {
        self.bronze_table_dir(table)
            .join(format!("run={run_id}"))
            .join("part.parquet")
    }

    pub fn time_series_file(&self, table: Table, date: NaiveDate, part: &str) -> PathBuf {
        self.date_partition_dir(table, date)
            .join(format!("{part}.parquet"))
    }

    pub fn token_partition_file(&self, table: Table, token_id: &str) -> PathBuf {
        self.bronze_table_dir(table)
            .join(format!("token={token_id}"))
            .join("part.parquet")
    }

    pub fn hourly_price_window_file(&self, source: &str, token_id: &str, start_ts: i64) -> PathBuf {
        self.bronze_table_dir(Table::Prices)
            .join("windows")
            .join(source)
            .join(safe_path_segment(token_id))
            .join(format!("{start_ts}.parquet"))
    }

    pub fn quarantine_bad_rows(&self, table: Table, run_id: &str) -> PathBuf {
        self.root
            .join("_quarantine")
            .join("bad_rows")
            .join(table.as_str())
            .join(format!("bad_rows-{run_id}.jsonl"))
    }

    pub fn quarantine_bad_file(&self, source: &str, filename: &str) -> PathBuf {
        self.root
            .join("_quarantine")
            .join("bad_files")
            .join(source)
            .join(filename)
    }

    pub fn layer_parquet_glob(&self, layer: &str, name: &str) -> String {
        self.root
            .join(layer)
            .join(name)
            .join("**/*.parquet")
            .to_string_lossy()
            .into_owned()
    }

    pub fn duckdb_parquet_glob(&self, table: Table) -> String {
        self.layer_parquet_glob("bronze", table.as_str())
    }

    pub fn scaffold_dirs(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(self.metadata_dir())?;
        std::fs::create_dir_all(self.bronze_dir())?;
        std::fs::create_dir_all(self.root.join("silver"))?;
        std::fs::create_dir_all(self.root.join("gold"))?;
        std::fs::create_dir_all(self.root.join("_raw"))?;
        std::fs::create_dir_all(self.root.join("_quarantine").join("bad_rows"))?;
        std::fs::create_dir_all(self.root.join("_quarantine").join("bad_files"))?;
        std::fs::create_dir_all(self.logs_dir())?;
        for table in Table::all() {
            std::fs::create_dir_all(self.bronze_table_dir(*table))?;
        }
        Ok(())
    }

    pub fn ensure_parent(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(())
    }
}

fn safe_path_segment(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '=') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bronze_paths() {
        let paths = LakePaths::new("./lake");
        assert_eq!(
            paths.bronze_table_dir(Table::Markets),
            PathBuf::from("./lake/bronze/markets")
        );
    }
}
