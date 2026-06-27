pub mod check;
pub mod clean;
pub mod cli;
pub mod clob;
pub mod config;
pub mod contract;
pub mod data;
pub mod duckdb;
pub mod duckdb_engine;
pub mod error;
pub mod explore;
pub mod gamma;
pub mod http;
pub mod ids;
pub mod init;
pub mod manifest;
pub mod metrics;
pub mod normalize;
pub mod ops;
pub mod parquet;
pub mod parquet_props;
pub mod paths;
pub mod prices;
pub mod quarantine;
pub mod quickstart;
pub mod repair;
pub mod schema;
pub mod server;
pub mod settings;
pub mod snapshot;
pub mod sql_cmd;
pub mod sync;

pub use error::{OddsfoxError, Result};

#[derive(Debug, Clone)]
pub struct Lake {
    pub root: std::path::PathBuf,
}

impl Lake {
    pub fn open(path: impl Into<std::path::PathBuf>) -> Result<Self> {
        Ok(Self { root: path.into() })
    }

    pub async fn sync_markets(&self, options: config::SyncMarketsOptions) -> Result<sync::SyncSummary> {
        let mut opts = options;
        opts.out = self.root.clone();
        sync::sync_markets(opts).await
    }

    pub async fn snapshot_books(&self, options: config::SnapshotBooksOptions) -> Result<()> {
        let mut opts = options;
        opts.out = self.root.clone();
        snapshot::snapshot_books(opts).await
    }

    pub fn check(&self) -> Result<check::CheckReport> {
        check::check_lake(&self.root)
    }

    pub fn create_duckdb_views(&self, db: impl Into<std::path::PathBuf>) -> Result<()> {
        duckdb::run(&config::DuckDbOptions {
            out: self.root.clone(),
            db: db.into(),
        })
    }
}
