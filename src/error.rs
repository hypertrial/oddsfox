use std::path::PathBuf;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, OddsfoxError>;

#[derive(Debug, Error)]
pub enum OddsfoxError {
    #[error("invalid date `{0}`: expected YYYY-MM-DD")]
    InvalidDate(String),

    #[error("invalid table `{0}`")]
    InvalidTable(String),

    #[error("invalid id `{value}` for {kind}")]
    InvalidId { kind: String, value: String },

    #[error("lake path not found: {0}")]
    LakeNotFound(PathBuf),

    #[error("manifest not found at {0}")]
    ManifestNotFound(PathBuf),

    #[error("lake is locked by another oddsfox process: {0}")]
    LakeLocked(PathBuf),

    #[error("HTTP error for {url}: status {status}")]
    Http { url: String, status: u16 },

    #[error("download failed for {url}: {message}")]
    Download { url: String, message: String },

    #[error("parse error in {table}: {message}")]
    Parse { table: String, message: String },

    #[error("parquet write failed: {0}")]
    ParquetWrite(String),

    #[error("manifest error: {0}")]
    Manifest(String),

    #[error("check failed with {count} issue(s)")]
    CheckFailed { count: usize },

    #[error("sync incomplete: {message}")]
    SyncIncomplete { message: String },

    #[error("duckdb error: {0}")]
    DuckDb(String),

    #[error("config error: {0}")]
    Config(String),

    #[error("not found: {kind} `{id}`")]
    NotFound { kind: String, id: String },

    #[error("websocket error: {0}")]
    WebSocket(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<duckdb::Error> for OddsfoxError {
    fn from(err: duckdb::Error) -> Self {
        OddsfoxError::DuckDb(err.to_string())
    }
}
