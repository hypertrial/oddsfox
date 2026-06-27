use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncStateRecord {
    pub source: String,
    pub cursor_key: String,
    pub cursor_value: String,
    pub last_ts: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunRecord {
    pub run_id: String,
    pub command: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub status: String,
    pub rows_written: i64,
    pub oddsfox_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionRecord {
    pub oddsfox_version: String,
    pub schema_version: String,
    pub lake_layout_version: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaRecord {
    pub table: String,
    pub schema_version: String,
    pub column_count: i32,
    pub columns_json: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataQualityRecord {
    pub check_name: String,
    pub entity_type: String,
    pub entity_id: String,
    pub severity: String,
    pub message: String,
    pub checked_at: DateTime<Utc>,
}
