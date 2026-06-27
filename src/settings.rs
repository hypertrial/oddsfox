use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::config::{
    DEFAULT_CLOB_BASE_URL, DEFAULT_DATA_BASE_URL, DEFAULT_GAMMA_BASE_URL,
    DEFAULT_MAX_RETRIES, DEFAULT_RAW_RETENTION_DAYS, DEFAULT_REQUESTS_PER_SECOND,
    DEFAULT_USER_AGENT, DEFAULT_WS_MARKET_URL,
};
use crate::error::{OddsfoxError, Result};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OddsfoxConfig {
    #[serde(default)]
    pub data: DataSection,
    #[serde(default)]
    pub polymarket: PolymarketSection,
    #[serde(default)]
    pub sync: SyncSection,
    #[serde(default)]
    pub duckdb: DuckDbSection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSection {
    #[serde(default = "default_home")]
    pub home: String,
    #[serde(default = "default_store")]
    pub store: String,
    #[serde(default = "default_raw_retention")]
    pub raw_retention_days: u32,
}

impl Default for DataSection {
    fn default() -> Self {
        Self {
            home: default_home(),
            store: default_store(),
            raw_retention_days: default_raw_retention(),
        }
    }
}

fn default_home() -> String {
    dirs_home().unwrap_or_else(|| "./.oddsfox".into())
}

fn default_store() -> String {
    "duckdb".into()
}

fn default_raw_retention() -> u32 {
    DEFAULT_RAW_RETENTION_DAYS
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymarketSection {
    #[serde(default = "default_gamma_url")]
    pub gamma_base_url: String,
    #[serde(default = "default_clob_url")]
    pub clob_base_url: String,
    #[serde(default = "default_data_url")]
    pub data_base_url: String,
    #[serde(default = "default_ws_url")]
    pub ws_market_url: String,
}

impl Default for PolymarketSection {
    fn default() -> Self {
        Self {
            gamma_base_url: default_gamma_url(),
            clob_base_url: default_clob_url(),
            data_base_url: default_data_url(),
            ws_market_url: default_ws_url(),
        }
    }
}

fn default_gamma_url() -> String {
    DEFAULT_GAMMA_BASE_URL.into()
}

fn default_clob_url() -> String {
    DEFAULT_CLOB_BASE_URL.into()
}

fn default_data_url() -> String {
    DEFAULT_DATA_BASE_URL.into()
}

fn default_ws_url() -> String {
    DEFAULT_WS_MARKET_URL.into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncSection {
    #[serde(default = "default_rps")]
    pub requests_per_second: f64,
    #[serde(default = "default_retries")]
    pub max_retries: u32,
    #[serde(default = "default_user_agent")]
    pub user_agent: String,
}

impl Default for SyncSection {
    fn default() -> Self {
        Self {
            requests_per_second: default_rps(),
            max_retries: default_retries(),
            user_agent: default_user_agent(),
        }
    }
}

fn default_rps() -> f64 {
    DEFAULT_REQUESTS_PER_SECOND
}

fn default_retries() -> u32 {
    DEFAULT_MAX_RETRIES
}

fn default_user_agent() -> String {
    DEFAULT_USER_AGENT.into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuckDbSection {
    #[serde(default = "default_db")]
    pub database: String,
}

impl Default for DuckDbSection {
    fn default() -> Self {
        Self {
            database: default_db(),
        }
    }
}

fn default_db() -> String {
    "catalog.duckdb".into()
}

pub fn dirs_home() -> Option<String> {
    std::env::var("HOME")
        .ok()
        .map(|home| format!("{home}/.oddsfox"))
}

pub fn load_config(path: &Path) -> Result<OddsfoxConfig> {
    let contents = std::fs::read_to_string(path)?;
    toml::from_str(&contents).map_err(|err| OddsfoxError::Config(err.to_string()))
}

pub fn save_config(path: &Path, config: &OddsfoxConfig) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let contents = toml::to_string_pretty(config).map_err(|err| OddsfoxError::Config(err.to_string()))?;
    std::fs::write(path, contents)?;
    Ok(())
}

pub fn resolve_config(config_path: Option<&Path>, out: Option<&Path>) -> Result<OddsfoxConfig> {
    if let Some(path) = config_path {
        return load_config(path);
    }
    if let Some(out) = out {
        let candidate = out.join("oddsfox.toml");
        if candidate.exists() {
            return load_config(&candidate);
        }
    }
    Ok(OddsfoxConfig::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_roundtrip() {
        let config = OddsfoxConfig::default();
        let raw = toml::to_string(&config).unwrap();
        let parsed: OddsfoxConfig = toml::from_str(&raw).unwrap();
        assert_eq!(parsed.polymarket.gamma_base_url, DEFAULT_GAMMA_BASE_URL);
    }
}
