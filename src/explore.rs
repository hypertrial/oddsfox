use std::path::Path;

use crate::config::Table;
use crate::error::{OddsfoxError, Result};
use crate::paths::LakePaths;

pub fn search(out: &Path, query: &str) -> Result<Vec<SearchHit>> {
    let paths = LakePaths::new(out);
    let glob = paths.duckdb_parquet_glob(Table::Markets);
    let conn = crate::duckdb_engine::open_connection(None)?;
    let sql = format!(
        "SELECT market_id, question, active, volume_24h
         FROM read_parquet('{glob}')
         WHERE lower(question) LIKE lower('%' || ? || '%')
         ORDER BY volume_24h DESC NULLS LAST
         LIMIT 25"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([query], |row| {
        Ok(SearchHit {
            market_id: row.get(0)?,
            question: row.get(1)?,
            active: row.get(2)?,
            volume_24h: row.get(3)?,
        })
    })?;
    Ok(rows.filter_map(|r| r.ok()).collect())
}

pub fn market_detail(out: &Path, market_id: &str) -> Result<MarketDetail> {
    let paths = LakePaths::new(out);
    let markets_glob = paths.duckdb_parquet_glob(Table::Markets);
    let outcomes_glob = paths.duckdb_parquet_glob(Table::Outcomes);
    let conn = crate::duckdb_engine::open_connection(None)?;
    let sql = format!(
        "SELECT market_id, event_id, question, active, closed, resolved, volume, volume_24h, liquidity
         FROM read_parquet('{markets_glob}')
         WHERE market_id = ?"
    );
    let mut stmt = conn.prepare(&sql)?;
    let market = stmt
        .query_row([market_id], |row| {
            Ok(MarketDetail {
                market_id: row.get(0)?,
                event_id: row.get(1)?,
                question: row.get(2)?,
                active: row.get(3)?,
                closed: row.get(4)?,
                resolved: row.get(5)?,
                volume: row.get(6)?,
                volume_24h: row.get(7)?,
                liquidity: row.get(8)?,
                outcomes: Vec::new(),
            })
        })
        .map_err(|_| OddsfoxError::NotFound {
            kind: "market".into(),
            id: market_id.to_string(),
        })?;

    let outcomes_sql = format!(
        "SELECT outcome_index, outcome_name, token_id, is_winner
         FROM read_parquet('{outcomes_glob}')
         WHERE market_id = ?
         ORDER BY outcome_index"
    );
    let mut outcomes_stmt = conn.prepare(&outcomes_sql)?;
    let outcomes = outcomes_stmt
        .query_map([market_id], |row| {
            Ok(OutcomeDetail {
                outcome_index: row.get(0)?,
                outcome_name: row.get(1)?,
                token_id: row.get(2)?,
                is_winner: row.get(3)?,
            })
        })?
        .filter_map(|r| r.ok())
        .collect();

    Ok(MarketDetail { outcomes, ..market })
}

pub fn event_detail(out: &Path, event_id: &str) -> Result<EventDetail> {
    let paths = LakePaths::new(out);
    let glob = paths.duckdb_parquet_glob(Table::Events);
    let conn = crate::duckdb_engine::open_connection(None)?;
    let sql = format!(
        "SELECT event_id, slug, title, category, active, closed
         FROM read_parquet('{glob}')
         WHERE event_id = ?"
    );
    conn.query_row(&sql, [event_id], |row| {
        Ok(EventDetail {
            event_id: row.get(0)?,
            slug: row.get(1)?,
            title: row.get(2)?,
            category: row.get(3)?,
            active: row.get(4)?,
            closed: row.get(5)?,
        })
    })
    .map_err(|_| OddsfoxError::NotFound {
        kind: "event".into(),
        id: event_id.to_string(),
    })
}

pub fn resolved_markets(out: &Path, since: Option<&str>) -> Result<Vec<MarketDetail>> {
    let paths = LakePaths::new(out);
    let glob = paths.duckdb_parquet_glob(Table::Markets);
    let conn = crate::duckdb_engine::open_connection(None)?;
    let since_filter = since
        .map(|s| format!("AND resolution_time >= '{s}'"))
        .unwrap_or_default();
    let sql = format!(
        "SELECT market_id, event_id, question, active, closed, resolved, volume, volume_24h, liquidity
         FROM read_parquet('{glob}')
         WHERE resolved = true {since_filter}
         ORDER BY resolution_time DESC NULLS LAST
         LIMIT 50"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        Ok(MarketDetail {
            market_id: row.get(0)?,
            event_id: row.get(1)?,
            question: row.get(2)?,
            active: row.get(3)?,
            closed: row.get(4)?,
            resolved: row.get(5)?,
            volume: row.get(6)?,
            volume_24h: row.get(7)?,
            liquidity: row.get(8)?,
            outcomes: Vec::new(),
        })
    })?;
    Ok(rows.filter_map(|r| r.ok()).collect())
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SearchHit {
    pub market_id: String,
    pub question: Option<String>,
    pub active: Option<bool>,
    pub volume_24h: Option<f64>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MarketDetail {
    pub market_id: String,
    pub event_id: Option<String>,
    pub question: Option<String>,
    pub active: Option<bool>,
    pub closed: Option<bool>,
    pub resolved: Option<bool>,
    pub volume: Option<f64>,
    pub volume_24h: Option<f64>,
    pub liquidity: Option<f64>,
    pub outcomes: Vec<OutcomeDetail>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct OutcomeDetail {
    pub outcome_index: i32,
    pub outcome_name: Option<String>,
    pub token_id: Option<String>,
    pub is_winner: Option<bool>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct EventDetail {
    pub event_id: String,
    pub slug: Option<String>,
    pub title: Option<String>,
    pub category: Option<String>,
    pub active: Option<bool>,
    pub closed: Option<bool>,
}
