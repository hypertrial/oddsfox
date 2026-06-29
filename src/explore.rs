use std::path::Path;

use chrono::NaiveDate;

use crate::config::Table;
use crate::duckdb_engine::{bronze_source_sql, open_connection};
use crate::error::{OddsfoxError, Result};
use crate::paths::LakePaths;

pub fn search(out: &Path, query: &str) -> Result<Vec<SearchHit>> {
    let paths = LakePaths::new(out);
    let source = bronze_source_sql(&paths, Table::Markets);
    let conn = open_connection(None)?;
    let sql = format!(
        "SELECT market_id, question, active, volume_24h
         FROM {source}
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
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

pub fn market_detail(out: &Path, market_id: &str) -> Result<MarketDetail> {
    let paths = LakePaths::new(out);
    let markets_source = bronze_source_sql(&paths, Table::Markets);
    let outcomes_source = bronze_source_sql(&paths, Table::Outcomes);
    let conn = open_connection(None)?;
    let sql = format!(
        "SELECT market_id, event_id, question, active, closed, resolved, volume, volume_24h, liquidity
         FROM {markets_source}
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
         FROM {outcomes_source}
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
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(MarketDetail { outcomes, ..market })
}

pub fn event_detail(out: &Path, event_id: &str) -> Result<EventDetail> {
    let paths = LakePaths::new(out);
    let source = bronze_source_sql(&paths, Table::Events);
    let conn = open_connection(None)?;
    let sql = format!(
        "SELECT event_id, slug, title, category, active, closed
         FROM {source}
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

pub fn resolved_markets(out: &Path, since: Option<NaiveDate>) -> Result<Vec<MarketDetail>> {
    let paths = LakePaths::new(out);
    let source = bronze_source_sql(&paths, Table::Markets);
    let conn = open_connection(None)?;
    let since_filter = if since.is_some() {
        " AND resolution_time >= CAST(? AS TIMESTAMP)"
    } else {
        ""
    };
    let sql = format!(
        "SELECT market_id, event_id, question, active, closed, resolved, volume, volume_24h, liquidity
         FROM {source}
         WHERE resolved = true{since_filter}
         ORDER BY resolution_time DESC NULLS LAST
         LIMIT 50"
    );
    let mut stmt = conn.prepare(&sql)?;
    let params: Vec<String> = since.map(|d| d.to_string()).into_iter().collect();
    let rows = stmt.query_map(duckdb::params_from_iter(params.iter()), |row| {
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
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gamma::GammaMarket;
    use crate::normalize::markets_batch;
    use crate::parquet::write_snapshot;

    fn resolved_market(id: &str, resolution_time: &str) -> GammaMarket {
        GammaMarket {
            id: id.into(),
            event_id: Some("event-1".into()),
            conditionId: None,
            questionID: None,
            slug: None,
            question: Some(format!("{id}?")),
            description: None,
            active: Some(false),
            closed: Some(true),
            resolved: Some(true),
            enableOrderBook: None,
            negRisk: None,
            liquidity: None,
            volume: None,
            volume24hr: None,
            openInterest: None,
            endDate: None,
            resolutionTime: Some(resolution_time.into()),
            resolutionSource: Some("test".into()),
            outcomes: None,
            outcomePrices: None,
            clobTokenIds: None,
            winningOutcome: None,
            winningOutcomeIndex: None,
        }
    }

    #[test]
    fn resolved_markets_filters_by_since_date() {
        let dir = tempfile::tempdir().unwrap();
        let paths = LakePaths::new(dir.path());
        paths.scaffold_dirs().unwrap();

        let markets = vec![
            resolved_market("old", "2023-12-31T00:00:00Z"),
            resolved_market("new", "2024-01-02T00:00:00Z"),
        ];
        let batch = markets_batch(&markets, "gamma", "http://test", "sha", "run-1").unwrap();
        write_snapshot(&paths, Table::Markets, "run-1", &[batch]).unwrap();
        crate::manifest::ManifestStore::open(dir.path())
            .unwrap()
            .append_completed_run("test", "run-1", chrono::Utc::now(), 2)
            .unwrap();

        let since = chrono::NaiveDate::from_ymd_opt(2024, 1, 1);
        let resolved = resolved_markets(dir.path(), since).unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].market_id, "new");
    }
}
