use chrono::Utc;

use crate::config::{SyncMarketsOptions, Table};
use crate::error::Result;
use crate::gamma::{fetch_all_events, FetchEventsParams, GammaEvent, GammaMarket};
use crate::http::HttpClient;
use crate::manifest::{new_run_id, ManifestStore, RunRecord, SyncStateRecord};
use crate::normalize::{
    events_batch, markets_batch, outcomes_batch, resolutions_batch as build_resolutions_batch,
};
use crate::parquet::write_snapshot;
use crate::paths::LakePaths;
use crate::quarantine::{sha256_hex, write_raw_json};

pub async fn sync_markets(options: SyncMarketsOptions) -> Result<SyncSummary> {
    let paths = LakePaths::new(&options.out);
    let store = ManifestStore::open(&options.out)?;
    let run_id = new_run_id();
    let started = Utc::now();
    let http = HttpClient::new(
        options.requests_per_second,
        options.max_retries,
        options.user_agent.clone(),
    )?;

    let (active, closed) = if options.all {
        (None, None)
    } else if options.closed {
        (Some(false), Some(true))
    } else {
        (Some(true), Some(false))
    };

    let params = FetchEventsParams {
        base_url: &options.gamma_base_url,
        active,
        closed,
        tag: options.tag.as_deref(),
        limit: 100,
        offset: 0,
    };

    let url = format!(
        "{}/events?limit={}&offset=0",
        options.gamma_base_url, params.limit
    );
    let body = http.get_bytes(&url).await?;
    write_raw_json(&paths, "gamma", &format!("events-{run_id}.json"), &body)?;
    let raw_sha = sha256_hex(&body);

    let events = fetch_all_events(&http, params, options.limit).await?;
    let markets = collect_markets(&events);

    let events_data = events_batch(&events, "gamma", &url, &raw_sha, &run_id)?;
    let markets_data = markets_batch(&markets, "gamma", &url, &raw_sha, &run_id)?;
    let outcomes_data = outcomes_batch(&markets, "gamma", &url, &raw_sha, &run_id)?;
    let resolutions_data =
        build_resolutions_batch(&markets, "gamma", &url, &raw_sha, &run_id)?;

    write_snapshot(&paths, Table::Events, &run_id, &[events_data])?;
    write_snapshot(&paths, Table::Markets, &run_id, &[markets_data])?;
    write_snapshot(&paths, Table::Outcomes, &run_id, &[outcomes_data])?;
    if resolutions_data.num_rows() > 0 {
        write_snapshot(&paths, Table::Resolutions, &run_id, &[resolutions_data])?;
    }

    store.upsert_sync_state(SyncStateRecord {
        source: "gamma".into(),
        cursor_key: "events".into(),
        cursor_value: events.len().to_string(),
        last_ts: Some(Utc::now()),
        updated_at: Utc::now(),
    })?;
    store.write_schema_records()?;
    crate::contract::refresh_contract(&paths)?;

    let rows = events.len() as i64 + markets.len() as i64;
    store.append_run(RunRecord {
        run_id: run_id.clone(),
        command: "sync markets".into(),
        started_at: started,
        finished_at: Some(Utc::now()),
        status: "complete".into(),
        rows_written: rows,
        oddsfox_version: env!("CARGO_PKG_VERSION").into(),
    })?;

    println!(
        "sync markets complete: {} events, {} markets (run={run_id})",
        events.len(),
        markets.len()
    );
    Ok(SyncSummary {
        events: events.len(),
        markets: markets.len(),
        run_id,
    })
}

fn collect_markets(events: &[GammaEvent]) -> Vec<GammaMarket> {
    events
        .iter()
        .flat_map(|event| {
            event
                .markets
                .iter()
                .cloned()
                .map(|mut market| {
                    if market.event_id.is_none() {
                        market.event_id = Some(event.id.clone());
                    }
                    market
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

pub async fn token_ids_for_market(out: &std::path::Path, market_id: &str) -> Result<Vec<String>> {
    let paths = LakePaths::new(out);
    let glob = paths.duckdb_parquet_glob(Table::Outcomes);
    let conn = crate::duckdb_engine::open_connection(None)?;
    let sql = format!(
        "SELECT token_id FROM read_parquet('{glob}') WHERE market_id = ? AND token_id IS NOT NULL"
    );
    let mut stmt = crate::duckdb_engine::map_duckdb(conn.prepare(&sql))?;
    let rows = crate::duckdb_engine::map_duckdb(stmt.query_map([market_id], |row| row.get::<_, String>(0)))?;
    Ok(rows.filter_map(|r| r.ok()).collect())
}

pub async fn top_token_ids(out: &std::path::Path, limit: usize) -> Result<Vec<String>> {
    let paths = LakePaths::new(out);
    let markets_glob = paths.duckdb_parquet_glob(Table::Markets);
    let outcomes_glob = paths.duckdb_parquet_glob(Table::Outcomes);
    let conn = crate::duckdb_engine::open_connection(None)?;
    let sql = format!(
        "SELECT o.token_id
         FROM read_parquet('{outcomes_glob}') o
         JOIN read_parquet('{markets_glob}') m ON o.market_id = m.market_id
         WHERE m.active = true AND o.token_id IS NOT NULL
         ORDER BY m.volume_24h DESC NULLS LAST
         LIMIT {limit}"
    );
    let mut stmt = crate::duckdb_engine::map_duckdb(conn.prepare(&sql))?;
    let rows = crate::duckdb_engine::map_duckdb(stmt.query_map([], |row| row.get::<_, String>(0)))?;
    Ok(rows.filter_map(|r| r.ok()).collect())
}

#[derive(Debug, Clone)]
pub struct SyncSummary {
    pub events: usize,
    pub markets: usize,
    pub run_id: String,
}
