use chrono::Utc;

use crate::config::{SyncMarketsOptions, Table, TokenPairFilter};
use crate::duckdb_engine::{open_connection, read_parquet_sql};
use crate::error::Result;
use crate::gamma::{fetch_all_events, FetchEventsParams, GammaEvent, GammaMarket};
use crate::http::HttpClient;
use crate::manifest::{new_run_id, ManifestStore, SyncStateRecord};
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
    let resolutions_data = build_resolutions_batch(&markets, "gamma", &url, &raw_sha, &run_id)?;

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
    store.append_completed_run("sync markets", &run_id, started, rows)?;

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
    let source = read_parquet_sql(&glob);
    let conn = open_connection(None)?;
    let sql = format!("SELECT token_id FROM {source} WHERE market_id = ? AND token_id IS NOT NULL");
    let mut stmt = crate::duckdb_engine::map_duckdb(conn.prepare(&sql))?;
    let rows = crate::duckdb_engine::map_duckdb(
        stmt.query_map([market_id], |row| row.get::<_, String>(0)),
    )?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

pub async fn top_token_ids(out: &std::path::Path, limit: usize) -> Result<Vec<String>> {
    Ok(top_token_pairs(out, limit)
        .await?
        .into_iter()
        .map(|(token_id, _)| token_id)
        .collect())
}

pub async fn top_token_pairs(out: &std::path::Path, limit: usize) -> Result<Vec<(String, String)>> {
    let paths = LakePaths::new(out);
    let markets_glob = paths.duckdb_parquet_glob(Table::Markets);
    let outcomes_glob = paths.duckdb_parquet_glob(Table::Outcomes);
    let markets_source = read_parquet_sql(&markets_glob);
    let outcomes_source = read_parquet_sql(&outcomes_glob);
    let conn = open_connection(None)?;
    let sql = format!(
        "SELECT o.token_id, o.market_id
         FROM {outcomes_source} o
         JOIN {markets_source} m ON o.market_id = m.market_id
         WHERE m.active = true AND o.token_id IS NOT NULL
         ORDER BY m.volume_24h DESC NULLS LAST
         LIMIT {limit}"
    );
    query_token_pairs(&conn, &sql, &[])
}

pub async fn all_token_pairs(
    out: &std::path::Path,
    filter: &TokenPairFilter,
) -> Result<Vec<(String, String)>> {
    let paths = LakePaths::new(out);
    let markets_glob = paths.duckdb_parquet_glob(Table::Markets);
    let outcomes_glob = paths.duckdb_parquet_glob(Table::Outcomes);
    let events_glob = paths.duckdb_parquet_glob(Table::Events);
    let markets_source = read_parquet_sql(&markets_glob);
    let outcomes_source = read_parquet_sql(&outcomes_glob);
    let events_source = read_parquet_sql(&events_glob);
    let conn = open_connection(None)?;

    let mut sql = format!(
        "SELECT DISTINCT o.token_id, o.market_id
         FROM {outcomes_source} o
         JOIN {markets_source} m ON o.market_id = m.market_id"
    );
    let mut params: Vec<String> = Vec::new();

    if filter.tag.is_some() {
        sql.push_str(&format!(
            " JOIN {events_source} e ON m.event_id = e.event_id"
        ));
    }

    sql.push_str(" WHERE o.token_id IS NOT NULL");

    if let Some(active) = filter.active {
        sql.push_str(if active {
            " AND m.active = true"
        } else {
            " AND m.active = false"
        });
    }

    if let Some(tag) = filter.tag.as_deref() {
        sql.push_str(" AND e.tags LIKE ?");
        params.push(format!("%{tag}%"));
    }

    sql.push_str(" ORDER BY o.token_id");

    if let Some(limit) = filter.limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }

    query_token_pairs(&conn, &sql, &params)
}

fn query_token_pairs(
    conn: &duckdb::Connection,
    sql: &str,
    params: &[String],
) -> Result<Vec<(String, String)>> {
    let mut stmt = crate::duckdb_engine::map_duckdb(conn.prepare(sql))?;
    let rows = crate::duckdb_engine::map_duckdb(
        stmt.query_map(duckdb::params_from_iter(params.iter()), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        }),
    )?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

#[cfg(test)]
mod token_pair_tests {
    use super::*;
    use crate::config::Table;
    use crate::normalize::{markets_batch, outcomes_batch};
    use crate::parquet::write_snapshot;

    #[tokio::test]
    async fn all_token_pairs_returns_distinct_pairs() {
        let dir = tempfile::tempdir().unwrap();
        let paths = LakePaths::new(dir.path());
        paths.scaffold_dirs().unwrap();
        let run_id = "run-test";

        let markets = vec![crate::gamma::GammaMarket {
            id: "m1".into(),
            event_id: Some("e1".into()),
            conditionId: None,
            questionID: None,
            slug: None,
            question: Some("Q?".into()),
            description: None,
            active: Some(true),
            closed: Some(false),
            resolved: None,
            enableOrderBook: None,
            negRisk: None,
            liquidity: None,
            volume: None,
            volume24hr: None,
            openInterest: None,
            endDate: None,
            resolutionTime: None,
            resolutionSource: None,
            outcomes: Some("[\"Yes\",\"No\"]".into()),
            outcomePrices: None,
            clobTokenIds: Some("[\"tok-a\",\"tok-b\"]".into()),
            winningOutcome: None,
            winningOutcomeIndex: None,
        }];
        let markets_data = markets_batch(&markets, "gamma", "http://test", "sha", run_id).unwrap();
        let outcomes_data =
            outcomes_batch(&markets, "gamma", "http://test", "sha", run_id).unwrap();
        write_snapshot(&paths, Table::Markets, run_id, &[markets_data]).unwrap();
        write_snapshot(&paths, Table::Outcomes, run_id, &[outcomes_data]).unwrap();

        let pairs = all_token_pairs(dir.path(), &TokenPairFilter::default())
            .await
            .unwrap();
        assert_eq!(pairs.len(), 2);
        assert!(pairs.iter().any(|(t, m)| t == "tok-a" && m == "m1"));
        assert!(pairs.iter().any(|(t, m)| t == "tok-b" && m == "m1"));
    }

    #[tokio::test]
    async fn all_token_pairs_filters_by_active() {
        let dir = tempfile::tempdir().unwrap();
        let paths = LakePaths::new(dir.path());
        paths.scaffold_dirs().unwrap();
        let run_id = "run-test";

        let markets = vec![
            crate::gamma::GammaMarket {
                id: "m-active".into(),
                event_id: Some("e1".into()),
                conditionId: None,
                questionID: None,
                slug: None,
                question: Some("Active?".into()),
                description: None,
                active: Some(true),
                closed: Some(false),
                resolved: None,
                enableOrderBook: None,
                negRisk: None,
                liquidity: None,
                volume: None,
                volume24hr: None,
                openInterest: None,
                endDate: None,
                resolutionTime: None,
                resolutionSource: None,
                outcomes: Some("[\"Yes\"]".into()),
                outcomePrices: None,
                clobTokenIds: Some("[\"tok-active\"]".into()),
                winningOutcome: None,
                winningOutcomeIndex: None,
            },
            crate::gamma::GammaMarket {
                id: "m-closed".into(),
                event_id: Some("e2".into()),
                conditionId: None,
                questionID: None,
                slug: None,
                question: Some("Closed?".into()),
                description: None,
                active: Some(false),
                closed: Some(true),
                resolved: None,
                enableOrderBook: None,
                negRisk: None,
                liquidity: None,
                volume: None,
                volume24hr: None,
                openInterest: None,
                endDate: None,
                resolutionTime: None,
                resolutionSource: None,
                outcomes: Some("[\"Yes\"]".into()),
                outcomePrices: None,
                clobTokenIds: Some("[\"tok-closed\"]".into()),
                winningOutcome: None,
                winningOutcomeIndex: None,
            },
        ];
        let markets_data = markets_batch(&markets, "gamma", "http://test", "sha", run_id).unwrap();
        let outcomes_data =
            outcomes_batch(&markets, "gamma", "http://test", "sha", run_id).unwrap();
        write_snapshot(&paths, Table::Markets, run_id, &[markets_data]).unwrap();
        write_snapshot(&paths, Table::Outcomes, run_id, &[outcomes_data]).unwrap();

        let active_pairs = all_token_pairs(
            dir.path(),
            &TokenPairFilter {
                active: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(active_pairs, vec![("tok-active".into(), "m-active".into())]);
    }
}

#[derive(Debug, Clone)]
pub struct SyncSummary {
    pub events: usize,
    pub markets: usize,
    pub run_id: String,
}
