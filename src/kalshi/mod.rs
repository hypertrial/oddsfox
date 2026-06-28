pub mod client;
pub mod models;
pub mod normalize;

use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

use chrono::Utc;
use futures_util::stream::{self, StreamExt};
use futures_util::TryStreamExt;

use crate::config::{
    kalshi_period_interval, resolve_price_time_range, KalshiStatus, SnapshotBooksOptions,
    SyncMarketsOptions, SyncPricesOptions, Table,
};
use crate::duckdb_engine::{open_connection, read_parquet_sql};
use crate::error::{OddsfoxError, Result};
use crate::http::HttpClient;
use crate::manifest::{new_run_id, ManifestStore, SyncStateRecord};
use crate::parquet::{write_snapshot, write_token_series};
use crate::paths::LakePaths;
use crate::quarantine::{sha256_hex, write_raw_json};

use client::{KalshiAuth, KalshiClient};

fn client_from_parts(
    base_url: String,
    key_id: Option<String>,
    private_key_path: Option<std::path::PathBuf>,
    requests_per_second: f64,
    max_retries: u32,
    user_agent: String,
) -> Result<KalshiClient> {
    let http = HttpClient::new(requests_per_second, max_retries, user_agent)?;
    let auth = match (key_id, private_key_path) {
        (Some(key_id), Some(path)) => Some(KalshiAuth::from_key_file(key_id, &path)?),
        _ => None,
    };
    Ok(KalshiClient::new(base_url, http, auth))
}

pub async fn sync_markets(options: SyncMarketsOptions) -> Result<crate::sync::SyncSummary> {
    let paths = LakePaths::new(&options.out);
    let store = ManifestStore::open(&options.out)?;
    let run_id = new_run_id();
    let started = Utc::now();
    let client = client_from_parts(
        options.kalshi_rest_base_url.clone(),
        options.kalshi_key_id.clone(),
        options.kalshi_private_key_path.clone(),
        options.requests_per_second,
        options.max_retries,
        options.user_agent.clone(),
    )?;

    let status = options.status.unwrap_or({
        if options.all {
            KalshiStatus::All
        } else if options.closed {
            KalshiStatus::Closed
        } else {
            KalshiStatus::Open
        }
    });
    let response = if let Some(event_ticker) = options.event.as_deref() {
        let event = client.get_event(event_ticker).await?;
        models::KalshiMarketResponse {
            markets: event.markets,
            cursor: None,
        }
    } else {
        client
            .get_markets(status, options.series.as_deref(), options.limit)
            .await?
    };
    let markets = response.markets;
    let events = normalize::events_from_markets(&markets);
    let raw = serde_json::to_vec(&markets)?;
    let raw_sha = sha256_hex(&raw);
    let raw_url = client.markets_url(status, options.series.as_deref(), options.limit);
    write_raw_json(&paths, "kalshi", &format!("markets-{run_id}.json"), &raw)?;

    let events_data = normalize::events_batch(&events, &raw_url, &raw_sha, &run_id)?;
    let markets_data = normalize::markets_batch(&markets, &raw_url, &raw_sha, &run_id)?;
    let outcomes_data = normalize::outcomes_batch(&markets, &raw_url, &raw_sha, &run_id)?;
    let resolutions_data = normalize::resolutions_batch(&markets, &raw_url, &raw_sha, &run_id)?;

    write_snapshot(&paths, Table::Events, &run_id, &[events_data])?;
    write_snapshot(&paths, Table::Markets, &run_id, &[markets_data])?;
    write_snapshot(&paths, Table::Outcomes, &run_id, &[outcomes_data])?;
    if resolutions_data.num_rows() > 0 {
        write_snapshot(&paths, Table::Resolutions, &run_id, &[resolutions_data])?;
    }

    store.upsert_sync_state(SyncStateRecord {
        source: "kalshi".into(),
        cursor_key: "markets".into(),
        cursor_value: markets.len().to_string(),
        last_ts: Some(Utc::now()),
        updated_at: Utc::now(),
    })?;
    store.write_schema_records()?;
    crate::contract::refresh_contract(&paths)?;

    store.append_completed_run(
        "sync markets --source kalshi",
        &run_id,
        started,
        (events.len() + markets.len()) as i64,
    )?;

    println!(
        "sync kalshi markets complete: {} events, {} markets (run={run_id})",
        events.len(),
        markets.len()
    );
    Ok(crate::sync::SyncSummary {
        events: events.len(),
        markets: markets.len(),
        run_id,
    })
}

pub async fn sync_prices(options: SyncPricesOptions) -> Result<()> {
    let paths = LakePaths::new(&options.out);
    let store = ManifestStore::open(&options.out)?;
    let run_id = new_run_id();
    let started = Utc::now();
    let client = client_from_parts(
        options.kalshi_rest_base_url.clone(),
        options.kalshi_key_id.clone(),
        options.kalshi_private_key_path.clone(),
        options.requests_per_second,
        options.max_retries,
        options.user_agent.clone(),
    )?;
    let time_range = resolve_price_time_range(options.since, options.until, options.recent_hours);
    let (start_ts, end_ts) = required_candlestick_range(&time_range)?;
    let period = kalshi_period_interval(options.period.or(options.fidelity))?;
    let fidelity = Some(period as i32);
    let merge_window = options.recent_hours.is_some();
    let concurrency = options.concurrency.max(1);
    let overwrite = options.overwrite;

    let markets = if let Some(market_id) = options.market_id.clone() {
        let ticker = normalize::strip_kalshi_market_id(&market_id);
        let series = options
            .series
            .clone()
            .or_else(|| infer_series(ticker))
            .ok_or_else(|| OddsfoxError::SyncIncomplete {
                message: "pass --series <series_ticker> for Kalshi candlesticks".into(),
            })?;
        vec![(market_id, Some(series))]
    } else if options.active || options.all {
        active_markets_for_prices(&options)?
    } else {
        return Err(OddsfoxError::SyncIncomplete {
            message: "pass --market, --active, or --all for Kalshi price sync".into(),
        });
    };

    if markets.is_empty() {
        return Err(OddsfoxError::SyncIncomplete {
            message: "no active Kalshi markets selected; run `sync markets --source kalshi` first"
                .into(),
        });
    }

    let total = markets.len();
    let processed = Arc::new(AtomicUsize::new(0));
    let total_points = Arc::new(AtomicI64::new(0));

    stream::iter(markets)
        .map(|(market_id, series)| {
            let client = client.clone();
            let paths = paths.clone();
            let run_id = run_id.clone();
            let processed = Arc::clone(&processed);
            let total_points = Arc::clone(&total_points);
            async move {
                let rows = sync_market_prices(KalshiMarketPriceSync {
                    client: &client,
                    paths: &paths,
                    run_id: &run_id,
                    market_id: &market_id,
                    series: series.as_deref(),
                    period,
                    fidelity,
                    start_ts,
                    end_ts,
                    merge_window,
                    overwrite,
                })
                .await?;
                total_points.fetch_add(rows, Ordering::Relaxed);
                let done = processed.fetch_add(1, Ordering::Relaxed) + 1;
                if done == total || done.is_multiple_of(25) {
                    println!(
                        "sync kalshi prices progress: {done}/{total} markets, {} points",
                        total_points.load(Ordering::Relaxed)
                    );
                }
                Ok::<(), OddsfoxError>(())
            }
        })
        .buffer_unordered(concurrency)
        .try_collect::<Vec<()>>()
        .await?;

    let rows = total_points.load(Ordering::Relaxed);
    store.append_completed_run("sync prices --source kalshi", &run_id, started, rows)?;
    println!("sync kalshi prices complete: {rows} points across {total} markets (run={run_id})");
    Ok(())
}

pub fn active_markets_for_prices(
    options: &SyncPricesOptions,
) -> Result<Vec<(String, Option<String>)>> {
    let paths = LakePaths::new(&options.out);
    let glob = paths.duckdb_parquet_glob(Table::Markets);
    if !crate::duckdb_engine::glob_exists(&glob) {
        return Ok(Vec::new());
    }
    let source = read_parquet_sql(&glob);
    let conn = open_connection(None)?;
    let limit = options
        .limit
        .map(|n| format!(" LIMIT {n}"))
        .unwrap_or_default();
    let active_filter = if options.all {
        options
            .filter_active
            .map(|active| format!(" AND active = {active}"))
            .unwrap_or_default()
    } else {
        " AND active = true".to_string()
    };
    let sql = format!(
        "SELECT market_id, json_extract_string(raw_json, '$.series_ticker')
         FROM {source}
         WHERE source = 'kalshi'{active_filter}
         ORDER BY market_id{limit}"
    );
    let mut stmt = crate::duckdb_engine::map_duckdb(conn.prepare(&sql))?;
    let rows = crate::duckdb_engine::map_duckdb(stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
    }))?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

async fn sync_market_prices(ctx: KalshiMarketPriceSync<'_>) -> Result<i64> {
    let ticker = normalize::strip_kalshi_market_id(ctx.market_id);
    let series = ctx
        .series
        .map(str::to_string)
        .or_else(|| infer_series(ticker))
        .ok_or_else(|| OddsfoxError::SyncIncomplete {
            message: format!(
                "could not infer series for Kalshi market `{}`",
                ctx.market_id
            ),
        })?;
    let candles = ctx
        .client
        .get_candlesticks(
            &series,
            ticker,
            ctx.period,
            Some(ctx.start_ts),
            Some(ctx.end_ts),
        )
        .await?;
    let (mut yes, mut no, _skipped) = normalize::price_points_from_candlesticks(&candles);
    let market = normalize::kalshi_market_id(ticker);
    let yes_token = normalize::kalshi_token_id(ticker, "yes");
    let no_token = normalize::kalshi_token_id(ticker, "no");
    let mut rows = 0;

    if ctx.merge_window {
        yes = merge_side_prices(
            &ctx.paths.token_partition_file(Table::Prices, &yes_token),
            &yes,
            ctx.start_ts,
            ctx.end_ts,
        )?;
        no = merge_side_prices(
            &ctx.paths.token_partition_file(Table::Prices, &no_token),
            &no,
            ctx.start_ts,
            ctx.end_ts,
        )?;
    } else {
        let yes_path = ctx.paths.token_partition_file(Table::Prices, &yes_token);
        let no_path = ctx.paths.token_partition_file(Table::Prices, &no_token);
        if !ctx.overwrite && (yes_path.exists() || no_path.exists()) {
            return Ok(0);
        }
    }

    if !yes.is_empty() {
        let batch = crate::normalize::prices_batch(
            &yes_token,
            Some(&market),
            &yes,
            "kalshi_candlesticks",
            ctx.fidelity,
            ctx.run_id,
        )?;
        rows += batch.num_rows() as i64;
        write_token_series(ctx.paths, Table::Prices, &yes_token, &[batch])?;
    }
    if !no.is_empty() {
        let batch = crate::normalize::prices_batch(
            &no_token,
            Some(&market),
            &no,
            "kalshi_candlesticks",
            ctx.fidelity,
            ctx.run_id,
        )?;
        rows += batch.num_rows() as i64;
        write_token_series(ctx.paths, Table::Prices, &no_token, &[batch])?;
    }
    Ok(rows)
}

struct KalshiMarketPriceSync<'a> {
    client: &'a KalshiClient,
    paths: &'a LakePaths,
    run_id: &'a str,
    market_id: &'a str,
    series: Option<&'a str>,
    period: u32,
    fidelity: Option<i32>,
    start_ts: i64,
    end_ts: i64,
    merge_window: bool,
    overwrite: bool,
}

fn required_candlestick_range(time_range: &crate::config::PriceTimeRange) -> Result<(i64, i64)> {
    match (time_range.start_ts, time_range.end_ts) {
        (Some(start), Some(end)) => Ok((start, end)),
        _ => Err(OddsfoxError::SyncIncomplete {
            message: "Kalshi candlesticks require --recent-hours or --since/--until bounds".into(),
        }),
    }
}

fn merge_side_prices(
    path: &std::path::Path,
    incoming: &[crate::clob::rest::PriceHistoryPoint],
    window_start_secs: i64,
    window_end_secs: i64,
) -> Result<Vec<crate::clob::rest::PriceHistoryPoint>> {
    let existing = crate::prices::load_price_history(path)?;
    Ok(crate::normalize::merge_price_history(
        existing,
        incoming,
        window_start_secs,
        window_end_secs,
    ))
}

pub async fn sync_trades(options: SyncPricesOptions) -> Result<()> {
    let paths = LakePaths::new(&options.out);
    let store = ManifestStore::open(&options.out)?;
    let run_id = new_run_id();
    let started = Utc::now();
    let client = client_from_parts(
        options.kalshi_rest_base_url.clone(),
        options.kalshi_key_id.clone(),
        options.kalshi_private_key_path.clone(),
        options.requests_per_second,
        options.max_retries,
        options.user_agent.clone(),
    )?;
    let market_id = options
        .market_id
        .as_deref()
        .ok_or_else(|| OddsfoxError::SyncIncomplete {
            message: "pass --market <kalshi_ticker> for Kalshi trade sync".into(),
        })?;
    let ticker = normalize::strip_kalshi_market_id(market_id);
    let start_ts = options
        .since
        .map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp());
    let end_ts = options
        .until
        .map(|d| d.and_hms_opt(23, 59, 59).unwrap().and_utc().timestamp());
    let trades = client
        .get_trades(Some(ticker), start_ts, end_ts, options.limit)
        .await?;
    if trades.is_empty() {
        println!("sync kalshi trades: no trades selected");
        return Ok(());
    }
    let batch = normalize::trades_batch(&trades, &run_id)?;
    let rows = batch.num_rows() as i64;
    write_snapshot(&paths, Table::Trades, &run_id, &[batch])?;
    store.append_completed_run("sync trades --source kalshi", &run_id, started, rows)?;
    println!("sync kalshi trades complete: {rows} trades (run={run_id})");
    Ok(())
}

pub async fn snapshot_books(options: SnapshotBooksOptions) -> Result<()> {
    let paths = LakePaths::new(&options.out);
    let store = ManifestStore::open(&options.out)?;
    let run_id = new_run_id();
    let started = Utc::now();
    let client = client_from_parts(
        options.kalshi_rest_base_url.clone(),
        options.kalshi_key_id.clone(),
        options.kalshi_private_key_path.clone(),
        options.requests_per_second,
        options.max_retries,
        options.user_agent.clone(),
    )?;
    let market_id = options
        .market_id
        .as_deref()
        .ok_or_else(|| OddsfoxError::SyncIncomplete {
            message: "pass --market <kalshi_ticker> for Kalshi orderbook snapshots".into(),
        })?;
    let ticker = normalize::strip_kalshi_market_id(market_id);
    let book = client.get_orderbook(ticker, options.depth).await?;
    let records = normalize::snapshot_records_from_orderbook(ticker, &book);
    if records.is_empty() {
        println!("snapshot kalshi books: no levels");
        return Ok(());
    }
    let books_batch = crate::normalize::orderbooks_batch(&records, "kalshi_orderbook", &run_id)?;
    let levels_batch = crate::normalize::book_levels_batch(&records, "kalshi_orderbook", &run_id)?;
    write_snapshot(&paths, Table::Orderbooks, &run_id, &[books_batch])?;
    write_snapshot(&paths, Table::BookLevels, &run_id, &[levels_batch])?;
    store.append_completed_run(
        "snapshot books --source kalshi",
        &run_id,
        started,
        records.len() as i64,
    )?;
    println!(
        "snapshot kalshi books complete: {} snapshots (run={run_id})",
        records.len()
    );
    Ok(())
}

pub async fn historical_cutoff(options: &SyncMarketsOptions) -> Result<Option<i64>> {
    let client = client_from_parts(
        options.kalshi_rest_base_url.clone(),
        options.kalshi_key_id.clone(),
        options.kalshi_private_key_path.clone(),
        options.requests_per_second,
        options.max_retries,
        options.user_agent.clone(),
    )?;
    client.get_historical_cutoff().await
}

fn infer_series(ticker: &str) -> Option<String> {
    ticker
        .split('-')
        .next()
        .map(str::to_string)
        .filter(|s| !s.is_empty())
}
