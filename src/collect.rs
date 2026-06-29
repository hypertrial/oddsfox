use std::collections::BTreeMap;
use std::io::{self, Write};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, Utc};
use futures_util::stream::{self, StreamExt};
use futures_util::TryStreamExt;

use crate::clob::rest::PriceHistoryPoint;
use crate::clob::ClobClient;
use crate::config::{
    BackfillSource, CollectHourlyOptions, KalshiStatus, Source, SyncMarketsOptions, Table,
};
use crate::duckdb_engine::{bronze_source_sql, open_connection};
use crate::error::{OddsfoxError, Result};
use crate::http::HttpClient;
use crate::kalshi::normalize::{price_points_from_candlesticks, strip_kalshi_market_id};
use crate::manifest::{new_run_id, ManifestStore, SyncStateRecord};
use crate::normalize::{point_timestamp_secs, prices_batch};
use crate::parquet::write_hourly_price_window;
use crate::paths::LakePaths;

const HOUR_SECS: i64 = 3600;
// ponytail: 7d chunk balances API payload vs call count; env/flag if limits bite.
const COLLECT_CHUNK_SECS: i64 = 7 * 24 * HOUR_SECS;
const CURSOR_VERSION: u32 = 1;
const COLLECT_SOURCE: &str = "collect";
type CursorLock = Arc<tokio::sync::Mutex<()>>;

pub async fn run(options: CollectHourlyOptions) -> Result<()> {
    crate::init::run_quiet(&options.out)?;
    loop {
        let command = collect_command(&options);
        let run_id = new_run_id();
        let started = Utc::now();
        let store = ManifestStore::open(&options.out)?;
        let run = store.start_run(command, &run_id, started)?;
        let progress = run_once(&options).await?;
        run.complete(progress.rows_written)?;
        log_collect(format!(
            "collect hourly complete: {} windows, {} rows",
            progress.windows_written, progress.rows_written
        ));
        if options.once {
            return Ok(());
        }
        tokio::time::sleep(sleep_until_next_horizon(options.lag_minutes)).await;
    }
}

fn collect_command(options: &CollectHourlyOptions) -> String {
    let mut command = format!(
        "collect hourly --source {}",
        collect_source_label(options.source)
    );
    if options.active {
        command.push_str(" --active");
    }
    if options.once {
        command.push_str(" --once");
    }
    command
}

async fn run_once(options: &CollectHourlyOptions) -> Result<CollectProgress> {
    let sources = selected_sources(options.source);
    for source in &sources {
        ensure_seed_cursor(options, *source)?;
        refresh_markets(options, *source).await?;
    }

    let mut total = CollectProgress::default();
    log_collect("collect hourly: market refresh complete, starting collection");
    for source in sources {
        log_collect(format!(
            "collect hourly: {} starting collection pass",
            source_label(source)
        ));
        let progress = collect_source_once(options, source).await?;
        total.windows_written += progress.windows_written;
        total.rows_written += progress.rows_written;
    }
    Ok(total)
}

async fn refresh_markets(options: &CollectHourlyOptions, source: Source) -> Result<()> {
    let base = SyncMarketsOptions {
        out: options.out.clone(),
        source,
        active: false,
        closed: false,
        all: true,
        status: None,
        series: None,
        event: None,
        tag: None,
        since: None,
        limit: None,
        overwrite: false,
        gamma_base_url: options.gamma_base_url.clone(),
        kalshi_rest_base_url: options.kalshi_rest_base_url.clone(),
        kalshi_key_id: options.kalshi_key_id.clone(),
        kalshi_private_key_path: options.kalshi_private_key_path.clone(),
        requests_per_second: options.requests_per_second,
        max_retries: options.max_retries,
        user_agent: options.user_agent.clone(),
        raw_retention_days: options.raw_retention_days,
    };
    match source {
        Source::Polymarket => {
            crate::sync::sync_markets(base).await?;
        }
        Source::Kalshi => {
            crate::kalshi::sync_markets(SyncMarketsOptions {
                status: Some(KalshiStatus::All),
                ..base
            })
            .await?;
        }
    }
    Ok(())
}

async fn collect_source_once(
    options: &CollectHourlyOptions,
    source: Source,
) -> Result<CollectProgress> {
    log_collect(format!(
        "collect hourly: {} loading tokens from lake",
        source_label(source)
    ));
    let out = options.out.clone();
    let active_only = options.active;
    let tokens = tokio::task::spawn_blocking(move || hourly_tokens(&out, source, active_only))
        .await
        .map_err(|err| OddsfoxError::SyncIncomplete {
            message: format!("hourly token load failed: {err}"),
        })??;
    if tokens.is_empty() {
        log_collect(format!(
            "collect hourly: {} no tokens to collect",
            source_label(source)
        ));
        return Ok(CollectProgress::default());
    }
    let horizon_ts = floor_to_hour(Utc::now().timestamp() - i64::from(options.lag_minutes) * 60);
    let seed_ts = seed_ts(options, source)?;
    let concurrency = options.concurrency.max(1);
    log_collect(format!(
        "collect hourly: {} {} tokens, since {}, horizon {}",
        source_label(source),
        tokens.len(),
        format_ts(seed_ts),
        format_ts(horizon_ts),
    ));

    match source {
        Source::Polymarket => {
            collect_polymarket(options, tokens, horizon_ts, seed_ts, concurrency).await
        }
        Source::Kalshi => collect_kalshi(options, tokens, horizon_ts, seed_ts, concurrency).await,
    }
}

async fn collect_polymarket(
    options: &CollectHourlyOptions,
    tokens: Vec<HourlyToken>,
    horizon_ts: i64,
    seed_ts: i64,
    concurrency: usize,
) -> Result<CollectProgress> {
    let http = HttpClient::new(
        options.requests_per_second,
        options.max_retries,
        options.user_agent.clone(),
    )?;
    let clob = ClobClient::new(options.clob_base_url.clone(), http);
    let paths = LakePaths::new(&options.out);
    let out = options.out.clone();
    let cursor_lock = cursor_lock();
    let total = tokens.len();
    let processed = Arc::new(AtomicUsize::new(0));
    let total_windows = Arc::new(AtomicUsize::new(0));
    let total_rows = Arc::new(AtomicI64::new(0));
    let windows_done = Arc::new(AtomicUsize::new(0));

    let results = stream::iter(tokens)
        .map(|token| {
            let clob = clob.clone();
            let paths = paths.clone();
            let out = out.clone();
            let cursor_lock = cursor_lock.clone();
            let processed = Arc::clone(&processed);
            let total_windows = Arc::clone(&total_windows);
            let total_rows = Arc::clone(&total_rows);
            let windows_done = Arc::clone(&windows_done);
            async move {
                let ctx = CollectContext {
                    out: &out,
                    paths: &paths,
                    cursor_lock: &cursor_lock,
                    windows_done: Some(windows_done),
                };
                let progress = collect_token_range(
                    ctx,
                    token,
                    horizon_ts,
                    seed_ts,
                    |token, chunk_start, chunk_end| {
                        let clob = clob.clone();
                        async move {
                            let points = clob
                                .get_prices_history(
                                    &token.token_id,
                                    Some("max"),
                                    Some(60),
                                    Some(chunk_start),
                                    Some(chunk_end),
                                )
                                .await?;
                            Ok(filter_window(points, chunk_start, chunk_end))
                        }
                    },
                )
                .await?;
                total_windows.fetch_add(progress.windows_written, Ordering::Relaxed);
                total_rows.fetch_add(progress.rows_written, Ordering::Relaxed);
                let done = processed.fetch_add(1, Ordering::Relaxed) + 1;
                if should_report_progress(done, total) {
                    print_collect_progress(
                        Source::Polymarket,
                        done,
                        total,
                        total_windows.load(Ordering::Relaxed),
                        total_rows.load(Ordering::Relaxed),
                    );
                }
                Ok::<_, OddsfoxError>(progress)
            }
        })
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(sum_progress(results))
}

async fn collect_kalshi(
    options: &CollectHourlyOptions,
    tokens: Vec<HourlyToken>,
    horizon_ts: i64,
    seed_ts: i64,
    concurrency: usize,
) -> Result<CollectProgress> {
    let client = crate::kalshi::client_from_parts(
        options.kalshi_rest_base_url.clone(),
        options.kalshi_key_id.clone(),
        options.kalshi_private_key_path.clone(),
        options.requests_per_second,
        options.max_retries,
        options.user_agent.clone(),
    )?;
    let paths = LakePaths::new(&options.out);
    let out = options.out.clone();
    let cursor_lock = cursor_lock();
    let total = tokens.len();
    let processed = Arc::new(AtomicUsize::new(0));
    let total_windows = Arc::new(AtomicUsize::new(0));
    let total_rows = Arc::new(AtomicI64::new(0));
    let windows_done = Arc::new(AtomicUsize::new(0));

    let results = stream::iter(tokens)
        .map(|token| {
            let client = client.clone();
            let paths = paths.clone();
            let out = out.clone();
            let cursor_lock = cursor_lock.clone();
            let processed = Arc::clone(&processed);
            let total_windows = Arc::clone(&total_windows);
            let total_rows = Arc::clone(&total_rows);
            let windows_done = Arc::clone(&windows_done);
            async move {
                let ctx = CollectContext {
                    out: &out,
                    paths: &paths,
                    cursor_lock: &cursor_lock,
                    windows_done: Some(windows_done),
                };
                let progress = collect_token_range(
                    ctx,
                    token,
                    horizon_ts,
                    seed_ts,
                    |token, chunk_start, chunk_end| {
                        let client = client.clone();
                        async move {
                            let series = token.series.clone().ok_or_else(|| {
                                OddsfoxError::SyncIncomplete {
                                    message: format!(
                                        "missing Kalshi series for `{}`",
                                        token.market_id
                                    ),
                                }
                            })?;
                            let ticker = strip_kalshi_market_id(&token.market_id).to_string();
                            let candles = client
                                .get_candlesticks(
                                    &series,
                                    &ticker,
                                    60,
                                    Some(chunk_start),
                                    Some(chunk_end),
                                )
                                .await?;
                            let (yes, no, _) = price_points_from_candlesticks(&candles);
                            let points = if token.token_id.ends_with(":yes") {
                                yes
                            } else {
                                no
                            };
                            Ok(filter_window(points, chunk_start, chunk_end))
                        }
                    },
                )
                .await?;
                total_windows.fetch_add(progress.windows_written, Ordering::Relaxed);
                total_rows.fetch_add(progress.rows_written, Ordering::Relaxed);
                let done = processed.fetch_add(1, Ordering::Relaxed) + 1;
                if should_report_progress(done, total) {
                    print_collect_progress(
                        Source::Kalshi,
                        done,
                        total,
                        total_windows.load(Ordering::Relaxed),
                        total_rows.load(Ordering::Relaxed),
                    );
                }
                Ok::<_, OddsfoxError>(progress)
            }
        })
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(sum_progress(results))
}

struct CollectContext<'a> {
    out: &'a std::path::Path,
    paths: &'a LakePaths,
    cursor_lock: &'a CursorLock,
    windows_done: Option<Arc<AtomicUsize>>,
}

async fn collect_token_range<F, Fut>(
    ctx: CollectContext<'_>,
    token: HourlyToken,
    horizon_ts: i64,
    seed_ts: i64,
    fetch: F,
) -> Result<CollectProgress>
where
    F: Fn(HourlyToken, i64, i64) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<PriceHistoryPoint>>>,
{
    let mut cursor = load_cursor_locked(ctx.out, token.source, &token.token_id, ctx.cursor_lock)
        .await?
        .unwrap_or_else(|| HourlyCursor::new(&token, seed_ts));
    if cursor.done {
        return Ok(CollectProgress::default());
    }

    let stop_ts = token.stop_ts.map(ceil_to_hour).unwrap_or(horizon_ts);
    let end_limit = stop_ts.min(horizon_ts);
    let mut progress = CollectProgress::default();

    while cursor.next_start_ts < end_limit {
        let chunk_start = cursor.next_start_ts;
        let chunk_end = chunk_end_ts(chunk_start, end_limit, COLLECT_CHUNK_SECS);
        let points = fetch(token.clone(), chunk_start, chunk_end).await?;
        let by_hour = group_points_by_hour(points);
        let mut chunk_rows = 0i64;
        let mut hour = chunk_start;
        while hour < chunk_end {
            if let Some(hour_points) = by_hour.get(&hour) {
                let window_points = filter_window(hour_points.clone(), hour, hour + HOUR_SECS);
                let rows = window_points.len() as i64;
                if rows > 0 {
                    let batch = prices_batch(
                        &token.token_id,
                        Some(&token.market_id),
                        &window_points,
                        token.price_source_label(),
                        Some(60),
                        "collect-hourly",
                    )?;
                    write_hourly_price_window(
                        ctx.paths,
                        token.cursor_source(),
                        &token.token_id,
                        hour,
                        &[batch],
                    )?;
                    chunk_rows += rows;
                    progress.rows_written += rows;
                }
            }
            progress.windows_written += 1;
            if let Some(windows_done) = &ctx.windows_done {
                let total = windows_done.fetch_add(1, Ordering::Relaxed) + 1;
                if total == 1 || total.is_multiple_of(100) {
                    log_collect_progress(format!(
                        "collect hourly progress ({}): {} windows processed",
                        source_label(token.source),
                        total
                    ));
                }
            }
            hour += HOUR_SECS;
        }

        cursor.next_start_ts = chunk_end;
        cursor.done = token.stop_ts.is_some() && cursor.next_start_ts >= stop_ts;
        save_cursor_locked(ctx.out, &cursor, chunk_rows, ctx.cursor_lock).await?;
    }

    if cursor.next_start_ts >= end_limit && token.stop_ts.is_some() && !cursor.done {
        cursor.done = true;
        save_cursor_locked(ctx.out, &cursor, 0, ctx.cursor_lock).await?;
    }
    Ok(progress)
}

fn chunk_end_ts(start: i64, end_limit: i64, max_chunk_secs: i64) -> i64 {
    (start + max_chunk_secs).min(end_limit)
}

fn group_points_by_hour(points: Vec<PriceHistoryPoint>) -> BTreeMap<i64, Vec<PriceHistoryPoint>> {
    let mut by_hour: BTreeMap<i64, Vec<PriceHistoryPoint>> = BTreeMap::new();
    for point in points {
        let hour = floor_to_hour(point_timestamp_secs(&point));
        by_hour.entry(hour).or_default().push(point);
    }
    by_hour
}

fn hourly_tokens(
    out: &std::path::Path,
    source: Source,
    active_only: bool,
) -> Result<Vec<HourlyToken>> {
    let paths = LakePaths::new(out);
    let markets = bronze_source_sql(&paths, Table::Markets);
    let outcomes = bronze_source_sql(&paths, Table::Outcomes);
    let source_filter = match source {
        Source::Polymarket => "gamma",
        Source::Kalshi => "kalshi",
    };
    let active_filter = if active_only {
        " AND m.active = true"
    } else {
        ""
    };
    let sql = format!(
        "SELECT o.token_id, o.market_id, m.source, m.active, m.closed, m.resolved,
                CASE WHEN m.close_time IS NULL THEN NULL ELSE CAST(epoch(m.close_time) AS BIGINT) END,
                CASE WHEN m.resolution_time IS NULL THEN NULL ELSE CAST(epoch(m.resolution_time) AS BIGINT) END,
                json_extract_string(m.raw_json, '$.series_ticker')
         FROM {outcomes} o
         JOIN {markets} m ON o.market_id = m.market_id
         WHERE o.token_id IS NOT NULL AND m.source = ?{active_filter}
         ORDER BY o.token_id"
    );
    let conn = open_connection(None)?;
    let mut stmt = crate::duckdb_engine::map_duckdb(conn.prepare(&sql))?;
    let rows = crate::duckdb_engine::map_duckdb(stmt.query_map([source_filter], |row| {
        let close_ts = row.get::<_, Option<i64>>(6)?;
        let resolution_ts = row.get::<_, Option<i64>>(7)?;
        Ok(HourlyToken {
            source,
            token_id: row.get(0)?,
            market_id: row.get(1)?,
            active: row.get::<_, Option<bool>>(3)?.unwrap_or(false),
            closed: row.get::<_, Option<bool>>(4)?.unwrap_or(false),
            resolved: row.get::<_, Option<bool>>(5)?.unwrap_or(false),
            stop_ts: resolution_ts.or(close_ts),
            series: row.get(8)?,
        })
    }))?;
    let mut tokens = BTreeMap::<String, HourlyToken>::new();
    for row in rows {
        let token = row?;
        tokens
            .entry(token.token_id.clone())
            .and_modify(|existing| existing.merge(&token))
            .or_insert(token);
    }
    Ok(tokens.into_values().collect())
}

fn ensure_seed_cursor(options: &CollectHourlyOptions, source: Source) -> Result<()> {
    let store = ManifestStore::open(&options.out)?;
    let key = seed_cursor_key(source);
    let since_ts = match options.since {
        Some(date) => date_start_ts(date),
        None => {
            if store.sync_state(COLLECT_SOURCE, &key).is_some() {
                return Ok(());
            }
            return Err(OddsfoxError::Config(format!(
                "`collect hourly --source {}` requires --since on first run",
                source_label(source)
            )));
        }
    };

    if let Some(record) = store.sync_state(COLLECT_SOURCE, &key) {
        let stored = record.cursor_value.parse::<i64>().unwrap_or(since_ts);
        if stored == since_ts {
            return Ok(());
        }
        let removed = clear_collect_token_cursors(&store, source)?;
        log_collect(format!(
            "collect hourly: {} seed {} -> {} (cleared {removed} token cursors)",
            source_label(source),
            format_ts(stored),
            format_ts(since_ts),
        ));
    }

    store.upsert_sync_state(SyncStateRecord {
        source: COLLECT_SOURCE.into(),
        cursor_key: key,
        cursor_value: since_ts.to_string(),
        last_ts: DateTime::from_timestamp(since_ts, 0),
        updated_at: Utc::now(),
    })
}

fn clear_collect_token_cursors(store: &ManifestStore, source: Source) -> Result<usize> {
    let prefix = format!("collect:hourly:{}:", source_label(source));
    let config_key = seed_cursor_key(source);
    store.remove_sync_states_where(|record| {
        record.source == COLLECT_SOURCE
            && record.cursor_key.starts_with(&prefix)
            && record.cursor_key != config_key
    })
}

fn seed_ts(options: &CollectHourlyOptions, source: Source) -> Result<i64> {
    let store = ManifestStore::open(&options.out)?;
    let key = seed_cursor_key(source);
    store
        .sync_state(COLLECT_SOURCE, &key)
        .and_then(|record| record.cursor_value.parse().ok())
        .or_else(|| options.since.map(date_start_ts))
        .ok_or_else(|| {
            OddsfoxError::Config(format!(
                "`collect hourly --source {}` requires --since on first run",
                source_label(source)
            ))
        })
}

fn load_cursor(
    out: &std::path::Path,
    source: Source,
    token_id: &str,
) -> Result<Option<HourlyCursor>> {
    let store = ManifestStore::open(out)?;
    Ok(store
        .sync_state(COLLECT_SOURCE, &cursor_key(source, token_id))
        .and_then(|record| serde_json::from_str(&record.cursor_value).ok()))
}

async fn load_cursor_locked(
    out: &std::path::Path,
    source: Source,
    token_id: &str,
    cursor_lock: &CursorLock,
) -> Result<Option<HourlyCursor>> {
    let _guard = cursor_lock.lock().await;
    load_cursor(out, source, token_id)
}

fn save_cursor(out: &std::path::Path, cursor: &HourlyCursor, rows: i64) -> Result<()> {
    let store = ManifestStore::open(out)?;
    let mut cursor = cursor.clone();
    cursor.last_window_rows = rows;
    cursor.updated_at = Utc::now();
    store.upsert_sync_state(cursor.sync_state())
}

async fn save_cursor_locked(
    out: &std::path::Path,
    cursor: &HourlyCursor,
    rows: i64,
    cursor_lock: &CursorLock,
) -> Result<()> {
    let _guard = cursor_lock.lock().await;
    save_cursor(out, cursor, rows)
}

fn filter_window(
    points: Vec<PriceHistoryPoint>,
    start_ts: i64,
    end_ts: i64,
) -> Vec<PriceHistoryPoint> {
    points
        .into_iter()
        .filter(|point| {
            let ts = point_timestamp_secs(point);
            ts >= start_ts && ts < end_ts
        })
        .collect()
}

fn selected_sources(source: BackfillSource) -> Vec<Source> {
    match source {
        BackfillSource::Polymarket => vec![Source::Polymarket],
        BackfillSource::Kalshi => vec![Source::Kalshi],
        BackfillSource::All => vec![Source::Polymarket, Source::Kalshi],
    }
}

fn collect_source_label(source: BackfillSource) -> &'static str {
    match source {
        BackfillSource::Polymarket => "polymarket",
        BackfillSource::Kalshi => "kalshi",
        BackfillSource::All => "all",
    }
}

fn sum_progress(results: Vec<CollectProgress>) -> CollectProgress {
    results
        .into_iter()
        .fold(CollectProgress::default(), |mut acc, item| {
            acc.windows_written += item.windows_written;
            acc.rows_written += item.rows_written;
            acc
        })
}

fn floor_to_hour(ts: i64) -> i64 {
    ts - ts.rem_euclid(HOUR_SECS)
}

fn ceil_to_hour(ts: i64) -> i64 {
    if ts == floor_to_hour(ts) {
        ts
    } else {
        floor_to_hour(ts) + HOUR_SECS
    }
}

fn date_start_ts(date: NaiveDate) -> i64 {
    date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp()
}

fn sleep_until_next_horizon(lag_minutes: u32) -> Duration {
    let now = Utc::now().timestamp();
    let lag = i64::from(lag_minutes) * 60;
    let current_horizon = floor_to_hour(now - lag);
    let wake = current_horizon + HOUR_SECS + lag;
    Duration::from_secs((wake - now).max(60) as u64)
}

fn cursor_lock() -> CursorLock {
    Arc::new(tokio::sync::Mutex::new(()))
}

fn cursor_key(source: Source, token_id: &str) -> String {
    format!("collect:hourly:{}:{token_id}", source_label(source))
}

fn seed_cursor_key(source: Source) -> String {
    format!("collect:hourly:{}:config", source_label(source))
}

fn source_label(source: Source) -> &'static str {
    match source {
        Source::Polymarket => "polymarket",
        Source::Kalshi => "kalshi",
    }
}

fn format_ts(ts: i64) -> String {
    DateTime::from_timestamp(ts, 0)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| ts.to_string())
}

fn print_collect_progress(source: Source, done: usize, total: usize, windows: usize, rows: i64) {
    log_collect_progress(format!(
        "collect hourly progress ({}): {done}/{total} tokens, {windows} windows, {rows} rows",
        source_label(source)
    ));
}

fn log_collect_progress(message: impl AsRef<str>) {
    crate::progress_log::log_progress(message);
}

fn log_collect(message: impl AsRef<str>) {
    println!("{}", message.as_ref());
    let _ = io::stdout().flush();
}

fn should_report_progress(done: usize, total: usize) -> bool {
    done == 1 || done == total || done.is_multiple_of(25)
}

#[derive(Clone, Debug)]
struct HourlyToken {
    source: Source,
    token_id: String,
    market_id: String,
    active: bool,
    closed: bool,
    resolved: bool,
    stop_ts: Option<i64>,
    series: Option<String>,
}

impl HourlyToken {
    fn merge(&mut self, other: &Self) {
        self.active |= other.active;
        self.closed |= other.closed;
        self.resolved |= other.resolved;
        self.stop_ts = self.stop_ts.or(other.stop_ts);
        self.series = self.series.clone().or_else(|| other.series.clone());
    }

    fn cursor_source(&self) -> &'static str {
        source_label(self.source)
    }

    fn price_source_label(&self) -> &'static str {
        match self.source {
            Source::Polymarket => "clob_prices_history",
            Source::Kalshi => "kalshi_candlesticks",
        }
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct HourlyCursor {
    version: u32,
    source: String,
    token_id: String,
    market_id: String,
    next_start_ts: i64,
    done: bool,
    last_window_rows: i64,
    updated_at: DateTime<Utc>,
}

impl HourlyCursor {
    fn new(token: &HourlyToken, next_start_ts: i64) -> Self {
        Self {
            version: CURSOR_VERSION,
            source: source_label(token.source).into(),
            token_id: token.token_id.clone(),
            market_id: token.market_id.clone(),
            next_start_ts,
            done: false,
            last_window_rows: 0,
            updated_at: Utc::now(),
        }
    }

    fn sync_state(&self) -> SyncStateRecord {
        SyncStateRecord {
            source: COLLECT_SOURCE.into(),
            cursor_key: cursor_key(
                if self.source == "kalshi" {
                    Source::Kalshi
                } else {
                    Source::Polymarket
                },
                &self.token_id,
            ),
            cursor_value: serde_json::to_string(self).unwrap(),
            last_ts: DateTime::from_timestamp(self.next_start_ts, 0),
            updated_at: Utc::now(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct CollectProgress {
    windows_written: usize,
    rows_written: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Source, Table};
    use crate::gamma::GammaMarket;
    use crate::normalize::{markets_batch, outcomes_batch};
    use crate::parquet::write_snapshot;
    use std::sync::atomic::AtomicUsize;

    fn token() -> HourlyToken {
        HourlyToken {
            source: Source::Polymarket,
            token_id: "tok".into(),
            market_id: "m1".into(),
            active: true,
            closed: false,
            resolved: false,
            stop_ts: None,
            series: None,
        }
    }

    fn collect_ctx<'a>(
        dir: &'a std::path::Path,
        paths: &'a LakePaths,
        lock: &'a CursorLock,
    ) -> CollectContext<'a> {
        CollectContext {
            out: dir,
            paths,
            cursor_lock: lock,
            windows_done: None,
        }
    }

    fn points_per_hour(start: i64, end: i64) -> Vec<PriceHistoryPoint> {
        let mut points = Vec::new();
        let mut hour = start;
        while hour < end {
            points.push(PriceHistoryPoint { t: hour, p: 0.42 });
            hour += HOUR_SECS;
        }
        points
    }

    #[test]
    fn cursor_initializes_from_since() {
        let cursor = HourlyCursor::new(&token(), 1_704_067_200);
        assert_eq!(cursor.next_start_ts, 1_704_067_200);
        assert!(!cursor.done);
    }

    #[test]
    fn hour_rounding_is_utc_boundary_based() {
        assert_eq!(floor_to_hour(3_599), 0);
        assert_eq!(ceil_to_hour(3_601), 7_200);
        assert_eq!(ceil_to_hour(3_600), 3_600);
    }

    #[test]
    fn filter_window_keeps_half_open_hour() {
        let points = vec![
            PriceHistoryPoint { t: 100, p: 0.1 },
            PriceHistoryPoint { t: 200, p: 0.2 },
            PriceHistoryPoint { t: 300, p: 0.3 },
        ];
        let got = filter_window(points, 100, 300);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].t, 100);
        assert_eq!(got[1].t, 200);
    }

    #[test]
    fn progress_reports_first_last_and_every_25() {
        assert!(should_report_progress(1, 100));
        assert!(should_report_progress(25, 100));
        assert!(should_report_progress(100, 100));
        assert!(!should_report_progress(2, 100));
        assert!(!should_report_progress(24, 100));
    }

    #[test]
    fn cursor_key_includes_source_and_token() {
        assert_eq!(
            cursor_key(Source::Kalshi, "kalshi:KX:yes"),
            "collect:hourly:kalshi:kalshi:KX:yes"
        );
    }

    #[test]
    fn group_points_by_hour_assigns_utc_buckets() {
        let points = vec![
            PriceHistoryPoint { t: 100, p: 0.1 },
            PriceHistoryPoint { t: 3_700, p: 0.2 },
            PriceHistoryPoint { t: 7_200, p: 0.3 },
        ];
        let by_hour = group_points_by_hour(points);
        assert_eq!(by_hour.len(), 3);
        assert_eq!(by_hour.get(&0).unwrap().len(), 1);
        assert_eq!(by_hour.get(&3_600).unwrap().len(), 1);
        assert_eq!(by_hour.get(&7_200).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn window_write_advances_cursor_one_hour() {
        let dir = tempfile::tempdir().unwrap();
        let paths = LakePaths::new(dir.path());
        paths.scaffold_dirs().unwrap();
        let lock = cursor_lock();

        let progress = collect_token_range(
            collect_ctx(dir.path(), &paths, &lock),
            token(),
            HOUR_SECS,
            0,
            |_token, _start, _end| async { Ok(vec![PriceHistoryPoint { t: 0, p: 0.42 }]) },
        )
        .await
        .unwrap();

        let cursor = load_cursor(dir.path(), Source::Polymarket, "tok")
            .unwrap()
            .unwrap();
        assert_eq!(cursor.next_start_ts, HOUR_SECS);
        assert_eq!(progress.windows_written, 1);
        assert_eq!(progress.rows_written, 1);
        assert!(paths
            .hourly_price_window_file("polymarket", "tok", 0)
            .exists());
    }

    #[tokio::test]
    async fn token_catches_up_until_horizon() {
        let dir = tempfile::tempdir().unwrap();
        let paths = LakePaths::new(dir.path());
        paths.scaffold_dirs().unwrap();
        let lock = cursor_lock();

        let progress = collect_token_range(
            collect_ctx(dir.path(), &paths, &lock),
            token(),
            HOUR_SECS * 2,
            0,
            |_token, start, end| async move { Ok(points_per_hour(start, end)) },
        )
        .await
        .unwrap();

        let cursor = load_cursor(dir.path(), Source::Polymarket, "tok")
            .unwrap()
            .unwrap();
        assert_eq!(cursor.next_start_ts, HOUR_SECS * 2);
        assert_eq!(progress.windows_written, 2);
        assert_eq!(progress.rows_written, 2);
    }

    #[tokio::test]
    async fn empty_window_still_advances_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let paths = LakePaths::new(dir.path());
        paths.scaffold_dirs().unwrap();
        let lock = cursor_lock();

        let progress = collect_token_range(
            collect_ctx(dir.path(), &paths, &lock),
            token(),
            HOUR_SECS,
            0,
            |_token, _, _| async { Ok(Vec::new()) },
        )
        .await
        .unwrap();

        let cursor = load_cursor(dir.path(), Source::Polymarket, "tok")
            .unwrap()
            .unwrap();
        assert_eq!(cursor.next_start_ts, HOUR_SECS);
        assert_eq!(progress.rows_written, 0);
        assert!(!paths
            .hourly_price_window_file("polymarket", "tok", 0)
            .exists());
    }

    #[tokio::test]
    async fn write_hourly_windows_advances_empty_hours() {
        let dir = tempfile::tempdir().unwrap();
        let paths = LakePaths::new(dir.path());
        paths.scaffold_dirs().unwrap();
        let lock = cursor_lock();

        let progress = collect_token_range(
            collect_ctx(dir.path(), &paths, &lock),
            token(),
            HOUR_SECS * 3,
            0,
            |_token, start, _end| async move { Ok(vec![PriceHistoryPoint { t: start, p: 0.42 }]) },
        )
        .await
        .unwrap();

        let cursor = load_cursor(dir.path(), Source::Polymarket, "tok")
            .unwrap()
            .unwrap();
        assert_eq!(cursor.next_start_ts, HOUR_SECS * 3);
        assert_eq!(progress.windows_written, 3);
        assert_eq!(progress.rows_written, 1);
        assert!(paths
            .hourly_price_window_file("polymarket", "tok", 0)
            .exists());
        assert!(!paths
            .hourly_price_window_file("polymarket", "tok", HOUR_SECS)
            .exists());
    }

    #[tokio::test]
    async fn collect_token_range_one_fetch_writes_multiple_windows() {
        let dir = tempfile::tempdir().unwrap();
        let paths = LakePaths::new(dir.path());
        paths.scaffold_dirs().unwrap();
        let lock = cursor_lock();
        let fetches = Arc::new(AtomicUsize::new(0));

        let progress = collect_token_range(
            collect_ctx(dir.path(), &paths, &lock),
            token(),
            HOUR_SECS * 3,
            0,
            {
                let fetches = Arc::clone(&fetches);
                move |_token, start, end| {
                    let fetches = Arc::clone(&fetches);
                    async move {
                        fetches.fetch_add(1, Ordering::Relaxed);
                        Ok(points_per_hour(start, end))
                    }
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(fetches.load(Ordering::Relaxed), 1);
        assert_eq!(progress.windows_written, 3);
        assert_eq!(progress.rows_written, 3);
        let cursor = load_cursor(dir.path(), Source::Polymarket, "tok")
            .unwrap()
            .unwrap();
        assert_eq!(cursor.next_start_ts, HOUR_SECS * 3);
    }

    #[tokio::test]
    async fn closed_market_cursor_becomes_done() {
        let dir = tempfile::tempdir().unwrap();
        let paths = LakePaths::new(dir.path());
        paths.scaffold_dirs().unwrap();
        let lock = cursor_lock();
        let mut token = token();
        token.active = false;
        token.closed = true;
        token.stop_ts = Some(1);

        collect_token_range(
            collect_ctx(dir.path(), &paths, &lock),
            token,
            HOUR_SECS,
            0,
            |_token, _, _| async { Ok(Vec::new()) },
        )
        .await
        .unwrap();

        let cursor = load_cursor(dir.path(), Source::Polymarket, "tok")
            .unwrap()
            .unwrap();
        assert!(cursor.done);
    }

    fn test_market(
        id: &str,
        active: bool,
        closed: bool,
        resolved: bool,
        yes_token: &str,
        no_token: &str,
    ) -> GammaMarket {
        GammaMarket {
            id: id.into(),
            event_id: Some(format!("e-{id}")),
            conditionId: None,
            questionID: None,
            slug: None,
            question: Some(format!("{id}?")),
            description: None,
            active: Some(active),
            closed: Some(closed),
            resolved: Some(resolved),
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
            clobTokenIds: Some(format!("[\"{yes_token}\",\"{no_token}\"]")),
            winningOutcome: None,
            winningOutcomeIndex: None,
        }
    }

    #[test]
    fn hourly_tokens_active_filter() {
        let dir = tempfile::tempdir().unwrap();
        let paths = LakePaths::new(dir.path());
        paths.scaffold_dirs().unwrap();
        let active_market = test_market(
            "m-active",
            true,
            false,
            false,
            "tok-active-yes",
            "tok-active-no",
        );
        let closed_market = test_market(
            "m-closed",
            false,
            true,
            true,
            "tok-closed-yes",
            "tok-closed-no",
        );
        write_snapshot(
            &paths,
            Table::Markets,
            "run-1",
            &[markets_batch(
                &[active_market.clone(), closed_market.clone()],
                "gamma",
                "http://test",
                "sha",
                "run-1",
            )
            .unwrap()],
        )
        .unwrap();
        write_snapshot(
            &paths,
            Table::Outcomes,
            "run-1",
            &[outcomes_batch(
                &[active_market, closed_market],
                "gamma",
                "http://test",
                "sha",
                "run-1",
            )
            .unwrap()],
        )
        .unwrap();
        crate::manifest::ManifestStore::open(dir.path())
            .unwrap()
            .append_completed_run("test", "run-1", chrono::Utc::now(), 2)
            .unwrap();

        let all = hourly_tokens(dir.path(), Source::Polymarket, false).unwrap();
        assert_eq!(all.len(), 4);
        let active = hourly_tokens(dir.path(), Source::Polymarket, true).unwrap();
        assert_eq!(active.len(), 2);
        assert!(active.iter().all(|t| t.token_id.starts_with("tok-active")));
    }

    #[test]
    fn deterministic_hourly_file_replaces_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let paths = LakePaths::new(dir.path());
        paths.scaffold_dirs().unwrap();
        let first = prices_batch(
            "tok",
            Some("m1"),
            &[PriceHistoryPoint { t: 0, p: 0.1 }],
            "test",
            Some(60),
            "run-1",
        )
        .unwrap();
        let second = prices_batch(
            "tok",
            Some("m1"),
            &[PriceHistoryPoint { t: 0, p: 0.2 }],
            "test",
            Some(60),
            "run-2",
        )
        .unwrap();

        write_hourly_price_window(&paths, "polymarket", "tok", 0, &[first]).unwrap();
        let path = write_hourly_price_window(&paths, "polymarket", "tok", 0, &[second]).unwrap();

        assert_eq!(crate::parquet::parquet_row_count(&path).unwrap(), 1);
    }
}
