use std::collections::BTreeMap;
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
use crate::manifest::{ManifestStore, SyncStateRecord};
use crate::normalize::{point_timestamp_secs, prices_batch};
use crate::parquet::write_hourly_price_window;
use crate::paths::LakePaths;

const HOUR_SECS: i64 = 3600;
const CURSOR_VERSION: u32 = 1;
const COLLECT_SOURCE: &str = "collect";
type CursorLock = Arc<tokio::sync::Mutex<()>>;

pub async fn run(options: CollectHourlyOptions) -> Result<()> {
    crate::init::run_quiet(&options.out)?;
    loop {
        let progress = run_once(&options).await?;
        println!(
            "collect hourly: {} windows, {} rows",
            progress.windows_written, progress.rows_written
        );
        if options.once {
            return Ok(());
        }
        tokio::time::sleep(sleep_until_next_horizon(options.lag_minutes)).await;
    }
}

async fn run_once(options: &CollectHourlyOptions) -> Result<CollectProgress> {
    let sources = selected_sources(options.source);
    for source in &sources {
        ensure_seed_cursor(options, *source)?;
        refresh_markets(options, *source).await?;
    }

    let mut total = CollectProgress::default();
    for source in sources {
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
    let tokens = hourly_tokens(&options.out, source)?;
    if tokens.is_empty() {
        return Ok(CollectProgress::default());
    }
    let horizon_ts = floor_to_hour(Utc::now().timestamp() - i64::from(options.lag_minutes) * 60);
    let seed_ts = seed_ts(options, source)?;
    let concurrency = options.concurrency.max(1);

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

    let results = stream::iter(tokens)
        .map(|token| {
            let clob = clob.clone();
            let paths = paths.clone();
            let out = out.clone();
            let cursor_lock = cursor_lock.clone();
            async move {
                collect_token_window(
                    &out,
                    &paths,
                    &cursor_lock,
                    token,
                    horizon_ts,
                    seed_ts,
                    |token, start, end| {
                        let clob = clob.clone();
                        async move {
                            let points = clob
                                .get_prices_history(
                                    &token.token_id,
                                    None,
                                    Some(60),
                                    Some(start),
                                    Some(end),
                                )
                                .await?;
                            Ok(filter_window(points, start, end))
                        }
                    },
                )
                .await
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

    let results = stream::iter(tokens)
        .map(|token| {
            let client = client.clone();
            let paths = paths.clone();
            let out = out.clone();
            let cursor_lock = cursor_lock.clone();
            async move {
                collect_token_window(
                    &out,
                    &paths,
                    &cursor_lock,
                    token,
                    horizon_ts,
                    seed_ts,
                    |token, start, end| {
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
                                .get_candlesticks(&series, &ticker, 60, Some(start), Some(end))
                                .await?;
                            let (yes, no, _) = price_points_from_candlesticks(&candles);
                            let points = if token.token_id.ends_with(":yes") {
                                yes
                            } else {
                                no
                            };
                            Ok(filter_window(points, start, end))
                        }
                    },
                )
                .await
            }
        })
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(sum_progress(results))
}

async fn collect_token_window<F, Fut>(
    out: &std::path::Path,
    paths: &LakePaths,
    cursor_lock: &CursorLock,
    token: HourlyToken,
    horizon_ts: i64,
    seed_ts: i64,
    fetch: F,
) -> Result<CollectProgress>
where
    F: Fn(HourlyToken, i64, i64) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<PriceHistoryPoint>>>,
{
    let mut cursor = load_cursor_locked(out, token.source, &token.token_id, cursor_lock)
        .await?
        .unwrap_or_else(|| HourlyCursor::new(&token, seed_ts));
    if cursor.done {
        return Ok(CollectProgress::default());
    }

    let stop_ts = token.stop_ts.map(ceil_to_hour).unwrap_or(horizon_ts);
    let end_limit = stop_ts.min(horizon_ts);
    let mut progress = CollectProgress::default();

    while cursor.next_start_ts < end_limit {
        let start_ts = cursor.next_start_ts;
        let end_ts = (start_ts + HOUR_SECS).min(end_limit);
        let points = fetch(token.clone(), start_ts, end_ts).await?;
        let rows = points.len() as i64;
        if !points.is_empty() {
            let batch = prices_batch(
                &token.token_id,
                Some(&token.market_id),
                &points,
                token.price_source_label(),
                Some(60),
                "collect-hourly",
            )?;
            write_hourly_price_window(
                paths,
                token.cursor_source(),
                &token.token_id,
                start_ts,
                &[batch],
            )?;
        }

        cursor.next_start_ts = start_ts + HOUR_SECS;
        cursor.done = token.stop_ts.is_some() && cursor.next_start_ts >= stop_ts;
        save_cursor_locked(out, &cursor, rows, cursor_lock).await?;
        progress.windows_written += 1;
        progress.rows_written += rows;
    }

    if cursor.next_start_ts >= end_limit && token.stop_ts.is_some() && !cursor.done {
        cursor.done = true;
        save_cursor_locked(out, &cursor, 0, cursor_lock).await?;
    }
    Ok(progress)
}

fn hourly_tokens(out: &std::path::Path, source: Source) -> Result<Vec<HourlyToken>> {
    let paths = LakePaths::new(out);
    let markets = bronze_source_sql(&paths, Table::Markets);
    let outcomes = bronze_source_sql(&paths, Table::Outcomes);
    let source_filter = match source {
        Source::Polymarket => "gamma",
        Source::Kalshi => "kalshi",
    };
    let sql = format!(
        "SELECT o.token_id, o.market_id, m.source, m.active, m.closed, m.resolved,
                CASE WHEN m.close_time IS NULL THEN NULL ELSE CAST(epoch(m.close_time) AS BIGINT) END,
                CASE WHEN m.resolution_time IS NULL THEN NULL ELSE CAST(epoch(m.resolution_time) AS BIGINT) END,
                json_extract_string(m.raw_json, '$.series_ticker')
         FROM {outcomes} o
         JOIN {markets} m ON o.market_id = m.market_id
         WHERE o.token_id IS NOT NULL AND m.source = ?
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
    if store.sync_state(COLLECT_SOURCE, &key).is_some() {
        return Ok(());
    }
    let since = options.since.ok_or_else(|| {
        OddsfoxError::Config(format!(
            "`collect hourly --source {}` requires --since on first run",
            source_label(source)
        ))
    })?;
    let since_ts = date_start_ts(since);
    store.upsert_sync_state(SyncStateRecord {
        source: COLLECT_SOURCE.into(),
        cursor_key: key,
        cursor_value: since_ts.to_string(),
        last_ts: DateTime::from_timestamp(since_ts, 0),
        updated_at: Utc::now(),
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
    use crate::config::Source;

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
    fn cursor_key_includes_source_and_token() {
        assert_eq!(
            cursor_key(Source::Kalshi, "kalshi:KX:yes"),
            "collect:hourly:kalshi:kalshi:KX:yes"
        );
    }

    #[tokio::test]
    async fn window_write_advances_cursor_one_hour() {
        let dir = tempfile::tempdir().unwrap();
        let paths = LakePaths::new(dir.path());
        paths.scaffold_dirs().unwrap();

        let progress = collect_token_window(
            dir.path(),
            &paths,
            &cursor_lock(),
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

        let progress = collect_token_window(
            dir.path(),
            &paths,
            &cursor_lock(),
            token(),
            HOUR_SECS * 2,
            0,
            |_token, start, _| async move { Ok(vec![PriceHistoryPoint { t: start, p: 0.42 }]) },
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

        let progress = collect_token_window(
            dir.path(),
            &paths,
            &cursor_lock(),
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
    async fn closed_market_cursor_becomes_done() {
        let dir = tempfile::tempdir().unwrap();
        let paths = LakePaths::new(dir.path());
        paths.scaffold_dirs().unwrap();
        let mut token = token();
        token.active = false;
        token.closed = true;
        token.stop_ts = Some(1);

        collect_token_window(
            dir.path(),
            &paths,
            &cursor_lock(),
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
