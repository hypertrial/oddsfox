use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::array::{Array, Float64Array, TimestampMillisecondArray};
use chrono::Utc;
use futures_util::stream::{self, StreamExt};
use futures_util::TryStreamExt;

use crate::clob::rest::PriceHistoryPoint;
use crate::clob::ClobClient;
use crate::config::{resolve_price_time_range, SyncPricesOptions, Table, TokenPairFilter};
use crate::error::{OddsfoxError, Result};
use crate::http::HttpClient;
use crate::manifest::{new_run_id, ManifestStore, SyncStateRecord};
use crate::normalize::{merge_price_history, point_timestamp_secs, prices_batch};
use crate::parquet::{read_all_batches, write_token_series};
use crate::paths::LakePaths;
use crate::progress_log::log_progress;
use crate::sync::{all_token_pairs, token_ids_for_market, top_token_pairs};

pub async fn sync_prices(options: SyncPricesOptions) -> Result<()> {
    let paths = LakePaths::new(&options.out);
    let store = ManifestStore::open(&options.out)?;
    let run_id = new_run_id();
    let started = Utc::now();
    let run = store.start_run("sync prices", &run_id, started)?;
    let http = HttpClient::new(
        options.requests_per_second,
        options.max_retries,
        options.user_agent.clone(),
    )?;
    let clob = ClobClient::new(options.clob_base_url.clone(), http);

    let pairs = select_token_pairs(&options).await?;
    if pairs.is_empty() {
        return Err(OddsfoxError::SyncIncomplete {
            message:
                "no tokens selected; run `sync markets` first or pass --market, --active, or --all"
                    .into(),
        });
    }

    let time_range = resolve_price_time_range(options.since, options.until, options.recent_hours);
    let interval = effective_interval(&options);
    let checkpoint = PriceCheckpoint::new(
        "polymarket",
        time_range.start_ts,
        time_range.end_ts,
        interval,
        options.fidelity,
    );
    let checkpoints = price_checkpoints(&store, "polymarket", &pairs);
    let fidelity_minutes = options.fidelity.map(|f| f as i32);
    let fidelity = options.fidelity;
    let overwrite = options.overwrite;
    let merge_window = options.recent_hours.is_some();
    let total = pairs.len();
    let processed = Arc::new(AtomicUsize::new(0));
    let skipped = Arc::new(AtomicUsize::new(0));
    let total_points = Arc::new(AtomicI64::new(0));
    let concurrency = options.concurrency.max(1);

    let states = stream::iter(pairs)
        .map(|(token_id, market_id)| {
            let clob = clob.clone();
            let paths = paths.clone();
            let run_id = run_id.clone();
            let interval = interval.map(str::to_string);
            let processed = Arc::clone(&processed);
            let skipped = Arc::clone(&skipped);
            let total_points = Arc::clone(&total_points);
            let start_ts = time_range.start_ts;
            let end_ts = time_range.end_ts;
            let checkpoint = checkpoint.clone();
            let stored_checkpoint = checkpoints.get(&token_id).cloned();
            async move {
                let output_path = paths.token_partition_file(Table::Prices, &token_id);
                if price_token_up_to_date(
                    &output_path,
                    overwrite,
                    merge_window,
                    &checkpoint,
                    stored_checkpoint.as_ref(),
                ) {
                    skipped.fetch_add(1, Ordering::Relaxed);
                    let done = processed.fetch_add(1, Ordering::Relaxed) + 1;
                    if done == total || done.is_multiple_of(25) {
                        print_progress(
                            done,
                            total,
                            total_points.load(Ordering::Relaxed),
                            skipped.load(Ordering::Relaxed),
                        );
                    }
                    return Ok::<_, OddsfoxError>(Some(checkpoint.sync_state(&token_id)?));
                }

                let mut history = clob
                    .get_prices_history(&token_id, interval.as_deref(), fidelity, start_ts, end_ts)
                    .await?;
                if merge_window {
                    let (window_start, window_end) = time_range
                        .start_ts
                        .zip(time_range.end_ts)
                        .ok_or_else(|| OddsfoxError::SyncIncomplete {
                            message: "recent-hours sync requires a resolved time window".into(),
                        })?;
                    let existing = load_price_history(&output_path)?;
                    history = merge_price_history(existing, &history, window_start, window_end);
                }
                if history.is_empty() {
                    let done = processed.fetch_add(1, Ordering::Relaxed) + 1;
                    if done == total || done.is_multiple_of(25) {
                        print_progress(
                            done,
                            total,
                            total_points.load(Ordering::Relaxed),
                            skipped.load(Ordering::Relaxed),
                        );
                    }
                    return Ok(None);
                }

                let batch = prices_batch(
                    &token_id,
                    Some(&market_id),
                    &history,
                    "clob_prices_history",
                    fidelity_minutes,
                    &run_id,
                )?;
                let points = batch.num_rows() as i64;
                write_token_series(&paths, Table::Prices, &token_id, &[batch])?;
                total_points.fetch_add(points, Ordering::Relaxed);
                let state = checkpoint.sync_state(&token_id)?;
                let done = processed.fetch_add(1, Ordering::Relaxed) + 1;
                if done == total || done.is_multiple_of(25) {
                    print_progress(
                        done,
                        total,
                        total_points.load(Ordering::Relaxed),
                        skipped.load(Ordering::Relaxed),
                    );
                }
                Ok(Some(state))
            }
        })
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;

    for state in states.into_iter().flatten() {
        store.upsert_sync_state(state)?;
    }

    let points_written = total_points.load(Ordering::Relaxed);
    let skipped_tokens = skipped.load(Ordering::Relaxed);
    run.complete(points_written)?;
    log_progress(format!(
        "sync prices complete: {points_written} points written, {skipped_tokens} skipped, {total} tokens (run={run_id})"
    ));
    Ok(())
}

fn effective_interval(options: &SyncPricesOptions) -> Option<&str> {
    if options.since.is_some() || options.until.is_some() || options.recent_hours.is_some() {
        None
    } else {
        options.interval.as_deref()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct PriceCheckpoint {
    provider: String,
    start_ts: Option<i64>,
    end_ts: Option<i64>,
    interval: Option<String>,
    fidelity: Option<u32>,
}

impl PriceCheckpoint {
    pub fn new(
        provider: &str,
        start_ts: Option<i64>,
        end_ts: Option<i64>,
        interval: Option<&str>,
        fidelity: Option<u32>,
    ) -> Self {
        Self {
            provider: provider.into(),
            start_ts,
            end_ts,
            interval: interval.map(str::to_string),
            fidelity,
        }
    }

    pub fn sync_state(&self, token_id: &str) -> Result<SyncStateRecord> {
        Ok(SyncStateRecord {
            source: self.provider.clone(),
            cursor_key: price_cursor_key(&self.provider, token_id),
            cursor_value: serde_json::to_string(self)?,
            last_ts: self
                .end_ts
                .and_then(|ts| chrono::DateTime::from_timestamp(ts, 0)),
            updated_at: Utc::now(),
        })
    }
}

pub fn price_cursor_key(source: &str, token_id: &str) -> String {
    format!("prices:{source}:{token_id}")
}

pub fn parse_price_checkpoint(record: &SyncStateRecord) -> Option<PriceCheckpoint> {
    serde_json::from_str(&record.cursor_value).ok()
}

fn price_checkpoints(
    store: &ManifestStore,
    source: &str,
    pairs: &[(String, String)],
) -> HashMap<String, PriceCheckpoint> {
    pairs
        .iter()
        .filter_map(|(token_id, _)| {
            store
                .sync_state(source, &price_cursor_key(source, token_id))
                .and_then(|record| parse_price_checkpoint(&record))
                .map(|checkpoint| (token_id.clone(), checkpoint))
        })
        .collect()
}

async fn select_token_pairs(options: &SyncPricesOptions) -> Result<Vec<(String, String)>> {
    if let Some(market_id) = options.market_id.as_deref() {
        let token_ids = token_ids_for_market(&options.out, market_id).await?;
        Ok(token_ids
            .into_iter()
            .map(|token_id| (token_id, market_id.to_string()))
            .collect())
    } else if options.all {
        all_token_pairs(
            &options.out,
            &TokenPairFilter {
                active: options.filter_active,
                tag: options.tag.clone(),
                limit: options.limit,
            },
        )
        .await
    } else if options.active {
        if let Some(top_limit) = options.top_limit {
            top_token_pairs(&options.out, top_limit).await
        } else {
            all_token_pairs(
                &options.out,
                &TokenPairFilter {
                    active: Some(true),
                    tag: options.tag.clone(),
                    limit: options.limit,
                },
            )
            .await
        }
    } else {
        Err(OddsfoxError::SyncIncomplete {
            message: "pass --market, --active, or --all to select tokens for price sync".into(),
        })
    }
}

pub fn load_price_history(path: &Path) -> Result<Vec<PriceHistoryPoint>> {
    let batches = read_all_batches(path)?;
    let mut points = Vec::new();
    for batch in batches {
        let ts_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| OddsfoxError::Parse {
                table: "prices".into(),
                message: "missing ts column".into(),
            })?;
        let price_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| OddsfoxError::Parse {
                table: "prices".into(),
                message: "missing price column".into(),
            })?;
        for row in 0..batch.num_rows() {
            if ts_col.is_null(row) || price_col.is_null(row) {
                continue;
            }
            points.push(PriceHistoryPoint {
                t: point_timestamp_secs(&PriceHistoryPoint {
                    t: ts_col.value(row),
                    p: 0.0,
                }),
                p: price_col.value(row),
            });
        }
    }
    Ok(points)
}

fn price_token_up_to_date(
    output_path: &Path,
    overwrite: bool,
    merge_window: bool,
    checkpoint: &PriceCheckpoint,
    stored_checkpoint: Option<&PriceCheckpoint>,
) -> bool {
    if overwrite || merge_window || !output_path.exists() {
        return false;
    }
    match stored_checkpoint {
        Some(stored) if stored == checkpoint => true,
        Some(_) => false,
        // ponytail: backfill can finish writing parquet before sync_state is flushed; use --overwrite to refetch
        None => true,
    }
}

fn print_progress(done: usize, total: usize, points: i64, skipped: usize) {
    if skipped > 0 {
        log_progress(format!(
            "sync prices progress: {done}/{total} tokens, {points} points written, {skipped} skipped"
        ));
    } else {
        log_progress(format!(
            "sync prices progress: {done}/{total} tokens, {points} points written"
        ));
    }
}

#[cfg(test)]
mod checkpoint_tests {
    use super::*;

    #[test]
    fn price_checkpoint_roundtrips_through_sync_state() {
        let checkpoint =
            PriceCheckpoint::new("polymarket", Some(10), Some(20), Some("max"), Some(60));
        let state = checkpoint.sync_state("tok-1").unwrap();

        assert_eq!(state.source, "polymarket");
        assert_eq!(state.cursor_key, "prices:polymarket:tok-1");
        assert_eq!(parse_price_checkpoint(&state), Some(checkpoint));
    }

    #[test]
    fn price_token_up_to_date_matches_stored_checkpoint() {
        let checkpoint = PriceCheckpoint::new("polymarket", Some(1), None, None, Some(1440));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part.parquet");
        std::fs::write(&path, b"x").unwrap();
        assert!(price_token_up_to_date(
            &path,
            false,
            false,
            &checkpoint,
            Some(&checkpoint),
        ));
    }

    #[test]
    fn price_token_up_to_date_skips_existing_file_without_stored_checkpoint() {
        let checkpoint = PriceCheckpoint::new("polymarket", Some(1), None, None, Some(1440));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part.parquet");
        std::fs::write(&path, b"x").unwrap();
        assert!(price_token_up_to_date(&path, false, false, &checkpoint, None));
    }

    #[test]
    fn price_token_up_to_date_refetches_when_stored_checkpoint_differs() {
        let checkpoint = PriceCheckpoint::new("polymarket", Some(1), None, None, Some(1440));
        let other = PriceCheckpoint::new("polymarket", Some(2), None, None, Some(1440));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part.parquet");
        std::fs::write(&path, b"x").unwrap();
        assert!(!price_token_up_to_date(
            &path,
            false,
            false,
            &checkpoint,
            Some(&other),
        ));
    }
}
