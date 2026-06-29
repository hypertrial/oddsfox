use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Builder, RecordBatch, StringBuilder, TimestampMillisecondBuilder,
};
use chrono::{NaiveDate, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::config::{OutputFormat, PnlOptions, SyncUserOptions, Table, UserSource};
use crate::data::{DataClient, PolymarketUserActivity, PolymarketUserPosition};
use crate::duckdb_engine::{glob_exists, open_connection, read_parquet_sql};
use crate::error::{OddsfoxError, Result};
use crate::http::HttpClient;
use crate::kalshi::client::{KalshiAuth, KalshiClient};
use crate::kalshi::models::{KalshiFill, KalshiPosition};
use crate::kalshi::normalize::{kalshi_market_id, kalshi_token_id};
use crate::manifest::{new_run_id, ManifestStore, SyncStateRecord};
use crate::normalize::IngestMetaBuilders;
use crate::parquet::{write_gold, write_snapshot};
use crate::paths::LakePaths;
use crate::schema::{user_fills, user_positions};

const POLYMARKET: &str = "polymarket";
const KALSHI: &str = "kalshi";

#[derive(Debug, Clone, Serialize)]
pub struct UserSyncSummary {
    pub fills: usize,
    pub positions: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct UserPnlRow {
    pub source: String,
    pub user_id: String,
    pub market_id: String,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub fees: f64,
    pub mark_value: f64,
    pub total_pnl: f64,
}

#[derive(Debug, Clone)]
pub struct UserFillRecord {
    pub fill_id: String,
    pub user_id: String,
    pub market_id: Option<String>,
    pub token_id: Option<String>,
    pub ts: i64,
    pub side: Option<String>,
    pub price: Option<f64>,
    pub size: Option<f64>,
    pub cash_delta: Option<f64>,
    pub fee: Option<f64>,
    pub realized_pnl: Option<f64>,
    pub raw_json: String,
    pub source: &'static str,
}

#[derive(Debug, Clone)]
pub struct UserPositionRecord {
    pub position_id: String,
    pub user_id: String,
    pub market_id: Option<String>,
    pub token_id: Option<String>,
    pub as_of: i64,
    pub size: Option<f64>,
    pub avg_price: Option<f64>,
    pub mark_price: Option<f64>,
    pub mark_value: Option<f64>,
    pub unrealized_pnl: Option<f64>,
    pub realized_pnl: Option<f64>,
    pub total_pnl: Option<f64>,
    pub status: Option<String>,
    pub raw_json: String,
    pub source: &'static str,
}

pub async fn sync_user(options: SyncUserOptions) -> Result<UserSyncSummary> {
    let store = ManifestStore::open(&options.out)?;
    let mut total = UserSyncSummary {
        fills: 0,
        positions: 0,
    };
    let mut completed = Vec::new();
    if matches!(options.source, UserSource::Polymarket | UserSource::All) {
        let result = sync_polymarket_user(&options, &store).await?;
        total.fills += result.summary.fills;
        total.positions += result.summary.positions;
        completed.push(result);
    }
    if matches!(options.source, UserSource::Kalshi | UserSource::All) {
        let result = sync_kalshi_user(&options, &store).await?;
        total.fills += result.summary.fills;
        total.positions += result.summary.positions;
        completed.push(result);
    }
    refresh_user_pnl(&options.out)?;
    for result in completed {
        if let Some(state) = result.state {
            store.upsert_sync_state(state)?;
        }
        store.append_completed_run(result.command, &result.run_id, result.started, result.rows)?;
    }
    println!(
        "sync user complete: {} fills, {} positions",
        total.fills, total.positions
    );
    Ok(total)
}

struct SourceUserSync {
    summary: UserSyncSummary,
    command: &'static str,
    run_id: String,
    started: chrono::DateTime<Utc>,
    rows: i64,
    state: Option<SyncStateRecord>,
}

pub fn run_pnl(options: &PnlOptions) -> Result<()> {
    let rows = pnl_rows(&options.out, options.source, options.user_id.as_deref())?;
    match options.format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&rows)?),
        OutputFormat::Text => print_pnl_table(&rows),
    }
    Ok(())
}

pub fn pnl_rows(
    lake_root: &Path,
    source: UserSource,
    user_id: Option<&str>,
) -> Result<Vec<UserPnlRow>> {
    let lake = LakePaths::new(lake_root);
    let conn = open_connection(None)?;
    let positions_glob = lake.duckdb_parquet_glob(Table::UserPositions);
    let fills_glob = lake.duckdb_parquet_glob(Table::UserFills);
    let has_positions = glob_exists(&positions_glob);
    let has_fills = glob_exists(&fills_glob);
    if !has_positions && !has_fills {
        return Ok(Vec::new());
    }

    let positions = if has_positions {
        read_parquet_sql(&positions_glob)
    } else {
        "(SELECT NULL::VARCHAR AS source, NULL::VARCHAR AS user_id, NULL::VARCHAR AS market_id, NULL::VARCHAR AS token_id, 0::BIGINT AS as_of, 0::BIGINT AS ingested_at, 0.0::DOUBLE AS realized_pnl, 0.0::DOUBLE AS unrealized_pnl, 0.0::DOUBLE AS mark_value, 0.0::DOUBLE AS total_pnl WHERE false)".into()
    };
    let fills = if has_fills {
        read_parquet_sql(&fills_glob)
    } else {
        "(SELECT NULL::VARCHAR AS source, NULL::VARCHAR AS user_id, NULL::VARCHAR AS market_id, NULL::VARCHAR AS fill_id, 0::BIGINT AS ingested_at, 0.0::DOUBLE AS fee WHERE false)".into()
    };
    let mut where_parts = Vec::new();
    if let Some(source) = source_filter(source) {
        where_parts.push(format!("source = '{}'", source));
    }
    if let Some(user_id) = user_id {
        where_parts.push(format!(
            "user_id = '{}'",
            crate::duckdb_engine::escape_sql_string(user_id)
        ));
    }
    let filter = if where_parts.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", where_parts.join(" AND "))
    };
    let pos_filter = filter.clone();
    let fill_filter = filter;
    let sql = format!(
        "WITH latest_positions AS (
            SELECT * EXCLUDE (rn)
            FROM (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY source, user_id, COALESCE(market_id, ''), COALESCE(token_id, '')
                           ORDER BY as_of DESC, ingested_at DESC
                       ) AS rn
                FROM {positions}
                {pos_filter}
            )
            WHERE rn = 1
        ), dedup_fills AS (
            SELECT * EXCLUDE (rn)
            FROM (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY source, fill_id
                           ORDER BY ingested_at DESC
                       ) AS rn
                FROM {fills}
                {fill_filter}
            )
            WHERE rn = 1
        ), pos AS (
            SELECT source, user_id, COALESCE(market_id, '') AS market_id,
                   SUM(COALESCE(realized_pnl, 0)) AS realized_pnl,
                   SUM(COALESCE(unrealized_pnl, 0)) AS unrealized_pnl,
                   SUM(COALESCE(mark_value, 0)) AS mark_value,
                   SUM(COALESCE(total_pnl, realized_pnl, 0)) AS total_pnl
            FROM latest_positions
            GROUP BY source, user_id, COALESCE(market_id, '')
        ), fees AS (
            SELECT source, user_id, COALESCE(market_id, '') AS market_id,
                   SUM(COALESCE(fee, 0)) AS fees
            FROM dedup_fills
            GROUP BY source, user_id, COALESCE(market_id, '')
        )
        SELECT COALESCE(pos.source, fees.source) AS source,
               COALESCE(pos.user_id, fees.user_id) AS user_id,
               COALESCE(pos.market_id, fees.market_id) AS market_id,
               COALESCE(pos.realized_pnl, 0) AS realized_pnl,
               COALESCE(pos.unrealized_pnl, 0) AS unrealized_pnl,
               COALESCE(fees.fees, 0) AS fees,
               COALESCE(pos.mark_value, 0) AS mark_value,
               COALESCE(pos.total_pnl, pos.realized_pnl, 0) - COALESCE(fees.fees, 0) AS total_pnl
        FROM pos
        FULL OUTER JOIN fees USING (source, user_id, market_id)
        ORDER BY total_pnl DESC, source, user_id, market_id"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        Ok(UserPnlRow {
            source: row.get(0)?,
            user_id: row.get(1)?,
            market_id: row.get(2)?,
            realized_pnl: row.get(3)?,
            unrealized_pnl: row.get(4)?,
            fees: row.get(5)?,
            mark_value: row.get(6)?,
            total_pnl: row.get(7)?,
        })
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn print_pnl_table(rows: &[UserPnlRow]) {
    println!("source\tuser_id\tmarket_id\trealized\tunrealized\tfees\tmark_value\ttotal");
    for row in rows {
        println!(
            "{}\t{}\t{}\t{:.4}\t{:.4}\t{:.4}\t{:.4}\t{:.4}",
            row.source,
            row.user_id,
            row.market_id,
            row.realized_pnl,
            row.unrealized_pnl,
            row.fees,
            row.mark_value,
            row.total_pnl
        );
    }
}

pub fn refresh_user_pnl(lake_root: &Path) -> Result<Vec<UserPnlRow>> {
    let rows = pnl_rows(lake_root, UserSource::All, None)?;
    if rows.is_empty() {
        return Ok(rows);
    }
    let lake = LakePaths::new(lake_root);
    let batch = pnl_batch(&rows)?;
    write_gold(&lake, "user_pnl", "user-pnl", &[batch])?;
    Ok(rows)
}

async fn sync_polymarket_user(
    options: &SyncUserOptions,
    store: &ManifestStore,
) -> Result<SourceUserSync> {
    let user_id = options
        .user_id
        .as_deref()
        .ok_or_else(|| OddsfoxError::SyncIncomplete {
            message: "pass --user <wallet_or_proxy> for Polymarket user sync".into(),
        })?;
    let paths = LakePaths::new(&options.out);
    let run_id = new_run_id();
    let started = Utc::now();
    let http = HttpClient::new(
        options.requests_per_second,
        options.max_retries,
        options.user_agent.clone(),
    )?;
    let client = DataClient::new(options.data_base_url.clone(), http);
    let start_ts = effective_start_secs(
        options.since,
        store.sync_state(POLYMARKET, &polymarket_activity_cursor_key(user_id)),
    );
    let activity = client
        .fetch_user_activity_since(user_id, start_ts, options.limit)
        .await?;
    let current = client
        .fetch_current_positions(user_id, options.limit)
        .await?;
    let closed = client
        .fetch_closed_positions(user_id, options.limit)
        .await?;
    let _ = client.fetch_user_value(user_id).await.ok();

    let since_ms = start_ts.map(|ts| ts * 1000);
    let fills: Vec<_> = activity
        .iter()
        .filter_map(|activity| polymarket_fill(user_id, activity))
        .filter(|fill| since_ms.is_none_or(|since| fill.ts >= since))
        .collect();
    let positions: Vec<_> = current
        .iter()
        .chain(closed.iter())
        .map(|position| polymarket_position(user_id, position))
        .collect();
    write_user_batches(&paths, &run_id, &fills, &positions)?;
    let rows = (fills.len() + positions.len()) as i64;
    let state = latest_fill_secs(&fills)
        .map(|ts| user_sync_state(POLYMARKET, polymarket_activity_cursor_key(user_id), ts));
    Ok(SourceUserSync {
        summary: UserSyncSummary {
            fills: fills.len(),
            positions: positions.len(),
        },
        command: "sync user --source polymarket",
        run_id,
        started,
        rows,
        state,
    })
}

async fn sync_kalshi_user(
    options: &SyncUserOptions,
    store: &ManifestStore,
) -> Result<SourceUserSync> {
    let key_id = options
        .kalshi_key_id
        .clone()
        .ok_or_else(|| OddsfoxError::Config("set kalshi.key_id for Kalshi user sync".into()))?;
    let private_key_path = options.kalshi_private_key_path.clone().ok_or_else(|| {
        OddsfoxError::Config("set kalshi.private_key_path for Kalshi user sync".into())
    })?;
    let paths = LakePaths::new(&options.out);
    let run_id = new_run_id();
    let started = Utc::now();
    let http = HttpClient::new(
        options.requests_per_second,
        options.max_retries,
        options.user_agent.clone(),
    )?;
    let auth = Some(KalshiAuth::from_key_file(
        key_id.clone(),
        &private_key_path,
    )?);
    let client = KalshiClient::new(options.kalshi_rest_base_url.clone(), http, auth);
    let start_ts = effective_start_secs(
        options.since,
        store.sync_state(KALSHI, &kalshi_fills_cursor_key(&key_id)),
    );
    let fills_raw = client.get_fills(start_ts, options.limit).await?;
    let positions_raw = client.get_positions(options.limit).await?;
    let fills: Vec<_> = fills_raw
        .iter()
        .map(|fill| kalshi_fill(&key_id, fill))
        .collect();
    let positions: Vec<_> = positions_raw
        .iter()
        .map(|position| kalshi_position(&key_id, position))
        .collect();
    write_user_batches(&paths, &run_id, &fills, &positions)?;
    let rows = (fills.len() + positions.len()) as i64;
    let state = latest_fill_secs(&fills)
        .map(|ts| user_sync_state(KALSHI, kalshi_fills_cursor_key(&key_id), ts));
    Ok(SourceUserSync {
        summary: UserSyncSummary {
            fills: fills.len(),
            positions: positions.len(),
        },
        command: "sync user --source kalshi",
        run_id,
        started,
        rows,
        state,
    })
}

fn write_user_batches(
    paths: &LakePaths,
    run_id: &str,
    fills: &[UserFillRecord],
    positions: &[UserPositionRecord],
) -> Result<()> {
    if !fills.is_empty() {
        let batch = user_fills_batch(fills, run_id)?;
        write_snapshot(paths, Table::UserFills, run_id, &[batch])?;
    }
    if !positions.is_empty() {
        let batch = user_positions_batch(positions, run_id)?;
        write_snapshot(paths, Table::UserPositions, run_id, &[batch])?;
    }
    Ok(())
}

fn polymarket_fill(user_id: &str, activity: &PolymarketUserActivity) -> Option<UserFillRecord> {
    let kind = activity.activity_type.as_deref()?.to_ascii_lowercase();
    if !matches!(kind.as_str(), "trade" | "buy" | "sell") {
        return None;
    }
    let raw_json = serde_json::to_string(activity).unwrap_or_else(|_| "{}".into());
    let ts = activity
        .timestamp
        .map(|ts| if ts > 9_999_999_999 { ts } else { ts * 1000 })
        .unwrap_or_else(|| Utc::now().timestamp_millis());
    let side = activity.side.clone().or_else(|| Some(kind.clone()));
    let price = activity.price;
    let size = activity.size;
    let cash_delta = activity
        .usdc_size
        .or_else(|| price.zip(size).map(|(price, size)| price * size));
    Some(UserFillRecord {
        fill_id: activity.transaction_hash.clone().unwrap_or_else(|| {
            deterministic_fill_id(FillIdParts {
                source: POLYMARKET,
                user_id,
                market_id: activity
                    .market
                    .as_deref()
                    .or(activity.condition_id.as_deref()),
                token_id: activity
                    .asset_id
                    .as_deref()
                    .or(activity.asset.as_deref())
                    .or(activity.token_id.as_deref()),
                ts,
                side: side.as_deref(),
                price,
                size,
                raw_json: &raw_json,
            })
        }),
        user_id: activity
            .proxy_wallet
            .clone()
            .or(activity.user.clone())
            .unwrap_or_else(|| user_id.to_string()),
        market_id: activity.market.clone().or(activity.condition_id.clone()),
        token_id: activity
            .asset_id
            .clone()
            .or(activity.asset.clone())
            .or(activity.token_id.clone()),
        ts,
        side,
        price,
        size,
        cash_delta,
        fee: None,
        realized_pnl: None,
        raw_json,
        source: POLYMARKET,
    })
}

fn polymarket_position(user_id: &str, position: &PolymarketUserPosition) -> UserPositionRecord {
    let raw_json = serde_json::to_string(position).unwrap_or_else(|_| "{}".into());
    let now = Utc::now().timestamp_millis();
    let market_id = position.market.clone().or(position.condition_id.clone());
    let token_id = position
        .asset_id
        .clone()
        .or(position.asset.clone())
        .or(position.token_id.clone());
    UserPositionRecord {
        position_id: format!(
            "polymarket:{}:{}",
            user_id,
            token_id
                .as_deref()
                .or(market_id.as_deref())
                .unwrap_or("unknown")
        ),
        user_id: position
            .proxy_wallet
            .clone()
            .or(position.user.clone())
            .unwrap_or_else(|| user_id.to_string()),
        market_id,
        token_id,
        as_of: now,
        size: position.size,
        avg_price: position.avg_price,
        mark_price: position.cur_price,
        mark_value: position.current_value.or(position.value),
        unrealized_pnl: position.cash_pnl,
        realized_pnl: position.realized_pnl,
        total_pnl: position
            .realized_pnl
            .zip(position.cash_pnl)
            .map(|(realized, unrealized)| realized + unrealized)
            .or(position.cash_pnl)
            .or(position.realized_pnl),
        status: position.status.clone(),
        raw_json,
        source: POLYMARKET,
    }
}

fn kalshi_fill(user_id: &str, fill: &KalshiFill) -> UserFillRecord {
    let raw_json = serde_json::to_string(fill).unwrap_or_else(|_| "{}".into());
    let ticker = fill
        .ticker
        .as_deref()
        .or(fill.market_ticker.as_deref())
        .unwrap_or("unknown");
    let side = fill
        .side
        .clone()
        .or(fill.action.clone())
        .unwrap_or_else(|| "yes".into())
        .to_ascii_lowercase();
    let ts = fill
        .created_ts
        .map(|ts| ts * 1000)
        .or_else(|| {
            crate::normalize::parse_ts(fill.created_time.as_deref()).map(|dt| dt.timestamp_millis())
        })
        .unwrap_or_else(|| Utc::now().timestamp_millis());
    let price = normalize_price(fill.yes_price_dollars.or(fill.yes_price));
    let size = fill.count_fp.or(fill.count);
    UserFillRecord {
        fill_id: fill
            .fill_id
            .clone()
            .or(fill.trade_id.clone())
            .or(fill.order_id.clone())
            .unwrap_or_else(|| {
                deterministic_fill_id(FillIdParts {
                    source: KALSHI,
                    user_id,
                    market_id: Some(ticker),
                    token_id: Some(&side),
                    ts,
                    side: Some(&side),
                    price,
                    size,
                    raw_json: &raw_json,
                })
            }),
        user_id: user_id.to_string(),
        market_id: Some(kalshi_market_id(ticker)),
        token_id: Some(kalshi_token_id(ticker, &side)),
        ts,
        side: Some(side),
        price,
        size,
        cash_delta: price.zip(size).map(|(price, size)| price * size),
        fee: normalize_price(fill.fee_dollars.or(fill.fee_cost).or(fill.fee)),
        realized_pnl: None,
        raw_json,
        source: KALSHI,
    }
}

fn kalshi_position(user_id: &str, position: &KalshiPosition) -> UserPositionRecord {
    let raw_json = serde_json::to_string(position).unwrap_or_else(|_| "{}".into());
    let ticker = position
        .ticker
        .as_deref()
        .or(position.market_ticker.as_deref())
        .unwrap_or("unknown");
    let yes_count = position.yes_count_fp.or(position.yes_count);
    let no_count = position.no_count_fp.or(position.no_count);
    let side = if no_count.unwrap_or(0.0) > yes_count.unwrap_or(0.0) {
        "no"
    } else {
        "yes"
    };
    let size = position
        .position_fp
        .or(position.position)
        .or(yes_count)
        .or_else(|| no_count.map(|count| -count));
    let realized_pnl = normalize_price(position.realized_pnl_dollars.or(position.realized_pnl));
    let mark_value = normalize_price(
        position
            .market_exposure_dollars
            .or(position.market_exposure),
    );
    UserPositionRecord {
        position_id: format!("kalshi:{user_id}:{ticker}:{side}"),
        user_id: user_id.to_string(),
        market_id: Some(kalshi_market_id(ticker)),
        token_id: Some(kalshi_token_id(ticker, side)),
        as_of: Utc::now().timestamp_millis(),
        size,
        avg_price: None,
        mark_price: None,
        mark_value,
        unrealized_pnl: None,
        realized_pnl,
        total_pnl: realized_pnl,
        status: None,
        raw_json,
        source: KALSHI,
    }
}

fn normalize_price(value: Option<f64>) -> Option<f64> {
    value.map(|value| if value > 1.0 { value / 100.0 } else { value })
}

fn since_secs(since: Option<NaiveDate>) -> Option<i64> {
    since.map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp())
}

fn effective_start_secs(since: Option<NaiveDate>, state: Option<SyncStateRecord>) -> Option<i64> {
    since_secs(since).or_else(|| state.and_then(|state| state.cursor_value.parse::<i64>().ok()))
}

fn latest_fill_secs(fills: &[UserFillRecord]) -> Option<i64> {
    fills.iter().map(|fill| fill.ts / 1000).max()
}

fn polymarket_activity_cursor_key(user_id: &str) -> String {
    format!("user_activity:{user_id}")
}

fn kalshi_fills_cursor_key(user_id: &str) -> String {
    format!("user_fills:{user_id}")
}

fn user_sync_state(source: &str, cursor_key: String, ts: i64) -> SyncStateRecord {
    SyncStateRecord {
        source: source.into(),
        cursor_key,
        cursor_value: ts.to_string(),
        last_ts: chrono::DateTime::from_timestamp(ts, 0),
        updated_at: Utc::now(),
    }
}

struct FillIdParts<'a> {
    source: &'a str,
    user_id: &'a str,
    market_id: Option<&'a str>,
    token_id: Option<&'a str>,
    ts: i64,
    side: Option<&'a str>,
    price: Option<f64>,
    size: Option<f64>,
    raw_json: &'a str,
}

fn deterministic_fill_id(parts: FillIdParts<'_>) -> String {
    let mut hasher = Sha256::new();
    let hash_parts = [
        parts.source.to_string(),
        parts.user_id.to_string(),
        parts.market_id.unwrap_or("").to_string(),
        parts.token_id.unwrap_or("").to_string(),
        parts.ts.to_string(),
        parts.side.unwrap_or("").to_string(),
        parts.price.map(|v| v.to_string()).unwrap_or_default(),
        parts.size.map(|v| v.to_string()).unwrap_or_default(),
        parts.raw_json.to_string(),
    ];
    for part in hash_parts {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    let digest = hasher.finalize();
    format!("{}:{}", parts.source, hex_prefix(&digest, 16))
}

fn hex_prefix(bytes: &[u8], n: usize) -> String {
    bytes
        .iter()
        .flat_map(|byte| [byte >> 4, byte & 0x0f])
        .take(n)
        .map(|nibble| char::from_digit(u32::from(nibble), 16).unwrap())
        .collect()
}

fn source_filter(source: UserSource) -> Option<&'static str> {
    match source {
        UserSource::Polymarket => Some(POLYMARKET),
        UserSource::Kalshi => Some(KALSHI),
        UserSource::All => None,
    }
}

fn user_fills_batch(records: &[UserFillRecord], run_id: &str) -> Result<RecordBatch> {
    let schema = user_fills::schema();
    let mut fill_id = StringBuilder::new();
    let mut user_id = StringBuilder::new();
    let mut market_id = StringBuilder::new();
    let mut token_id = StringBuilder::new();
    let mut ts = TimestampMillisecondBuilder::new();
    let mut side = StringBuilder::new();
    let mut price = Float64Builder::new();
    let mut size = Float64Builder::new();
    let mut cash_delta = Float64Builder::new();
    let mut fee = Float64Builder::new();
    let mut realized_pnl = Float64Builder::new();
    let mut raw_json = StringBuilder::new();
    let mut meta = IngestMetaBuilders::new();
    for record in records {
        fill_id.append_value(&record.fill_id);
        user_id.append_value(&record.user_id);
        market_id.append_option(record.market_id.as_deref());
        token_id.append_option(record.token_id.as_deref());
        ts.append_value(record.ts);
        side.append_option(record.side.as_deref());
        append_f64(&mut price, record.price);
        append_f64(&mut size, record.size);
        append_f64(&mut cash_delta, record.cash_delta);
        append_f64(&mut fee, record.fee);
        append_f64(&mut realized_pnl, record.realized_pnl);
        raw_json.append_value(&record.raw_json);
        meta.append(record.source, None, None, run_id);
    }
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(fill_id.finish()),
        Arc::new(user_id.finish()),
        Arc::new(market_id.finish()),
        Arc::new(token_id.finish()),
        Arc::new(ts.finish()),
        Arc::new(side.finish()),
        Arc::new(price.finish()),
        Arc::new(size.finish()),
        Arc::new(cash_delta.finish()),
        Arc::new(fee.finish()),
        Arc::new(realized_pnl.finish()),
        Arc::new(raw_json.finish()),
    ];
    columns.extend(meta.finish());
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn user_positions_batch(records: &[UserPositionRecord], run_id: &str) -> Result<RecordBatch> {
    let schema = user_positions::schema();
    let mut position_id = StringBuilder::new();
    let mut user_id = StringBuilder::new();
    let mut market_id = StringBuilder::new();
    let mut token_id = StringBuilder::new();
    let mut as_of = TimestampMillisecondBuilder::new();
    let mut size = Float64Builder::new();
    let mut avg_price = Float64Builder::new();
    let mut mark_price = Float64Builder::new();
    let mut mark_value = Float64Builder::new();
    let mut unrealized_pnl = Float64Builder::new();
    let mut realized_pnl = Float64Builder::new();
    let mut total_pnl = Float64Builder::new();
    let mut status = StringBuilder::new();
    let mut raw_json = StringBuilder::new();
    let mut meta = IngestMetaBuilders::new();
    for record in records {
        position_id.append_value(&record.position_id);
        user_id.append_value(&record.user_id);
        market_id.append_option(record.market_id.as_deref());
        token_id.append_option(record.token_id.as_deref());
        as_of.append_value(record.as_of);
        append_f64(&mut size, record.size);
        append_f64(&mut avg_price, record.avg_price);
        append_f64(&mut mark_price, record.mark_price);
        append_f64(&mut mark_value, record.mark_value);
        append_f64(&mut unrealized_pnl, record.unrealized_pnl);
        append_f64(&mut realized_pnl, record.realized_pnl);
        append_f64(&mut total_pnl, record.total_pnl);
        status.append_option(record.status.as_deref());
        raw_json.append_value(&record.raw_json);
        meta.append(record.source, None, None, run_id);
    }
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(position_id.finish()),
        Arc::new(user_id.finish()),
        Arc::new(market_id.finish()),
        Arc::new(token_id.finish()),
        Arc::new(as_of.finish()),
        Arc::new(size.finish()),
        Arc::new(avg_price.finish()),
        Arc::new(mark_price.finish()),
        Arc::new(mark_value.finish()),
        Arc::new(unrealized_pnl.finish()),
        Arc::new(realized_pnl.finish()),
        Arc::new(total_pnl.finish()),
        Arc::new(status.finish()),
        Arc::new(raw_json.finish()),
    ];
    columns.extend(meta.finish());
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn pnl_batch(rows: &[UserPnlRow]) -> Result<RecordBatch> {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("source", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("user_id", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("market_id", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("realized_pnl", arrow::datatypes::DataType::Float64, false),
        arrow::datatypes::Field::new("unrealized_pnl", arrow::datatypes::DataType::Float64, false),
        arrow::datatypes::Field::new("fees", arrow::datatypes::DataType::Float64, false),
        arrow::datatypes::Field::new("mark_value", arrow::datatypes::DataType::Float64, false),
        arrow::datatypes::Field::new("total_pnl", arrow::datatypes::DataType::Float64, false),
    ]));
    let mut source = StringBuilder::new();
    let mut user_id = StringBuilder::new();
    let mut market_id = StringBuilder::new();
    let mut realized_pnl = Float64Builder::new();
    let mut unrealized_pnl = Float64Builder::new();
    let mut fees = Float64Builder::new();
    let mut mark_value = Float64Builder::new();
    let mut total_pnl = Float64Builder::new();
    for row in rows {
        source.append_value(&row.source);
        user_id.append_value(&row.user_id);
        market_id.append_value(&row.market_id);
        realized_pnl.append_value(row.realized_pnl);
        unrealized_pnl.append_value(row.unrealized_pnl);
        fees.append_value(row.fees);
        mark_value.append_value(row.mark_value);
        total_pnl.append_value(row.total_pnl);
    }
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(source.finish()),
            Arc::new(user_id.finish()),
            Arc::new(market_id.finish()),
            Arc::new(realized_pnl.finish()),
            Arc::new(unrealized_pnl.finish()),
            Arc::new(fees.finish()),
            Arc::new(mark_value.finish()),
            Arc::new(total_pnl.finish()),
        ],
    )?)
}

fn append_f64(builder: &mut Float64Builder, value: Option<f64>) {
    if let Some(value) = value {
        builder.append_value(value);
    } else {
        builder.append_null();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_polymarket_user_rows() {
        let activity: PolymarketUserActivity = serde_json::from_value(serde_json::json!({
            "type": "TRADE",
            "proxyWallet": "0xabc",
            "conditionId": "cond",
            "asset": "token",
            "timestamp": 1700000000,
            "price": 0.4,
            "size": 2,
            "side": "BUY"
        }))
        .unwrap();
        let fill = polymarket_fill("0xabc", &activity).unwrap();
        assert_eq!(fill.user_id, "0xabc");
        assert_eq!(fill.cash_delta, Some(0.8));

        let position: PolymarketUserPosition = serde_json::from_value(serde_json::json!({
            "proxyWallet": "0xabc",
            "conditionId": "cond",
            "asset": "token",
            "size": 2,
            "avgPrice": 0.3,
            "curPrice": 0.5,
            "currentValue": 1.0,
            "cashPnl": 0.4
        }))
        .unwrap();
        let position = polymarket_position("0xabc", &position);
        assert_eq!(position.total_pnl, Some(0.4));
    }

    #[test]
    fn normalizes_kalshi_user_rows() {
        let fill: KalshiFill = serde_json::from_value(serde_json::json!({
            "fill_id": "f1",
            "ticker": "KXTEST-26",
            "side": "yes",
            "yes_price_dollars": 0.61,
            "count": 3,
            "fee_dollars": 0.01,
            "created_ts": 1700000000
        }))
        .unwrap();
        let fill = kalshi_fill("key", &fill);
        assert_eq!(fill.price, Some(0.61));
        assert_eq!(fill.fee, Some(0.01));

        let position: KalshiPosition = serde_json::from_value(serde_json::json!({
            "ticker": "KXTEST-26",
            "yes_count": 2,
            "realized_pnl_dollars": 0.12
        }))
        .unwrap();
        let position = kalshi_position("key", &position);
        assert_eq!(position.total_pnl, Some(0.12));
    }

    #[test]
    fn pnl_query_handles_positions_and_fees() {
        let dir = tempfile::tempdir().unwrap();
        let lake = LakePaths::new(dir.path());
        lake.scaffold_dirs().unwrap();
        let fills = vec![UserFillRecord {
            fill_id: "f1".into(),
            user_id: "u1".into(),
            market_id: Some("m1".into()),
            token_id: Some("t1".into()),
            ts: 1,
            side: Some("buy".into()),
            price: Some(0.5),
            size: Some(2.0),
            cash_delta: Some(1.0),
            fee: Some(0.01),
            realized_pnl: None,
            raw_json: "{}".into(),
            source: POLYMARKET,
        }];
        let positions = vec![UserPositionRecord {
            position_id: "p1".into(),
            user_id: "u1".into(),
            market_id: Some("m1".into()),
            token_id: Some("t1".into()),
            as_of: 1,
            size: Some(2.0),
            avg_price: Some(0.4),
            mark_price: Some(0.6),
            mark_value: Some(1.2),
            unrealized_pnl: Some(0.2),
            realized_pnl: Some(0.1),
            total_pnl: Some(0.3),
            status: Some("open".into()),
            raw_json: "{}".into(),
            source: POLYMARKET,
        }];
        write_user_batches(&lake, "run-1", &fills, &positions).unwrap();
        let newer_positions = vec![UserPositionRecord {
            position_id: "p2".into(),
            user_id: "u1".into(),
            market_id: Some("m1".into()),
            token_id: Some("t1".into()),
            as_of: 2,
            size: Some(2.0),
            avg_price: Some(0.4),
            mark_price: Some(0.7),
            mark_value: Some(1.4),
            unrealized_pnl: Some(0.3),
            realized_pnl: Some(0.2),
            total_pnl: Some(0.5),
            status: Some("open".into()),
            raw_json: "{}".into(),
            source: POLYMARKET,
        }];
        write_user_batches(&lake, "run-2", &fills, &newer_positions).unwrap();
        let rows = pnl_rows(dir.path(), UserSource::All, Some("u1")).unwrap();
        assert_eq!(rows.len(), 1);
        assert!((rows[0].total_pnl - 0.49).abs() < 1e-9);

        refresh_user_pnl(dir.path()).unwrap();
        let conn = crate::duckdb_engine::open_connection(None).unwrap();
        crate::duckdb_engine::register_layer_views(&conn, &lake).unwrap();
        let total: f64 = conn
            .query_row(
                "SELECT total_pnl FROM gold_user_pnl WHERE user_id = 'u1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!((total - 0.49).abs() < 1e-9);
    }

    #[test]
    fn user_sync_state_uses_latest_fill_timestamp() {
        let fills = vec![
            UserFillRecord {
                fill_id: "f1".into(),
                user_id: "u1".into(),
                market_id: None,
                token_id: None,
                ts: 1_700_000_000_000,
                side: None,
                price: None,
                size: None,
                cash_delta: None,
                fee: None,
                realized_pnl: None,
                raw_json: "{}".into(),
                source: POLYMARKET,
            },
            UserFillRecord {
                ts: 1_700_000_010_000,
                fill_id: "f2".into(),
                user_id: "u1".into(),
                market_id: None,
                token_id: None,
                side: None,
                price: None,
                size: None,
                cash_delta: None,
                fee: None,
                realized_pnl: None,
                raw_json: "{}".into(),
                source: POLYMARKET,
            },
        ];
        let state = user_sync_state(
            POLYMARKET,
            polymarket_activity_cursor_key("u1"),
            latest_fill_secs(&fills).unwrap(),
        );
        assert_eq!(state.cursor_key, "user_activity:u1");
        assert_eq!(state.cursor_value, "1700000010");
    }
}
