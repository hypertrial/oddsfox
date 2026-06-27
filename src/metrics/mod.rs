mod calibration;
mod forecasting;
mod liquidity;
mod price;
mod quality;

pub use calibration::compute_calibration;
pub use forecasting::{brier_score, compute_accuracy_metrics, log_loss};
pub use liquidity::compute_liquidity_metrics;
pub use quality::run_quality_checks;

use chrono::Utc;

use crate::config::ComputeOptions;
use crate::error::Result;
use crate::manifest::{new_run_id, ManifestStore, RunRecord};
use crate::paths::LakePaths;

pub async fn compute_all(options: ComputeOptions) -> Result<()> {
    compute_liquidity(&options).await?;
    compute_accuracy(&options).await?;
    compute_calibration_metrics(&options).await?;
    run_quality_checks(&options.out)?;
    Ok(())
}

pub async fn compute_liquidity(options: &ComputeOptions) -> Result<()> {
    let store = ManifestStore::open(&options.out)?;
    let run_id = new_run_id();
    let started = Utc::now();
    let rows = compute_liquidity_metrics(&options.out, options.active)?;
    store.append_run(RunRecord {
        run_id,
        command: "compute liquidity".into(),
        started_at: started,
        finished_at: Some(Utc::now()),
        status: "complete".into(),
        rows_written: rows,
        oddsfox_version: env!("CARGO_PKG_VERSION").into(),
    })?;
    println!("compute liquidity complete: {rows} metric points");
    Ok(())
}

pub async fn compute_accuracy(options: &ComputeOptions) -> Result<()> {
    let store = ManifestStore::open(&options.out)?;
    let run_id = new_run_id();
    let started = Utc::now();
    let rows = compute_accuracy_metrics(&options.out, options.since)?;
    store.append_run(RunRecord {
        run_id,
        command: "compute accuracy".into(),
        started_at: started,
        finished_at: Some(Utc::now()),
        status: "complete".into(),
        rows_written: rows,
        oddsfox_version: env!("CARGO_PKG_VERSION").into(),
    })?;
    println!("compute accuracy complete: {rows} metric points");
    Ok(())
}

pub async fn compute_calibration_metrics(options: &ComputeOptions) -> Result<()> {
    let store = ManifestStore::open(&options.out)?;
    let run_id = new_run_id();
    let started = Utc::now();
    let rows = compute_calibration(&options.out, options.bucket_width)?;
    store.append_run(RunRecord {
        run_id,
        command: "compute calibration".into(),
        started_at: started,
        finished_at: Some(Utc::now()),
        status: "complete".into(),
        rows_written: rows,
        oddsfox_version: env!("CARGO_PKG_VERSION").into(),
    })?;
    println!("compute calibration complete: {rows} buckets");
    Ok(())
}

pub fn market_metrics(out: &std::path::Path, market_id: &str) -> Result<Vec<MetricRow>> {
    let paths = LakePaths::new(out);
    let glob = paths.layer_parquet_glob("gold", "metric_points");
    let conn = crate::duckdb_engine::open_connection(None)?;
    let sql = format!(
        "SELECT metric_name, market_id, token_id, ts, value, window_seconds
         FROM read_parquet('{glob}')
         WHERE market_id = ?
         ORDER BY ts DESC
         LIMIT 100"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([market_id], |row| {
        Ok(MetricRow {
            metric_name: row.get(0)?,
            market_id: row.get(1)?,
            token_id: row.get(2)?,
            ts: row.get(3)?,
            value: row.get(4)?,
            window_seconds: row.get(5)?,
        })
    })?;
    Ok(rows.filter_map(|r| r.ok()).collect())
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MetricRow {
    pub metric_name: String,
    pub market_id: Option<String>,
    pub token_id: Option<String>,
    pub ts: i64,
    pub value: f64,
    pub window_seconds: Option<i32>,
}
