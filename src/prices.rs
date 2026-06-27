use chrono::Utc;

use crate::clob::ClobClient;
use crate::config::{SyncPricesOptions, Table};
use crate::error::Result;
use crate::http::HttpClient;
use crate::manifest::{new_run_id, ManifestStore, RunRecord};
use crate::normalize::prices_batch;
use crate::parquet::write_time_series;
use crate::paths::LakePaths;
use crate::sync::{token_ids_for_market, top_token_ids};

pub async fn sync_prices(options: SyncPricesOptions) -> Result<()> {
    let paths = LakePaths::new(&options.out);
    let store = ManifestStore::open(&options.out)?;
    let run_id = new_run_id();
    let started = Utc::now();
    let http = HttpClient::new(
        options.requests_per_second,
        options.max_retries,
        options.user_agent.clone(),
    )?;
    let clob = ClobClient::new(options.clob_base_url.clone(), http);

    let token_ids = if let Some(market_id) = options.market_id.as_deref() {
        token_ids_for_market(&options.out, market_id).await?
    } else if options.active {
        top_token_ids(&options.out, 100).await?
    } else {
        Vec::new()
    };

    let mut total_points = 0_i64;
    for token_id in token_ids {
        let history = clob
            .get_prices_history(
                &token_id,
                options.interval.as_deref(),
                options.fidelity,
                options.since.map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp()),
                options.until.map(|d| d.and_hms_opt(23, 59, 59).unwrap().and_utc().timestamp()),
            )
            .await?;
        if history.is_empty() {
            continue;
        }
        let batch = prices_batch(
            &token_id,
            None,
            &history,
            "clob_prices_history",
            options.fidelity.map(|f| f as i32),
            &run_id,
        )?;
        total_points += batch.num_rows() as i64;
        let date = Utc::now().date_naive();
        write_time_series(&paths, Table::Prices, date, &token_id, &[batch])?;
    }

    store.append_run(RunRecord {
        run_id: run_id.clone(),
        command: "sync prices".into(),
        started_at: started,
        finished_at: Some(Utc::now()),
        status: "complete".into(),
        rows_written: total_points,
        oddsfox_version: env!("CARGO_PKG_VERSION").into(),
    })?;
    println!("sync prices complete: {total_points} points (run={run_id})");
    Ok(())
}
