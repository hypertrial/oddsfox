use crate::config::QuickstartOptions;
use crate::duckdb::default_db_for_lake;
use crate::error::Result;
use crate::paths::LakePaths;

pub async fn run(options: QuickstartOptions) -> Result<()> {
    crate::init::run(&options.out)?;

    let sync_options = crate::config::SyncMarketsOptions {
        out: options.out.clone(),
        source: crate::config::Source::Polymarket,
        active: true,
        closed: false,
        all: false,
        status: None,
        series: None,
        event: None,
        tag: None,
        since: None,
        limit: Some(100),
        overwrite: false,
        gamma_base_url: crate::config::DEFAULT_GAMMA_BASE_URL.into(),
        kalshi_rest_base_url: crate::config::DEFAULT_KALSHI_REST_BASE_URL.into(),
        kalshi_key_id: None,
        kalshi_private_key_path: None,
        requests_per_second: crate::config::DEFAULT_REQUESTS_PER_SECOND,
        max_retries: crate::config::DEFAULT_MAX_RETRIES,
        user_agent: crate::config::DEFAULT_USER_AGENT.into(),
        raw_retention_days: crate::config::DEFAULT_RAW_RETENTION_DAYS,
    };
    crate::sync::sync_markets(sync_options).await?;

    let snapshot_options = crate::config::SnapshotBooksOptions {
        out: options.out.clone(),
        source: crate::config::Source::Polymarket,
        market_id: None,
        active: true,
        top_volume: Some(options.top_volume),
        tokens_file: None,
        depth: None,
        clob_base_url: crate::config::DEFAULT_CLOB_BASE_URL.into(),
        kalshi_rest_base_url: crate::config::DEFAULT_KALSHI_REST_BASE_URL.into(),
        kalshi_key_id: None,
        kalshi_private_key_path: None,
        requests_per_second: crate::config::DEFAULT_REQUESTS_PER_SECOND,
        max_retries: crate::config::DEFAULT_MAX_RETRIES,
        user_agent: crate::config::DEFAULT_USER_AGENT.into(),
    };
    let _ = crate::snapshot::snapshot_books(snapshot_options).await;

    let compute_options = crate::config::ComputeOptions {
        out: options.out.clone(),
        active: true,
        resolved: false,
        since: None,
        bucket_width: 0.05,
    };
    let _ = crate::metrics::compute_liquidity(&compute_options).await;

    let lake = LakePaths::new(&options.out);
    let duckdb_options = crate::config::DuckDbOptions {
        out: options.out.clone(),
        db: default_db_for_lake(&lake),
    };
    crate::duckdb::run(&duckdb_options)?;

    let report = crate::check::check_lake(&options.out)?;
    if !report.issues.is_empty() {
        for issue in report.issues {
            println!("quickstart warning: {issue}");
        }
    }

    println!(
        "quickstart: ok — run `oddsfox serve --out {} --port {}`",
        options.out.display(),
        options.port
    );
    Ok(())
}
