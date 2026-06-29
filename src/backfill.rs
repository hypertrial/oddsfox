use crate::config::{
    apply_active_minute_defaults, BackfillOptions, BackfillSource, DuckDbOptions, Source,
    SyncMarketsOptions, SyncPricesOptions, DEFAULT_BACKFILL_INTERVAL,
};
use crate::error::Result;
use crate::progress_log::log_progress;

pub async fn run(mut options: BackfillOptions) -> Result<()> {
    let (fidelity, recent_hours) =
        apply_active_minute_defaults(options.active, options.fidelity, options.recent_hours);
    options.fidelity = fidelity;
    options.recent_hours = recent_hours;

    crate::init::run_quiet(&options.out)?;

    match options.source {
        BackfillSource::Polymarket => backfill_polymarket(&options).await?,
        BackfillSource::Kalshi => backfill_kalshi(&options).await?,
        BackfillSource::All => {
            backfill_polymarket(&options).await?;
            backfill_kalshi(&options).await?;
        }
    }

    let db_path = options.db.clone();
    crate::duckdb::run(&DuckDbOptions {
        out: options.out.clone(),
        db: db_path.clone(),
    })?;

    log_progress("backfill: checking lake");
    let report = crate::check::check_lake(&options.out)?;
    if report.issues.is_empty() {
        log_progress("backfill: lake check ok");
    } else {
        for issue in report.issues {
            log_progress(format!("backfill warning: {issue}"));
        }
    }

    log_progress(format!("backfill complete: catalog at `{}`", db_path.display()));
    println!(
        "query: `oddsfox sql \"SELECT market_id, question, volume_24h FROM bronze_markets ORDER BY volume_24h DESC NULLS LAST\" --limit 10 --out {} --db {}`",
        options.out.display(),
        db_path.display()
    );
    println!("inspect: `oddsfox head --out {}`", options.out.display());
    println!(
        "serve: `oddsfox serve --out {} --port {}`",
        options.out.display(),
        options.port
    );
    Ok(())
}

fn base_sync_markets_options(options: &BackfillOptions, source: Source) -> SyncMarketsOptions {
    SyncMarketsOptions {
        out: options.out.clone(),
        source,
        active: false,
        closed: false,
        all: false,
        status: None,
        series: None,
        event: None,
        tag: None,
        since: None,
        limit: options.limit,
        overwrite: false,
        gamma_base_url: options.gamma_base_url.clone(),
        kalshi_rest_base_url: options.kalshi_rest_base_url.clone(),
        kalshi_key_id: options.kalshi_key_id.clone(),
        kalshi_private_key_path: options.kalshi_private_key_path.clone(),
        requests_per_second: options.requests_per_second,
        max_retries: options.max_retries,
        user_agent: options.user_agent.clone(),
        raw_retention_days: options.raw_retention_days,
    }
}

fn base_sync_prices_options(options: &BackfillOptions, source: Source) -> SyncPricesOptions {
    SyncPricesOptions {
        out: options.out.clone(),
        source,
        market_id: None,
        series: None,
        active: false,
        all: false,
        filter_active: None,
        tag: None,
        limit: options.limit,
        top_limit: None,
        interval: None,
        fidelity: options.fidelity,
        period: None,
        since: options.since,
        until: options.until,
        recent_hours: options.recent_hours,
        overwrite: options.overwrite,
        concurrency: options.concurrency,
        clob_base_url: options.clob_base_url.clone(),
        kalshi_rest_base_url: options.kalshi_rest_base_url.clone(),
        kalshi_key_id: options.kalshi_key_id.clone(),
        kalshi_private_key_path: options.kalshi_private_key_path.clone(),
        requests_per_second: options.requests_per_second,
        max_retries: options.max_retries,
        user_agent: options.user_agent.clone(),
    }
}

async fn backfill_polymarket(options: &BackfillOptions) -> Result<()> {
    let sync_summary = crate::sync::sync_markets(SyncMarketsOptions {
        active: options.active || (!options.closed && !options.all),
        closed: options.closed,
        all: options.all && !options.active,
        tag: options.tag.clone(),
        ..base_sync_markets_options(options, Source::Polymarket)
    })
    .await?;

    let filter_active = if options.active {
        Some(true)
    } else if options.all && !options.active && !options.closed {
        None
    } else if options.closed {
        Some(false)
    } else {
        None
    };

    crate::prices::sync_prices(SyncPricesOptions {
        all: true,
        filter_active,
        tag: options.tag.clone(),
        interval: if options.recent_hours.is_some() {
            None
        } else {
            options
                .interval
                .clone()
                .or_else(|| Some(DEFAULT_BACKFILL_INTERVAL.to_string()))
        },
        ..base_sync_prices_options(options, Source::Polymarket)
    })
    .await?;

    log_progress(format!(
        "polymarket backfill complete: {} events, {} markets",
        sync_summary.events, sync_summary.markets
    ));
    Ok(())
}

async fn backfill_kalshi(options: &BackfillOptions) -> Result<()> {
    let markets_options = SyncMarketsOptions {
        active: options.active || !options.all,
        all: options.all && !options.active,
        status: if options.active {
            Some(crate::config::KalshiStatus::Open)
        } else {
            None
        },
        series: options.tag.clone(),
        ..base_sync_markets_options(options, Source::Kalshi)
    };
    if let Some(cutoff) = crate::kalshi::historical_cutoff(&markets_options).await? {
        println!("kalshi historical cutoff: {cutoff}");
    }
    let sync_summary = crate::kalshi::sync_markets(markets_options).await?;
    crate::kalshi::sync_prices(kalshi_price_options(options)).await?;
    log_progress(format!(
        "kalshi backfill complete: {} events, {} markets",
        sync_summary.events, sync_summary.markets
    ));
    Ok(())
}

fn kalshi_price_options(options: &BackfillOptions) -> SyncPricesOptions {
    SyncPricesOptions {
        active: options.active || !options.all,
        all: options.all && !options.active,
        filter_active: if options.active { Some(true) } else { None },
        period: options.fidelity,
        ..base_sync_prices_options(options, Source::Kalshi)
    }
}
