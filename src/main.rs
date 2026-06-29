use std::path::PathBuf;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use oddsfox::cli::{
    base_snapshot_books_options, base_sync_markets_options, base_sync_prices_options,
    base_sync_user_options, lake_root_from_config, Cli, CollectCommands, Commands, ComputeCommands,
    MetricsCommands, SnapshotCommands, SyncCommands,
};
use oddsfox::config::{
    apply_active_minute_defaults, parse_date, Source, Table, TopBy, DEFAULT_BACKFILL_CONCURRENCY,
    DEFAULT_BACKFILL_FIDELITY_MINUTES, DEFAULT_BACKFILL_INTERVAL,
    DEFAULT_BACKFILL_REQUESTS_PER_SECOND,
};
use oddsfox::duckdb::default_db_for_lake;
use oddsfox::error::Result;
use oddsfox::settings::resolve_config;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("oddsfox=info".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();
    let config = resolve_config(cli.config.as_deref(), None)?;

    match &cli.command {
        Commands::Init { out } => {
            let root = out
                .clone()
                .unwrap_or_else(|| PathBuf::from(&config.data.home));
            oddsfox::init::run(&root)?;
        }
        Commands::Quickstart {
            out,
            port,
            top_volume,
        } => {
            let root = out
                .clone()
                .unwrap_or_else(|| PathBuf::from(&config.data.home));
            oddsfox::quickstart::run(oddsfox::config::QuickstartOptions {
                out: root,
                port: *port,
                top_volume: *top_volume,
            })
            .await?;
        }
        Commands::Backfill {
            source,
            out,
            db,
            active,
            closed,
            all,
            tag,
            limit,
            fidelity,
            interval,
            since,
            until,
            recent_hours,
            rps,
            concurrency,
            overwrite,
            port,
        } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            let db_path = db
                .clone()
                .unwrap_or_else(|| default_db_for_lake(&oddsfox::paths::LakePaths::new(&root)));
            oddsfox::backfill::run(oddsfox::config::BackfillOptions {
                out: root,
                db: db_path,
                source: *source,
                active: *active,
                closed: *closed,
                all: *all,
                tag: tag.clone(),
                limit: *limit,
                interval: interval
                    .clone()
                    .or_else(|| config.backfill.interval.clone())
                    .or_else(|| Some(DEFAULT_BACKFILL_INTERVAL.to_string())),
                fidelity: fidelity
                    .or(config.backfill.fidelity_minutes)
                    .or(if *active {
                        None
                    } else {
                        Some(DEFAULT_BACKFILL_FIDELITY_MINUTES)
                    }),
                since: since.as_ref().map(|s| parse_date(s)).transpose()?,
                until: until.as_ref().map(|s| parse_date(s)).transpose()?,
                recent_hours: *recent_hours,
                requests_per_second: rps
                    .or(config.backfill.requests_per_second)
                    .unwrap_or(DEFAULT_BACKFILL_REQUESTS_PER_SECOND),
                concurrency: concurrency
                    .or(config.backfill.concurrency)
                    .unwrap_or(DEFAULT_BACKFILL_CONCURRENCY),
                overwrite: *overwrite,
                max_retries: config.sync.max_retries,
                user_agent: config.sync.user_agent.clone(),
                gamma_base_url: config.polymarket.gamma_base_url.clone(),
                clob_base_url: config.polymarket.clob_base_url.clone(),
                kalshi_rest_base_url: config.kalshi.rest_base_url.clone(),
                kalshi_key_id: config.kalshi.key_id.clone(),
                kalshi_private_key_path: config.kalshi.private_key_path.clone().map(PathBuf::from),
                raw_retention_days: config.data.raw_retention_days,
                port: *port,
            })
            .await?;
        }
        Commands::Collect { target } => match target {
            CollectCommands::Hourly {
                source,
                since,
                out,
                rps,
                concurrency,
                lag_minutes,
                once,
            } => {
                let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
                oddsfox::collect::run(oddsfox::config::CollectHourlyOptions {
                    out: root,
                    source: *source,
                    since: since.as_ref().map(|s| parse_date(s)).transpose()?,
                    requests_per_second: rps
                        .or(config.backfill.requests_per_second)
                        .unwrap_or(DEFAULT_BACKFILL_REQUESTS_PER_SECOND),
                    concurrency: concurrency
                        .or(config.backfill.concurrency)
                        .unwrap_or(DEFAULT_BACKFILL_CONCURRENCY),
                    lag_minutes: *lag_minutes,
                    once: *once,
                    max_retries: config.sync.max_retries,
                    user_agent: config.sync.user_agent.clone(),
                    gamma_base_url: config.polymarket.gamma_base_url.clone(),
                    clob_base_url: config.polymarket.clob_base_url.clone(),
                    kalshi_rest_base_url: config.kalshi.rest_base_url.clone(),
                    kalshi_key_id: config.kalshi.key_id.clone(),
                    kalshi_private_key_path: config
                        .kalshi
                        .private_key_path
                        .clone()
                        .map(PathBuf::from),
                    raw_retention_days: config.data.raw_retention_days,
                })
                .await?;
            }
        },
        Commands::Sync { target } => match target {
            SyncCommands::Markets {
                source,
                status,
                series,
                event,
                active,
                closed,
                all,
                tag,
                since,
                limit,
                out,
            } => {
                let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
                let options = oddsfox::config::SyncMarketsOptions {
                    active: *active || (!*closed && !*all),
                    closed: *closed,
                    all: *all,
                    status: *status,
                    series: series.clone(),
                    event: event.clone(),
                    tag: tag.clone(),
                    since: since.as_ref().map(|s| parse_date(s)).transpose()?,
                    limit: *limit,
                    ..base_sync_markets_options(root, *source, &config)
                };
                match source {
                    Source::Polymarket => {
                        oddsfox::sync::sync_markets(options).await?;
                    }
                    Source::Kalshi => {
                        oddsfox::kalshi::sync_markets(options).await?;
                    }
                }
            }
            SyncCommands::Prices {
                source,
                market,
                series,
                active,
                all,
                tag,
                limit,
                top_limit,
                interval,
                fidelity,
                period,
                since,
                until,
                recent_hours,
                overwrite,
                rps,
                concurrency,
                out,
            } => {
                let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
                let filter_active = if *all {
                    if *active {
                        Some(true)
                    } else {
                        None
                    }
                } else if *active {
                    Some(true)
                } else {
                    None
                };
                let (fidelity, recent_hours) =
                    apply_active_minute_defaults(*active, *fidelity, *recent_hours);
                let options = oddsfox::config::SyncPricesOptions {
                    market_id: market.clone(),
                    series: series.clone(),
                    active: *active && !*all,
                    all: *all,
                    filter_active,
                    tag: tag.clone(),
                    limit: *limit,
                    top_limit: *top_limit,
                    interval: interval.clone(),
                    fidelity,
                    period: *period,
                    since: since.as_ref().map(|s| parse_date(s)).transpose()?,
                    until: until.as_ref().map(|s| parse_date(s)).transpose()?,
                    recent_hours,
                    overwrite: *overwrite,
                    concurrency: concurrency.unwrap_or(1),
                    requests_per_second: rps.unwrap_or(config.sync.requests_per_second),
                    ..base_sync_prices_options(root, *source, &config)
                };
                match source {
                    Source::Polymarket => {
                        oddsfox::prices::sync_prices(options).await?;
                    }
                    Source::Kalshi => {
                        oddsfox::kalshi::sync_prices(options).await?;
                    }
                }
            }
            SyncCommands::Trades {
                source,
                market,
                since,
                until,
                limit,
                rps,
                out,
            } => {
                let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
                let options = oddsfox::config::SyncPricesOptions {
                    market_id: market.clone(),
                    limit: *limit,
                    since: since.as_ref().map(|s| parse_date(s)).transpose()?,
                    until: until.as_ref().map(|s| parse_date(s)).transpose()?,
                    requests_per_second: rps.unwrap_or(config.sync.requests_per_second),
                    ..base_sync_prices_options(root, *source, &config)
                };
                match source {
                    Source::Kalshi => oddsfox::kalshi::sync_trades(options).await?,
                    Source::Polymarket => {
                        return Err(oddsfox::error::OddsfoxError::SyncIncomplete {
                            message: "sync trades currently supports --source kalshi".into(),
                        });
                    }
                }
            }
            SyncCommands::User {
                source,
                user,
                since,
                limit,
                out,
            } => {
                let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
                let options = oddsfox::config::SyncUserOptions {
                    user_id: user.clone(),
                    since: since.as_ref().map(|s| parse_date(s)).transpose()?,
                    limit: *limit,
                    ..base_sync_user_options(root, *source, &config)
                };
                oddsfox::user::sync_user(options).await?;
            }
        },
        Commands::Snapshot { target } => match target {
            SnapshotCommands::Books {
                source,
                market,
                active,
                top_volume,
                tokens,
                depth,
                out,
            } => {
                let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
                let options = oddsfox::config::SnapshotBooksOptions {
                    market_id: market.clone(),
                    active: *active,
                    top_volume: *top_volume,
                    tokens_file: tokens.clone(),
                    depth: *depth,
                    ..base_snapshot_books_options(root, *source, &config)
                };
                match source {
                    Source::Polymarket => oddsfox::snapshot::snapshot_books(options).await?,
                    Source::Kalshi => oddsfox::kalshi::snapshot_books(options).await?,
                }
            }
        },
        Commands::Watch {
            out,
            market,
            active,
            tag,
            top_volume,
        } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            oddsfox::clob::watch_markets(oddsfox::config::WatchOptions {
                out: root,
                market_id: market.clone(),
                active: *active,
                tag: tag.clone(),
                top_volume: *top_volume,
                ws_url: config.polymarket.ws_market_url.clone(),
                clob_base_url: config.polymarket.clob_base_url.clone(),
            })
            .await?;
        }
        Commands::Compute { target } => {
            let compute = |out: &PathBuf,
                           active: bool,
                           resolved: bool,
                           since: Option<String>,
                           bucket_width: f64|
             -> Result<oddsfox::config::ComputeOptions> {
                Ok(oddsfox::config::ComputeOptions {
                    out: out.clone(),
                    active,
                    resolved,
                    since: since.as_ref().map(|s| parse_date(s)).transpose()?,
                    bucket_width,
                })
            };
            match target {
                ComputeCommands::Liquidity { active, out } => {
                    let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
                    oddsfox::metrics::compute_liquidity(&compute(
                        &root, *active, false, None, 0.05,
                    )?)
                    .await?;
                }
                ComputeCommands::Accuracy {
                    resolved,
                    since,
                    out,
                } => {
                    let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
                    oddsfox::metrics::compute_accuracy(&compute(
                        &root,
                        false,
                        *resolved,
                        since.clone(),
                        0.05,
                    )?)
                    .await?;
                }
                ComputeCommands::Calibration {
                    resolved: _,
                    bucket_width,
                    since,
                    out,
                } => {
                    let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
                    oddsfox::metrics::compute_calibration_metrics(&compute(
                        &root,
                        false,
                        true,
                        since.clone(),
                        *bucket_width,
                    )?)
                    .await?;
                }
                ComputeCommands::All { since, out } => {
                    let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
                    oddsfox::metrics::compute_all(compute(&root, true, true, since.clone(), 0.05)?)
                        .await?;
                }
            }
        }
        Commands::Search { query, out } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            let hits = oddsfox::explore::search(&root, query)?;
            println!("{}", serde_json::to_string_pretty(&hits)?);
        }
        Commands::Market { market_id, out } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            let detail = oddsfox::explore::market_detail(&root, market_id)?;
            println!("{}", serde_json::to_string_pretty(&detail)?);
        }
        Commands::Event { event_id, out } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            let detail = oddsfox::explore::event_detail(&root, event_id)?;
            println!("{}", serde_json::to_string_pretty(&detail)?);
        }
        Commands::Resolved { out, since } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            let since = since.as_ref().map(|s| parse_date(s)).transpose()?;
            let markets = oddsfox::explore::resolved_markets(&root, since)?;
            println!("{}", serde_json::to_string_pretty(&markets)?);
        }
        Commands::Top { by, out, limit } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            let markets =
                oddsfox::snapshot::top_markets(&root, TopBy::from(*by), *limit, None, None)?;
            println!("{}", serde_json::to_string_pretty(&markets)?);
        }
        Commands::Metrics { target } => match target {
            MetricsCommands::Market { market_id, out } => {
                let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
                let metrics = oddsfox::metrics::market_metrics(&root, market_id)?;
                println!("{}", serde_json::to_string_pretty(&metrics)?);
            }
        },
        Commands::Pnl {
            source,
            user,
            format,
            out,
        } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            oddsfox::user::run_pnl(&oddsfox::config::PnlOptions {
                out: root,
                source: *source,
                user_id: user.clone(),
                format: *format,
            })?;
        }
        Commands::Check { out } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            oddsfox::check::run(&root)?;
        }
        Commands::Repair { out } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            oddsfox::repair::run(&root).await?;
        }
        Commands::Clean { out, dry_run } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            oddsfox::clean::run(&root, *dry_run)?;
        }
        Commands::Schema { table } => {
            let table: Table = table.parse()?;
            oddsfox::schema::print_schema(table);
        }
        Commands::Contract { out } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            oddsfox::contract::refresh_contract(&oddsfox::paths::LakePaths::new(&root))?;
            oddsfox::contract::print_contract()?;
        }
        Commands::Duckdb { out, db } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            oddsfox::duckdb::run(&oddsfox::config::DuckDbOptions {
                out: root.clone(),
                db: db
                    .clone()
                    .unwrap_or_else(|| default_db_for_lake(&oddsfox::paths::LakePaths::new(&root))),
            })?;
        }
        Commands::Sql {
            query,
            limit,
            out,
            db,
        } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            let db_path = db
                .clone()
                .unwrap_or_else(|| default_db_for_lake(&oddsfox::paths::LakePaths::new(&root)));
            oddsfox::sql_cmd::run_adhoc(&root, &db_path, query, *limit)?;
        }
        Commands::Serve { port, out } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            oddsfox::server::serve(oddsfox::config::ServeOptions {
                out: root,
                port: *port,
            })
            .await?;
        }
        Commands::Stats { out } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            oddsfox::ops::stats(&root)?;
        }
        Commands::Head {
            out,
            export_dir,
            limit,
        } => {
            let root = lake_root_from_config(cli.config.as_deref(), out.clone())?;
            let export_dir = export_dir
                .clone()
                .unwrap_or_else(|| oddsfox::head::default_export_dir(&root));
            oddsfox::head::run(&oddsfox::head::HeadOptions {
                out: root,
                export_dir,
                limit: *limit,
            })?;
        }
    }

    Ok(())
}
