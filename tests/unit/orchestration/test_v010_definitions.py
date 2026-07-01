from dagster import build_schedule_context

from oddsfox.orchestration.definitions import defs
from oddsfox.orchestration.schedules import (
    polymarket_minutely_odds_cold_schedule,
    polymarket_minutely_odds_live_schedule,
    polymarket_minutely_odds_schedule,
)


def test_definitions_expose_v010_jobs_only():
    expected = {
        "polymarket_ingest_full_refresh_events",
        "polymarket_ingest_incremental",
        "polymarket_minutely_odds_ingest",
        "dbt_full_refresh",
        "wc2026_polymarket_full_pipeline",
    }

    assert {
        job.name for job in defs.resolve_all_job_defs() if job.name != "__ASSET_JOB"
    } == expected


def test_definitions_expose_v010_asset_keys():
    expected = {
        "dlt_polymarket_markets",
        "polymarket_markets_snapshot",
        "polymarket_wc2026_registry",
        "polymarket_market_metadata_backfill",
        "polymarket_token_odds_history",
        "polymarket_token_odds_history_minutely",
        "polymarket_odds_repair",
        "polymarket_stg_markets",
        "polymarket_stg_market_tokens",
        "polymarket_stg_odds",
        "polymarket_stg_odds_daily",
        "polymarket_stg_pipeline_run_events",
        "polymarket_stg_sync_ledger",
        "polymarket_stg_token_sync_skips",
        "polymarket_int_wc2026_markets",
        "polymarket_int_token_universe",
        "polymarket_int_wc2026_token_universe",
        "polymarket_int_token_timeseries",
        "polymarket_int_token_daily_timeseries",
        "polymarket_market_coverage",
        "polymarket_wc2026_markets",
        "polymarket_token_coverage",
        "polymarket_wc2026_token_minutely_odds",
        "polymarket_wc2026_token_daily_odds",
        "polymarket_wc2026_whale_minutely_odds",
        "polymarket_sync_run_observability",
    }

    asset_keys = {key.path[-1] for key in defs.resolve_all_asset_keys()}
    assert expected <= asset_keys
    excluded_source_slug = "fifa" + "index"
    assert not any(excluded_source_slug in key for key in asset_keys)


def test_minutely_schedules_share_job_and_cold_config():
    schedules = (
        polymarket_minutely_odds_schedule,
        polymarket_minutely_odds_cold_schedule,
        polymarket_minutely_odds_live_schedule,
    )
    assert {schedule.job_name for schedule in schedules} == {
        "polymarket_minutely_odds_ingest"
    }

    context = build_schedule_context()
    cold_run_config = (
        polymarket_minutely_odds_cold_schedule.evaluate_tick(context)
        .run_requests[0]
        .run_config
    )
    cold_config = cold_run_config["ops"]["polymarket_token_odds_history_minutely"][
        "config"
    ]
    assert cold_config["force"] is False
    assert cold_config["overlap_minutes"] == 2

    assert (
        polymarket_minutely_odds_schedule.evaluate_tick(context)
        .run_requests[0]
        .run_config
        == {}
    )
    assert (
        polymarket_minutely_odds_live_schedule.evaluate_tick(context)
        .run_requests[0]
        .run_config
        == {}
    )
