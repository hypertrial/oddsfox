from dagster import AssetSelection, define_asset_job, multiprocess_executor

from oddsfox.orchestration.config import (
    dbt_full_refresh_run_config,
    full_refresh_events_run_config,
    minutely_odds_run_config,
)

_ANALYTICS_BUILD_EXECUTOR = multiprocess_executor.configured(
    {"max_concurrent": 1},
    name="duckdb_serial_multiprocess",
)
_DUCKDB_WAREHOUSE_TAGS = {"duckdb_warehouse": "true"}

POLYMARKET_INCREMENTAL_SELECTION = AssetSelection.assets(
    "polymarket_market_metadata_backfill",
    "polymarket_token_odds_history",
)

POLYMARKET_MINUTELY_ODDS_SELECTION = AssetSelection.assets(
    "polymarket_token_odds_history_minutely",
)

POLYMARKET_FULL_REFRESH_EVENTS_SELECTION = AssetSelection.assets(
    "dlt_polymarket_markets",
    "polymarket_markets_snapshot",
    "polymarket_wc2026_registry",
    "polymarket_market_metadata_backfill",
    "polymarket_token_odds_history",
)

POLYMARKET_FULL_PIPELINE_SELECTION = (
    POLYMARKET_FULL_REFRESH_EVENTS_SELECTION
    | POLYMARKET_MINUTELY_ODDS_SELECTION
    | AssetSelection.groups("analytics")
)

polymarket_ingest_incremental = define_asset_job(
    "polymarket_ingest_incremental",
    selection=POLYMARKET_INCREMENTAL_SELECTION,
    tags=_DUCKDB_WAREHOUSE_TAGS,
)

polymarket_minutely_odds_ingest = define_asset_job(
    "polymarket_minutely_odds_ingest",
    selection=POLYMARKET_MINUTELY_ODDS_SELECTION,
    config=minutely_odds_run_config(),
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    tags=_DUCKDB_WAREHOUSE_TAGS,
)

polymarket_ingest_full_refresh_events = define_asset_job(
    "polymarket_ingest_full_refresh_events",
    selection=POLYMARKET_FULL_REFRESH_EVENTS_SELECTION,
    config=full_refresh_events_run_config(),
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    tags=_DUCKDB_WAREHOUSE_TAGS,
)

dbt_full_refresh = define_asset_job(
    "dbt_full_refresh",
    selection=AssetSelection.groups("analytics"),
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config=dbt_full_refresh_run_config(),
    tags=_DUCKDB_WAREHOUSE_TAGS,
)

wc2026_polymarket_full_pipeline = define_asset_job(
    "wc2026_polymarket_full_pipeline",
    selection=POLYMARKET_FULL_PIPELINE_SELECTION,
    executor_def=_ANALYTICS_BUILD_EXECUTOR,
    config={**full_refresh_events_run_config(), **dbt_full_refresh_run_config()},
    tags=_DUCKDB_WAREHOUSE_TAGS,
)
