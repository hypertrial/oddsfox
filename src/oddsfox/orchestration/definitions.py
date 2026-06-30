from dagster import Definitions
from dagster_dbt import DbtCliResource
from dagster_dlt import DagsterDltResource

from oddsfox.config.settings import (
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
    resolve_dbt_executable,
)
from oddsfox.orchestration.assets import (
    polymarket_dbt,
    polymarket_market_metadata_backfill,
    polymarket_markets_raw_dlt,
    polymarket_markets_snapshot,
    polymarket_odds_repair,
    polymarket_token_odds_history,
    polymarket_token_odds_history_minutely,
    polymarket_wc2026_registry,
)
from oddsfox.orchestration.jobs import (
    dbt_full_refresh,
    polymarket_ingest_full_refresh_events,
    polymarket_ingest_incremental,
    polymarket_minutely_odds_ingest,
    wc2026_polymarket_full_pipeline,
)
from oddsfox.orchestration.schedules import (
    polymarket_minutely_odds_cold_schedule,
    polymarket_minutely_odds_live_schedule,
    polymarket_minutely_odds_schedule,
)

defs = Definitions(
    assets=[
        polymarket_markets_raw_dlt,
        polymarket_markets_snapshot,
        polymarket_wc2026_registry,
        polymarket_market_metadata_backfill,
        polymarket_token_odds_history,
        polymarket_token_odds_history_minutely,
        polymarket_odds_repair,
        polymarket_dbt,
    ],
    jobs=[
        polymarket_ingest_incremental,
        polymarket_ingest_full_refresh_events,
        polymarket_minutely_odds_ingest,
        dbt_full_refresh,
        wc2026_polymarket_full_pipeline,
    ],
    schedules=[
        polymarket_minutely_odds_schedule,
        polymarket_minutely_odds_cold_schedule,
        polymarket_minutely_odds_live_schedule,
    ],
    resources={
        "dbt": DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROFILES_DIR),
            profile="oddsfox",
            target="dev",
            dbt_executable=resolve_dbt_executable(),
        ),
        "dlt": DagsterDltResource(),
    },
)
