from typing import Any, Callable

import dlt
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_dlt import DagsterDltResource, dlt_assets

from oddsfox.config.settings import DUCKDB_PATH
from oddsfox.ingestion.polymarket.dlt_source import polymarket_markets_source
from oddsfox.orchestration import polymarket_ops as ops
from oddsfox.orchestration.config import (
    DbtBuildConfig,
    MarketsSyncConfig,
    MetadataBackfillConfig,
    MinutelyOddsSyncConfig,
    OddsSyncConfig,
    RepairConfig,
    Wc2026RegistryConfig,
)
from oddsfox.orchestration.dbt_project import DBT_PROJECT
from oddsfox.orchestration.translators import PolymarketDagsterDbtTranslator
from oddsfox.storage.duckdb.connection import get_connection
from oddsfox.storage.duckdb.metadata import get_sync_run_metrics
from oddsfox.storage.duckdb.observability import (
    delta_dbt_models,
    delta_raw_layer,
    format_dbt_snapshot_log,
    format_raw_snapshot_log,
    snapshot_dbt_models,
    snapshot_raw_layer,
)
from oddsfox.storage.duckdb.schemas.polymarket import (
    drop_legacy_bootstrap_markets_table_if_needed,
)


def _materialize_odds_sync(
    context: AssetExecutionContext,
    config: OddsSyncConfig,
    *,
    plan_iterator_factory: Callable[..., Any] | None = None,
) -> MaterializeResult:
    def _odds_progress(phase: str, payload: dict[str, Any]) -> None:
        context.log.info("[%s] %s", phase, payload)

    resolved_plan_iterator = plan_iterator_factory
    pre = snapshot_raw_layer(level=config.raw_snapshot_level)
    sync_kwargs: dict[str, Any] = {
        "max_workers": config.workers,
        "batch_size": config.batch_size,
        "fidelity": config.fidelity,
        "requests_per_second": config.requests_per_second,
        "auto_tune_rps": config.auto_tune_rps,
        "auto_tune_max_rps": config.auto_tune_max_rps,
        "force": config.force,
        "clob_cutoff_date": config.clob_cutoff,
        "skip_recent_minutes": config.skip_recent_minutes,
        "overlap_minutes": config.overlap_minutes,
        "window_hours": config.window_hours,
        "rebuild_minutely": config.rebuild_minutely,
        "reconcile_ledger": config.reconcile_ledger,
        "short_range_first": config.short_range_first,
        "market_scope": config.market_scope,
        "ended_market_grace_days": config.ended_market_grace_days,
        "min_volume": config.min_volume,
        "max_volume": config.max_volume,
        "minutely_backfill_days": config.minutely_backfill_days,
        "empty_token_skip_runs": config.empty_skip_runs,
        "routine_interval_hours": config.routine_interval_hours,
        "empty_retry_base_hours": config.empty_retry_base_hours,
        "empty_retry_max_hours": config.empty_retry_max_hours,
        "error_retry_minutes": config.error_retry_minutes,
        "transient_retries": config.transient_retries,
        "transient_backoff_seconds": config.transient_backoff_seconds,
        "market_page_size": config.market_page_size,
        "progress_callback": _odds_progress,
        "progress_log_interval_tokens": config.progress_log_interval_tokens,
        "progress_log_interval_seconds": config.progress_log_interval_seconds,
        "no_progress_soft_timeout_seconds": config.no_progress_soft_timeout_seconds,
        "no_progress_hard_timeout_seconds": config.no_progress_hard_timeout_seconds,
        "progress_poll_seconds": config.progress_poll_seconds,
    }
    if resolved_plan_iterator is not None:
        sync_kwargs["plan_iterator_factory"] = resolved_plan_iterator
    run_summary = ops.sync_odds(**sync_kwargs)
    post = snapshot_raw_layer(level=config.raw_snapshot_level)
    dd = delta_raw_layer(pre, post)
    metadata = {
        "workers": MetadataValue.int(config.workers),
        "force": MetadataValue.bool(config.force),
        "fidelity": MetadataValue.int(config.fidelity),
        "minutely_backfill_days": MetadataValue.int(config.minutely_backfill_days),
        "planning": MetadataValue.json(run_summary.get("planning", {})),
        "planning_context": MetadataValue.json(run_summary.get("planning_context", {})),
        "totals": MetadataValue.json(run_summary.get("totals", {})),
        "duckdb_raw_pre": MetadataValue.json(pre),
        "duckdb_raw_post": MetadataValue.json(post),
        "duckdb_raw_delta": MetadataValue.json(dd),
        "run_summary": MetadataValue.json(run_summary),
    }
    if config.min_volume is not None:
        metadata["min_volume"] = MetadataValue.float(config.min_volume)
    if config.max_volume is not None:
        metadata["max_volume"] = MetadataValue.float(config.max_volume)
    return MaterializeResult(metadata=metadata)


_POLYMARKET_DLT_PIPELINE = dlt.pipeline(
    pipeline_name="polymarket_wc2026_raw",
    destination=dlt.destinations.duckdb(credentials=str(DUCKDB_PATH)),
    dataset_name="polymarket_raw",
)


@dlt_assets(
    name="polymarket_markets_raw_dlt",
    group_name="ingestion",
    dlt_source=polymarket_markets_source(),
    dlt_pipeline=_POLYMARKET_DLT_PIPELINE,
)
def polymarket_markets_raw_dlt(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    with get_connection() as conn:
        if drop_legacy_bootstrap_markets_table_if_needed(conn):
            context.log.info(
                "Dropped legacy bootstrap polymarket_raw.markets; dlt will recreate it"
            )
    yield from dlt.run(context=context)


@asset(
    name="polymarket_markets_snapshot",
    deps=["dlt_polymarket_markets"],
    group_name="ingestion",
)
def polymarket_markets_snapshot(
    context: AssetExecutionContext,
    config: MarketsSyncConfig,
) -> MaterializeResult:
    guardrail = ops.ProgressGuardrail(
        asset="polymarket_markets_snapshot",
        logger=context.log,
        progress_log_interval_seconds=config.progress_log_interval_seconds,
        no_progress_soft_timeout_seconds=config.no_progress_soft_timeout_seconds,
        no_progress_hard_timeout_seconds=config.no_progress_hard_timeout_seconds,
        work_log_interval=config.progress_log_interval_pages,
    )

    def _markets_progress(phase: str, payload: dict[str, Any]) -> None:
        guardrail.record_progress(
            work_increment=1,
            phase=phase,
            diagnostics=payload,
        )

    context.log.info(
        "polymarket_markets_snapshot start (discovery_mode=%s, progress_log_interval_pages=%s, progress_log_interval_seconds=%s, no_progress_soft_timeout_seconds=%s, no_progress_hard_timeout_seconds=%s)",
        config.discovery_mode,
        config.progress_log_interval_pages,
        config.progress_log_interval_seconds,
        config.no_progress_soft_timeout_seconds,
        config.no_progress_hard_timeout_seconds,
    )
    guardrail.record_progress(
        work_increment=0,
        phase="start",
        diagnostics={
            "mode": "wc2026_event_first",
            "discovery_mode": config.discovery_mode,
        },
        force_log=True,
    )
    pre = snapshot_raw_layer(level=config.raw_snapshot_level)
    context.log.info("DuckDB pre-run state: %s", format_raw_snapshot_log(pre))
    run_summary = ops.sync_markets(
        discovery_mode=config.discovery_mode,
        force_full_discovery=config.force_full_discovery,
        max_event_pages=config.max_event_pages,
        max_pages_without_progress=config.max_pages_without_progress,
        keyset_closed=config.keyset_closed,
        keyset_tag_slugs=config.keyset_tag_slugs,
        keyset_volume_min=config.keyset_volume_min,
        progress_callback=_markets_progress,
        progress_log_interval_pages=config.progress_log_interval_pages,
        progress_log_interval_seconds=config.progress_log_interval_seconds,
        no_progress_soft_timeout_seconds=config.no_progress_soft_timeout_seconds,
        no_progress_hard_timeout_seconds=config.no_progress_hard_timeout_seconds,
        progress_poll_seconds=config.progress_poll_seconds,
    )
    guardrail.record_progress(
        work_increment=0,
        phase="sync_markets_complete",
        diagnostics={
            "total_fetched": run_summary.get("total_fetched"),
            "aborted": run_summary.get("aborted", False),
        },
        force_log=True,
    )
    post = snapshot_raw_layer(level=config.raw_snapshot_level)
    dd = delta_raw_layer(pre, post)
    context.log.info("DuckDB delta after polymarket_markets_snapshot: %s", dd)
    context.log.info("Run summary for sync_markets: %s", run_summary)
    return MaterializeResult(
        metadata={
            "source": MetadataValue.text("gamma-api.polymarket.com"),
            "duckdb_raw_pre": MetadataValue.json(pre),
            "duckdb_raw_post": MetadataValue.json(post),
            "duckdb_raw_delta": MetadataValue.json(dd),
            "run_summary": MetadataValue.json(run_summary),
        }
    )


@asset(
    name="polymarket_wc2026_registry",
    deps=[polymarket_markets_snapshot],
    group_name="ingestion",
)
def polymarket_wc2026_registry(
    context: AssetExecutionContext,
    config: Wc2026RegistryConfig,
) -> MaterializeResult:
    def _registry_progress(phase: str, payload: dict[str, Any]) -> None:
        context.log.info("[%s] %s", phase, payload)

    if config.skip_if_snapshot_refreshed and not config.force_refresh:
        snapshot_metrics = get_sync_run_metrics("sync_markets")
        if snapshot_metrics and snapshot_metrics.get("registry_refreshed") is True:
            context.log.info(
                "Skipping WC2026 registry refresh; snapshot already refreshed registry"
            )
            pre = snapshot_raw_layer(level=config.raw_snapshot_level)
            run_summary = {
                "skipped": True,
                "reason": "snapshot_refreshed_registry",
                "snapshot_metrics": snapshot_metrics,
            }
            return MaterializeResult(
                metadata={
                    "duckdb_raw_pre": MetadataValue.json(pre),
                    "duckdb_raw_post": MetadataValue.json(pre),
                    "duckdb_raw_delta": MetadataValue.json({}),
                    "run_summary": MetadataValue.json(run_summary),
                }
            )

    pre = snapshot_raw_layer(level=config.raw_snapshot_level)
    run_summary = ops.sync_wc2026_registry(
        max_event_pages=config.max_event_pages,
        max_pages_without_progress=config.max_pages_without_progress,
        keyset_closed=config.keyset_closed,
        keyset_tag_slugs=config.keyset_tag_slugs,
        keyset_volume_min=config.keyset_volume_min,
        progress_callback=_registry_progress,
    )
    post = snapshot_raw_layer(level=config.raw_snapshot_level)
    dd = delta_raw_layer(pre, post)
    return MaterializeResult(
        metadata={
            "duckdb_raw_pre": MetadataValue.json(pre),
            "duckdb_raw_post": MetadataValue.json(post),
            "duckdb_raw_delta": MetadataValue.json(dd),
            "run_summary": MetadataValue.json(run_summary),
        }
    )


@asset(
    name="polymarket_market_metadata_backfill",
    deps=[polymarket_wc2026_registry],
    group_name="ingestion",
)
def polymarket_market_metadata_backfill(
    context: AssetExecutionContext,
    config: MetadataBackfillConfig,
) -> MaterializeResult:
    guardrail = ops.ProgressGuardrail(
        asset="polymarket_market_metadata_backfill",
        logger=context.log,
        progress_log_interval_seconds=config.progress_log_interval_seconds,
        no_progress_soft_timeout_seconds=config.no_progress_soft_timeout_seconds,
        no_progress_hard_timeout_seconds=config.no_progress_hard_timeout_seconds,
        work_log_interval=config.progress_log_interval_batches,
    )
    guardrail.record_progress(
        work_increment=0,
        phase="start",
        diagnostics={
            "batch_size": config.batch_size,
            "max_markets": config.max_markets,
        },
        force_log=True,
    )

    def _metadata_progress(phase: str, payload: dict) -> None:
        context.log.info("[%s] %s", phase, payload)
        guardrail.record_progress(work_increment=1, phase=phase, diagnostics=payload)

    def _run_phase_with_guardrail(phase_name: str, run_fn) -> dict[str, Any]:
        result: dict[str, Any] | None = None
        error: Exception | None = None

        def _target() -> None:
            nonlocal result, error
            try:
                result = run_fn()
            except Exception as exc:  # pragma: no cover
                error = exc

        worker = ops.Thread(target=_target, daemon=True)
        worker.start()
        while worker.is_alive():
            worker.join(timeout=max(1, config.progress_poll_seconds))
            if worker.is_alive():
                guardrail.check(
                    phase=phase_name,
                    diagnostics={"worker_alive": True},
                )
        if error is not None:
            raise error
        guardrail.record_progress(
            work_increment=0,
            phase=f"{phase_name}_complete",
            diagnostics={"worker_alive": False},
            force_log=True,
        )
        return result or {}

    pre = snapshot_raw_layer(level=config.raw_snapshot_level)
    backfill_summaries = [
        _run_phase_with_guardrail(
            "backfill_market_metadata",
            lambda: ops.backfill_market_metadata(
                batch_size=config.batch_size,
                max_markets=config.max_markets,
                force=config.force,
                include_tokens=True,
                include_slugs=config.include_slugs,
                include_event_slugs=config.include_event_slugs,
                include_end_dates=config.include_end_dates,
                progress_callback=_metadata_progress,
                progress_every_n_batches=config.progress_log_interval_batches,
                gamma_requests_per_second=config.gamma_requests_per_second,
                market_scope=config.market_scope,
                event_slug_fallback_max_pages=config.event_slug_fallback_max_pages,
                event_slug_fallback_max_pages_without_progress=config.event_slug_fallback_max_pages_without_progress,
                event_slug_fallback_progress_every_pages=config.event_slug_fallback_progress_pages,
            ),
        )
    ]
    post = snapshot_raw_layer(level=config.raw_snapshot_level)
    dd = delta_raw_layer(pre, post)
    return MaterializeResult(
        metadata={
            "batch_size": MetadataValue.int(config.batch_size),
            "duckdb_raw_pre": MetadataValue.json(pre),
            "duckdb_raw_post": MetadataValue.json(post),
            "duckdb_raw_delta": MetadataValue.json(dd),
            "backfill_summaries": MetadataValue.json(backfill_summaries),
        }
    )


@asset(
    name="polymarket_token_odds_history",
    deps=[polymarket_market_metadata_backfill],
    group_name="ingestion",
)
def polymarket_token_odds_history(
    context: AssetExecutionContext,
    config: OddsSyncConfig,
) -> MaterializeResult:
    return _materialize_odds_sync(context, config)


@asset(
    name="polymarket_token_odds_history_minutely",
    deps=[polymarket_market_metadata_backfill],
    group_name="ingestion",
)
def polymarket_token_odds_history_minutely(
    context: AssetExecutionContext,
    config: MinutelyOddsSyncConfig,
) -> MaterializeResult:
    return _materialize_odds_sync(context, config)


@asset(name="polymarket_odds_repair", group_name="ingestion")
def polymarket_odds_repair(
    context: AssetExecutionContext,
    config: RepairConfig,
) -> MaterializeResult:
    pre = snapshot_raw_layer(level=config.raw_snapshot_level)
    reconcile_meta = ops.reconcile_odds_ledger(
        persist_run_metrics=config.persist_run_metrics
    )
    post = snapshot_raw_layer(level=config.raw_snapshot_level)
    dd = delta_raw_layer(pre, post)
    return MaterializeResult(
        metadata={
            "reconcile": MetadataValue.json(reconcile_meta),
            "duckdb_raw_pre": MetadataValue.json(pre),
            "duckdb_raw_post": MetadataValue.json(post),
            "duckdb_raw_delta": MetadataValue.json(dd),
        }
    )


@dbt_assets(
    manifest=DBT_PROJECT.manifest_path,
    project=DBT_PROJECT,
    name="polymarket_dbt",
    dagster_dbt_translator=PolymarketDagsterDbtTranslator(),
)
def polymarket_dbt(
    context: AssetExecutionContext, dbt: DbtCliResource, config: DbtBuildConfig
):
    pre_raw = snapshot_raw_layer(level=config.raw_snapshot_level)
    pre_dbt = snapshot_dbt_models()
    removed = ops.delete_orphan_market_tokens()
    if removed:
        context.log.info(
            "Removed %s orphan market_tokens row(s) (market_id not in markets) before dbt build",
            removed,
        )

    yield from ops.stream_dbt_build(
        asset_name="polymarket_dbt",
        context=context,
        dbt=dbt,
        config=config,
    )

    post_raw = snapshot_raw_layer(level=config.raw_snapshot_level)
    post_dbt = snapshot_dbt_models()
    context.log.info(
        "DuckDB delta after dbt build (raw tables): %s",
        delta_raw_layer(pre_raw, post_raw),
    )
    context.log.info(
        "dbt model state after build: %s", format_dbt_snapshot_log(post_dbt)
    )
    context.log.info(
        "DuckDB delta after dbt build (dbt models only): %s",
        delta_dbt_models(pre_dbt, post_dbt),
    )


__all__ = [
    "polymarket_dbt",
    "polymarket_market_metadata_backfill",
    "polymarket_markets_raw_dlt",
    "polymarket_markets_snapshot",
    "polymarket_odds_repair",
    "polymarket_token_odds_history",
    "polymarket_token_odds_history_minutely",
    "polymarket_wc2026_registry",
]
