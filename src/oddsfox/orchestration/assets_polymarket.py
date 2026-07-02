from typing import Any, Callable

import dlt
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_dlt import DagsterDltResource, dlt_assets

from oddsfox.ingestion.polymarket.dlt_source import (
    collect_raw_markets,
    normalize_market_payloads_for_dlt,
    polymarket_markets_source,
)
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
from oddsfox.storage.duckdb.connection import active_duckdb_path, get_connection
from oddsfox.storage.duckdb.metadata import get_sync_run_metrics
from oddsfox.storage.duckdb.observability import (
    delta_dbt_models,
    delta_raw_layer,
    format_dbt_snapshot_log,
    format_raw_snapshot_log,
    snapshot_dbt_models,
    snapshot_raw_layer,
)
from oddsfox.storage.duckdb.schemas.polymarket import ensure_polymarket_indexes


def _raw_snapshot_metadata(
    pre: dict[str, Any],
    post: dict[str, Any],
    delta: dict[str, Any],
    *,
    run_summary: dict[str, Any] | None = None,
) -> dict[str, MetadataValue]:
    metadata = {
        "duckdb_raw_pre": MetadataValue.json(pre),
        "duckdb_raw_post": MetadataValue.json(post),
        "duckdb_raw_delta": MetadataValue.json(delta),
    }
    if run_summary is not None:
        metadata["run_summary"] = MetadataValue.json(run_summary)
    return metadata


def _run_with_raw_snapshot(
    raw_snapshot_level: str,
    run_fn: Callable[[dict[str, Any]], dict[str, Any]],
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, MetadataValue],
]:
    pre = snapshot_raw_layer(level=raw_snapshot_level)
    run_summary = run_fn(pre)
    post = snapshot_raw_layer(level=raw_snapshot_level)
    delta = delta_raw_layer(pre, post)
    return (
        run_summary,
        pre,
        post,
        delta,
        _raw_snapshot_metadata(
            pre,
            post,
            delta,
            run_summary=run_summary,
        ),
    )


def _build_odds_sync_kwargs(
    config: OddsSyncConfig,
    progress_callback: Callable[[str, dict[str, Any]], None],
    *,
    plan_iterator_factory: Callable[..., Any] | None = None,
) -> dict[str, Any]:
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
        "progress_callback": progress_callback,
        "progress_log_interval_tokens": config.progress_log_interval_tokens,
        "progress_log_interval_seconds": config.progress_log_interval_seconds,
        "no_progress_soft_timeout_seconds": config.no_progress_soft_timeout_seconds,
        "no_progress_hard_timeout_seconds": config.no_progress_hard_timeout_seconds,
        "progress_poll_seconds": config.progress_poll_seconds,
    }
    if plan_iterator_factory is not None:
        sync_kwargs["plan_iterator_factory"] = plan_iterator_factory
    return sync_kwargs


def _odds_sync_metadata(
    config: OddsSyncConfig,
    run_summary: dict[str, Any],
    raw_metadata: dict[str, MetadataValue],
) -> dict[str, MetadataValue]:
    metadata = {
        "workers": MetadataValue.int(config.workers),
        "force": MetadataValue.bool(config.force),
        "fidelity": MetadataValue.int(config.fidelity),
        "minutely_backfill_days": MetadataValue.int(config.minutely_backfill_days),
        "planning": MetadataValue.json(run_summary.get("planning", {})),
        "planning_context": MetadataValue.json(run_summary.get("planning_context", {})),
        "totals": MetadataValue.json(run_summary.get("totals", {})),
        **raw_metadata,
    }
    if config.min_volume is not None:
        metadata["min_volume"] = MetadataValue.float(config.min_volume)
    if config.max_volume is not None:
        metadata["max_volume"] = MetadataValue.float(config.max_volume)
    return metadata


def _run_with_guardrail_thread(
    guardrail: Any,
    phase_name: str,
    run_fn: Callable[[], dict[str, Any]],
    *,
    poll_seconds: float,
) -> dict[str, Any]:
    result: dict[str, Any] | None = None
    error: Exception | None = None

    def _target() -> None:
        nonlocal result, error
        try:
            result = run_fn()
        except Exception as exc:
            error = exc

    worker = ops.Thread(target=_target, daemon=True)
    worker.start()
    while worker.is_alive():
        worker.join(timeout=max(1, poll_seconds))
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


def _materialize_odds_sync(
    context: AssetExecutionContext,
    config: OddsSyncConfig,
    *,
    plan_iterator_factory: Callable[..., Any] | None = None,
) -> MaterializeResult:
    def _odds_progress(phase: str, payload: dict[str, Any]) -> None:
        context.log.info("[%s] %s", phase, payload)

    sync_kwargs = _build_odds_sync_kwargs(
        config,
        _odds_progress,
        plan_iterator_factory=plan_iterator_factory,
    )
    run_summary, _, _, _, raw_metadata = _run_with_raw_snapshot(
        config.raw_snapshot_level,
        lambda _pre: ops.sync_odds(**sync_kwargs),
    )
    metadata = _odds_sync_metadata(config, run_summary, raw_metadata)
    return MaterializeResult(metadata=metadata)


_DLT_PIPELINE_BY_PATH: dict[str, dlt.Pipeline] = {}


def get_polymarket_dlt_pipeline() -> dlt.Pipeline:
    db_path = str(active_duckdb_path())
    cached = _DLT_PIPELINE_BY_PATH.get(db_path)
    if cached is not None:
        return cached
    pipe = dlt.pipeline(
        pipeline_name="polymarket_wc2026_raw",
        destination=dlt.destinations.duckdb(credentials=db_path),
        dataset_name="polymarket_raw",
    )
    _DLT_PIPELINE_BY_PATH[db_path] = pipe
    return pipe


_POLYMARKET_DLT_PIPELINE = get_polymarket_dlt_pipeline()


@dlt_assets(
    name="dlt_polymarket_markets",
    group_name="ingestion",
    dlt_source=polymarket_markets_source(),
    dlt_pipeline=_POLYMARKET_DLT_PIPELINE,
)
def polymarket_markets_raw_dlt(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    pipeline = get_polymarket_dlt_pipeline()
    if pipeline.has_pending_data:
        context.log.info(
            "Clearing pending dlt packages for polymarket_wc2026_raw before extract"
        )
        pipeline.drop_pending_packages()
    rows = normalize_market_payloads_for_dlt(collect_raw_markets())
    dlt_source = polymarket_markets_source(rows=rows)
    yield from dlt.run(context=context, dlt_pipeline=pipeline, dlt_source=dlt_source)
    with get_connection() as conn:
        ensure_polymarket_indexes(conn)


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

    def _sync_markets(pre: dict[str, Any]) -> dict[str, Any]:
        context.log.info("DuckDB pre-run state: %s", format_raw_snapshot_log(pre))
        return ops.sync_markets(
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

    run_summary, _, _, raw_delta, raw_metadata = _run_with_raw_snapshot(
        config.raw_snapshot_level,
        _sync_markets,
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
    context.log.info("DuckDB delta after polymarket_markets_snapshot: %s", raw_delta)
    context.log.info("Run summary for sync_markets: %s", run_summary)
    return MaterializeResult(
        metadata={
            "source": MetadataValue.text("gamma-api.polymarket.com"),
            **raw_metadata,
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
                metadata=_raw_snapshot_metadata(
                    pre,
                    pre,
                    {},
                    run_summary=run_summary,
                )
            )

    run_summary, _, _, _, raw_metadata = _run_with_raw_snapshot(
        config.raw_snapshot_level,
        lambda _pre: ops.sync_wc2026_registry(
            max_event_pages=config.max_event_pages,
            max_pages_without_progress=config.max_pages_without_progress,
            keyset_closed=config.keyset_closed,
            keyset_tag_slugs=config.keyset_tag_slugs,
            keyset_volume_min=config.keyset_volume_min,
            progress_callback=_registry_progress,
        ),
    )
    return MaterializeResult(metadata=raw_metadata)


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

    pre = snapshot_raw_layer(level=config.raw_snapshot_level)
    backfill_summaries = [
        _run_with_guardrail_thread(
            guardrail,
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
            poll_seconds=config.progress_poll_seconds,
        )
    ]
    orphan_market_tokens_removed = ops.delete_orphan_market_tokens()
    if orphan_market_tokens_removed:
        context.log.info(
            "Removed %s orphan market_tokens row(s) (market_id not in markets) after metadata backfill",
            orphan_market_tokens_removed,
        )
    post = snapshot_raw_layer(level=config.raw_snapshot_level)
    dd = delta_raw_layer(pre, post)
    return MaterializeResult(
        metadata={
            "batch_size": MetadataValue.int(config.batch_size),
            **_raw_snapshot_metadata(pre, post, dd),
            "backfill_summaries": MetadataValue.json(backfill_summaries),
            "orphan_market_tokens_removed": MetadataValue.int(
                orphan_market_tokens_removed
            ),
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
            **_raw_snapshot_metadata(pre, post, dd),
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
