"""Dagster-free entrypoints used by Polymarket assets."""

from __future__ import annotations

from threading import Thread
from typing import Any, Callable

from dagster import MaterializeResult, MetadataValue

from oddsfox.ingestion.polymarket.markets import (
    backfill_end_dates,
    backfill_event_slugs,
    backfill_market_metadata,
    backfill_slugs,
    backfill_tokens,
    sync_markets,
)
from oddsfox.ingestion.polymarket.markets.fetch import build_client
from oddsfox.ingestion.polymarket.odds import reconcile_odds_ledger, sync_odds
from oddsfox.ingestion.polymarket.wc2026_scope import (
    load_wc2026_config,
    refresh_registry_from_events,
    resolve_keyset_tag_slugs,
)
from oddsfox.ingestion.snapshot_http import TransientSnapshotHttpError
from oddsfox.orchestration.dbt_build import stream_dbt_build
from oddsfox.resources.progress_guardrails import ProgressGuardrail
from oddsfox.storage.duckdb.markets import delete_orphan_market_tokens
from oddsfox.storage.duckdb.observability import (
    delta_dbt_models,
    delta_raw_layer,
    format_dbt_snapshot_log,
    format_raw_snapshot_log,
    snapshot_dbt_models,
    snapshot_raw_layer,
)


def degraded_snapshot_result(
    *,
    asset: str,
    exc: TransientSnapshotHttpError,
    config: Any,
    guardrail: ProgressGuardrail,
    logger: Any,
) -> MaterializeResult:
    logger.warning(
        "%s degraded on transient HTTP failure: %s (status_code=%s)",
        asset,
        exc,
        exc.status_code,
    )
    guardrail.record_progress(
        work_increment=0,
        phase="degraded",
        diagnostics={
            "status_code": exc.status_code,
            "source_file": exc.source_file,
            "reason": str(exc),
        },
        force_log=True,
    )
    dagster_config = (
        config.model_dump() if hasattr(config, "model_dump") else dict(config)
    )
    return MaterializeResult(
        metadata={
            "degraded": MetadataValue.bool(True),
            "reason": MetadataValue.text(str(exc)),
            "status_code": MetadataValue.int(exc.status_code),
            "source_file": MetadataValue.text(exc.source_file or ""),
            "dagster_config": MetadataValue.json(dagster_config),
        }
    )


def sync_wc2026_registry(
    *,
    max_event_pages: int | None = None,
    max_pages_without_progress: int | None = None,
    keyset_closed: bool | None = None,
    keyset_tag_slugs: list[str] | None = None,
    keyset_volume_min: float | None = None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    client = build_client()
    cfg = load_wc2026_config()
    effective_keyset_tag_slugs = resolve_keyset_tag_slugs(
        keyset_tag_slugs, config=cfg, client=client
    )
    return refresh_registry_from_events(
        client,
        config=cfg,
        max_pages=max_event_pages,
        max_pages_without_progress=max_pages_without_progress,
        keyset_closed=keyset_closed,
        keyset_tag_slugs=effective_keyset_tag_slugs or None,
        keyset_volume_min=keyset_volume_min,
        progress_callback=progress_callback,
    )


__all__ = [
    "ProgressGuardrail",
    "Thread",
    "backfill_end_dates",
    "backfill_event_slugs",
    "backfill_market_metadata",
    "backfill_slugs",
    "backfill_tokens",
    "delta_dbt_models",
    "delta_raw_layer",
    "delete_orphan_market_tokens",
    "degraded_snapshot_result",
    "format_dbt_snapshot_log",
    "format_raw_snapshot_log",
    "reconcile_odds_ledger",
    "snapshot_dbt_models",
    "snapshot_raw_layer",
    "stream_dbt_build",
    "sync_markets",
    "sync_odds",
    "sync_wc2026_registry",
]
