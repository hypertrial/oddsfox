"""Stable import surface for Polymarket Dagster assets and tests.

Patch symbols on this module (not ``assets_polymarket.ops`` or ``pipeline_ops``) in
integration tests to avoid hitting live Gamma/CLOB APIs.
"""

from __future__ import annotations

from oddsfox.orchestration import pipeline_ops as _ops

ProgressGuardrail = _ops.ProgressGuardrail
Thread = _ops.Thread
backfill_end_dates = _ops.backfill_end_dates
backfill_event_slugs = _ops.backfill_event_slugs
backfill_market_metadata = _ops.backfill_market_metadata
backfill_slugs = _ops.backfill_slugs
backfill_tokens = _ops.backfill_tokens
delta_dbt_models = _ops.delta_dbt_models
delta_raw_layer = _ops.delta_raw_layer
delete_orphan_market_tokens = _ops.delete_orphan_market_tokens
format_dbt_snapshot_log = _ops.format_dbt_snapshot_log
format_raw_snapshot_log = _ops.format_raw_snapshot_log
reconcile_odds_ledger = _ops.reconcile_odds_ledger
snapshot_dbt_models = _ops.snapshot_dbt_models
snapshot_raw_layer = _ops.snapshot_raw_layer
stream_dbt_build = _ops.stream_dbt_build
sync_markets = _ops.sync_markets
sync_odds = _ops.sync_odds
sync_wc2026_registry = _ops.sync_wc2026_registry

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
