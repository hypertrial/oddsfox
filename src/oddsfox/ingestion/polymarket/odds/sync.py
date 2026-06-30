from __future__ import annotations

import sys
import time
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timezone
from queue import Queue
from threading import Thread

from tqdm import tqdm

from oddsfox.ingestion.polymarket.odds import execution as _execution_mod
from oddsfox.ingestion.polymarket.odds import planning as _planning_mod
from oddsfox.ingestion.polymarket.odds import writer as _writer_mod
from oddsfox.ingestion.polymarket.odds.deps import (
    EngineRuntime,
    ExecutionRuntime,
    OddsSyncRuntime,
    PlanningRuntime,
    PolymarketOddsBinding,
    WriterRuntime,
)
from oddsfox.ingestion.polymarket.odds.engine import (
    init_db as _engine_init_db,
)
from oddsfox.ingestion.polymarket.odds.engine import (
    reconcile_odds_ledger as _engine_reconcile_odds_ledger,
)
from oddsfox.ingestion.polymarket.odds.engine import (
    sync_odds as _engine_sync_odds,
)
from oddsfox.ingestion.polymarket.odds.fetch import (
    fetch_token_history_with_retry,
)
from oddsfox.ingestion.polymarket.odds.support import (
    DEFAULT_AUTOTUNE_429_THRESHOLD,
    DEFAULT_AUTOTUNE_ERROR_THRESHOLD,
    DEFAULT_AUTOTUNE_WINDOW_REQUESTS,
    DEFAULT_EMPTY_RETRY_BASE_HOURS,
    DEFAULT_EMPTY_RETRY_MAX_HOURS,
    DEFAULT_EMPTY_TOKEN_SKIP_RUNS,
    DEFAULT_ERROR_RETRY_MINUTES,
    DEFAULT_MARKET_PAGE_SIZE,
    DEFAULT_MIN_SPLIT_WINDOW_MINUTES,
    DEFAULT_OVERLAP_MINUTES,
    DEFAULT_ROUTINE_INTERVAL_HOURS,
    DEFAULT_SKIP_RECENT_MINUTES,
    DEFAULT_TRANSIENT_BACKOFF_SECONDS,
    DEFAULT_TRANSIENT_RETRIES,
    DEFAULT_WINDOW_HOURS,
    DEFAULT_WRITER_CHUNK_ROWS,
    DEFAULT_WRITER_FLUSH_ROWS,
    MAX_FLUSH_ROWS_CAP,
    MAX_INFLIGHT_CAP,
    MAX_WORKERS_CAP,
    InflightTokenFuture,
    PlanningState,
    TokenPlan,
    WriterBuffers,
    build_inflight_future_diagnostics,
    build_planning_context,
)
from oddsfox.resources.http import RateLimiter
from oddsfox.resources.progress_guardrails import (
    NoProgressTimeoutError,
    ProgressGuardrail,
)
from oddsfox.storage.duckdb import (
    TokenSyncSchedulerState,
    count_candidate_market_tokens,
    ensure_duck_db,
    get_connection,
    get_token_sync_snapshot,
    iter_due_market_tokens,
    iter_markets_with_tokens,
    reconcile_token_sync_ledger_from_history,
    refresh_token_odds_daily,
    save_odds_bulk_upsert,
    save_skipped_tokens,
    save_sync_run_metrics,
    snapshot_raw_layer,
    upsert_skipped_tokens_batch,
    upsert_token_sync_state_batch,
)

_parse_created_at = _planning_mod.parse_created_at
_parse_cutoff_date = _planning_mod.parse_cutoff_date
_build_single_token_plan = _planning_mod.build_single_token_plan
_build_token_plans = _planning_mod.build_token_plans
_iter_windows = _execution_mod.iter_windows
_default_rate_limiter_factory = _execution_mod.default_rate_limiter_factory
_checked_at_from_plan = _execution_mod.checked_at_from_plan
_empty_retry_next_check = _execution_mod.empty_retry_next_check
_is_interval_too_long_error = _execution_mod.is_interval_too_long_error
_dynamic_writer_flush_rows = _writer_mod.dynamic_writer_flush_rows
_apply_writer_item = _writer_mod.apply_writer_item
_maybe_auto_tune_rps = _writer_mod.maybe_auto_tune_rps
_build_inflight_future_diagnostics = build_inflight_future_diagnostics
_build_planning_context = build_planning_context


def _iter_token_plans_paged(*args, **kwargs):
    fac = sys.modules[__name__]
    kwargs.setdefault("iter_due_market_tokens_fn", fac.iter_due_market_tokens)
    kwargs.setdefault("iter_markets_with_tokens_fn", fac.iter_markets_with_tokens)
    kwargs.setdefault("get_token_sync_snapshot_fn", fac.get_token_sync_snapshot)
    kwargs.setdefault("token_sync_scheduler_state_cls", fac.TokenSyncSchedulerState)
    return _planning_mod.iter_token_plans_paged(*args, **kwargs)


def _fetch_window_with_auto_split(*args, **kwargs):
    kwargs.setdefault("fetch_token_history_fn", fetch_token_history_with_retry)
    return _execution_mod.fetch_window_with_auto_split(*args, **kwargs)


def _sync_token_plan(*args, **kwargs):
    kwargs.setdefault("fetch_window_fn", _fetch_window_with_auto_split)
    return _execution_mod.sync_token_plan(*args, **kwargs)


def _flush_writer_buffers(*args, **kwargs):
    fac = sys.modules[__name__]
    kwargs.setdefault("save_odds_bulk_upsert_fn", fac.save_odds_bulk_upsert)
    kwargs.setdefault(
        "upsert_token_sync_state_batch_fn", fac.upsert_token_sync_state_batch
    )
    kwargs.setdefault("upsert_skipped_tokens_batch_fn", fac.upsert_skipped_tokens_batch)
    return _writer_mod.flush_writer_buffers(*args, **kwargs)


def _refresh_dirty_daily_keys(*args, **kwargs):
    fac = sys.modules[__name__]
    kwargs.setdefault("refresh_token_odds_daily_fn", fac.refresh_token_odds_daily)
    return _writer_mod.refresh_dirty_daily_keys(*args, **kwargs)


def _writer_loop(*args, **kwargs):
    fac = sys.modules[__name__]
    kwargs.setdefault("get_connection_fn", fac.get_connection)
    kwargs.setdefault("dynamic_writer_flush_rows_fn", fac._dynamic_writer_flush_rows)
    kwargs.setdefault("flush_writer_buffers_fn", fac._flush_writer_buffers)
    kwargs.setdefault("apply_writer_item_fn", fac._apply_writer_item)
    kwargs.setdefault("refresh_dirty_daily_keys_fn", fac._refresh_dirty_daily_keys)
    return _writer_mod.writer_loop(*args, **kwargs)


def _default_polymarket_odds_runtime() -> OddsSyncRuntime:
    """Resolve hooks from this facade so tests can monkeypatch ``odds_sync.*`` aliases."""
    fac = sys.modules[__name__]
    return OddsSyncRuntime(
        planning=PlanningRuntime(
            iter_markets_with_tokens=fac.iter_markets_with_tokens,
            iter_due_market_tokens=fac.iter_due_market_tokens,
            count_candidate_market_tokens=fac.count_candidate_market_tokens,
            get_token_sync_snapshot=fac.get_token_sync_snapshot,
            token_sync_scheduler_state=fac.TokenSyncSchedulerState,
        ),
        execution=ExecutionRuntime(
            fetch_window_with_auto_split_impl=fac._fetch_window_with_auto_split,
            fetch_token_history_with_retry=fetch_token_history_with_retry,
            default_rate_limiter_factory=_default_rate_limiter_factory,
            sync_token_plan=fac._sync_token_plan,
        ),
        writer=WriterRuntime(
            get_connection=get_connection,
            refresh_token_odds_daily=refresh_token_odds_daily,
            save_odds_bulk_upsert=save_odds_bulk_upsert,
            upsert_skipped_tokens_batch=upsert_skipped_tokens_batch,
            upsert_token_sync_state_batch=upsert_token_sync_state_batch,
            dynamic_writer_flush_rows=fac._dynamic_writer_flush_rows,
            flush_writer_buffers=fac._flush_writer_buffers,
            apply_writer_item=fac._apply_writer_item,
            writer_loop=fac._writer_loop,
        ),
        engine=EngineRuntime(
            ensure_duck_db=fac.ensure_duck_db,
            snapshot_raw_layer=fac.snapshot_raw_layer,
            save_skipped_tokens=save_skipped_tokens,
            save_sync_run_metrics=save_sync_run_metrics,
            reconcile_token_sync_ledger_from_history=reconcile_token_sync_ledger_from_history,
            progress_guardrail=fac.ProgressGuardrail,
            no_progress_timeout_error=fac.NoProgressTimeoutError,
            thread_cls=fac.Thread,
            thread_pool_executor=fac.ThreadPoolExecutor,
            wait_fn=fac.wait,
            tqdm_mod=fac.tqdm,
            time_mod=fac.time,
        ),
    )


def _default_polymarket_odds_binding() -> PolymarketOddsBinding:
    return _default_polymarket_odds_runtime()


def init_db():
    return _engine_init_db()


def reconcile_odds_ledger(*args, **kwargs):
    return _engine_reconcile_odds_ledger(*args, **kwargs)


def sync_odds(*args, **kwargs):
    fidelity = kwargs.get("fidelity")
    if fidelity is not None and int(fidelity) < 1:
        raise ValueError("fidelity must be at least 1 minute")
    odds_binding = kwargs.pop("odds_binding", None)
    explicit_runtime = kwargs.pop("runtime", None) or odds_binding
    runtime = explicit_runtime or _default_polymarket_odds_runtime()
    if "plan_iterator_factory" not in kwargs:
        if explicit_runtime is None:
            kwargs["plan_iterator_factory"] = iter_token_plans_paged
        else:

            def _runtime_plan_iterator_factory(**plan_kwargs):
                plan_kwargs.setdefault(
                    "iter_due_market_tokens_fn", runtime.iter_due_market_tokens
                )
                plan_kwargs.setdefault(
                    "iter_markets_with_tokens_fn", runtime.iter_markets_with_tokens
                )
                plan_kwargs.setdefault(
                    "get_token_sync_snapshot_fn", runtime.get_token_sync_snapshot
                )
                plan_kwargs.setdefault(
                    "token_sync_scheduler_state_cls", runtime.token_sync_scheduler_state
                )
                return _planning_mod.iter_token_plans_paged(**plan_kwargs)

            kwargs["plan_iterator_factory"] = _runtime_plan_iterator_factory
    persist_run_metrics = bool(kwargs.get("persist_run_metrics", True))
    result = _engine_sync_odds(*args, runtime=runtime, **kwargs)
    if persist_run_metrics and result.get("noop"):
        runtime.save_sync_run_metrics(
            "sync_odds",
            {
                "noop": True,
                "duration_seconds": result.get("duration_seconds"),
                "planning": result.get("planning"),
                "planning_context": result.get("planning_context"),
                "invalid_tokens": result.get("invalid_tokens"),
                "totals": result.get("totals"),
                "duckdb_raw_pre": result.get("duckdb_raw_pre"),
                "duckdb_raw_post": result.get("duckdb_raw_post"),
                "aborted": result.get("aborted"),
                "abort_reason": result.get("abort_reason"),
            },
        )
    return result


build_single_token_plan = _build_single_token_plan
build_token_plans = _build_token_plans
iter_token_plans_paged = _iter_token_plans_paged
iter_windows = _iter_windows

__all__ = [
    "DEFAULT_AUTOTUNE_429_THRESHOLD",
    "DEFAULT_AUTOTUNE_ERROR_THRESHOLD",
    "DEFAULT_AUTOTUNE_WINDOW_REQUESTS",
    "DEFAULT_EMPTY_RETRY_BASE_HOURS",
    "DEFAULT_EMPTY_RETRY_MAX_HOURS",
    "DEFAULT_EMPTY_TOKEN_SKIP_RUNS",
    "DEFAULT_ERROR_RETRY_MINUTES",
    "DEFAULT_MARKET_PAGE_SIZE",
    "DEFAULT_MIN_SPLIT_WINDOW_MINUTES",
    "DEFAULT_OVERLAP_MINUTES",
    "DEFAULT_ROUTINE_INTERVAL_HOURS",
    "DEFAULT_SKIP_RECENT_MINUTES",
    "DEFAULT_TRANSIENT_BACKOFF_SECONDS",
    "DEFAULT_TRANSIENT_RETRIES",
    "DEFAULT_WINDOW_HOURS",
    "DEFAULT_WRITER_CHUNK_ROWS",
    "DEFAULT_WRITER_FLUSH_ROWS",
    "InflightTokenFuture",
    "MAX_FLUSH_ROWS_CAP",
    "MAX_INFLIGHT_CAP",
    "MAX_WORKERS_CAP",
    "NoProgressTimeoutError",
    "OddsSyncRuntime",
    "PlanningState",
    "ProgressGuardrail",
    "Queue",
    "RateLimiter",
    "Thread",
    "ThreadPoolExecutor",
    "TokenPlan",
    "TokenSyncSchedulerState",
    "WriterBuffers",
    "_apply_writer_item",
    "_build_inflight_future_diagnostics",
    "_build_planning_context",
    "_checked_at_from_plan",
    "_default_rate_limiter_factory",
    "_dynamic_writer_flush_rows",
    "_empty_retry_next_check",
    "_fetch_window_with_auto_split",
    "_flush_writer_buffers",
    "_is_interval_too_long_error",
    "_maybe_auto_tune_rps",
    "_parse_created_at",
    "_parse_cutoff_date",
    "_refresh_dirty_daily_keys",
    "_sync_token_plan",
    "_writer_loop",
    "build_single_token_plan",
    "build_token_plans",
    "datetime",
    "count_candidate_market_tokens",
    "ensure_duck_db",
    "fetch_token_history_with_retry",
    "get_connection",
    "get_token_sync_snapshot",
    "init_db",
    "iter_due_market_tokens",
    "iter_markets_with_tokens",
    "iter_token_plans_paged",
    "iter_windows",
    "reconcile_odds_ledger",
    "refresh_token_odds_daily",
    "save_odds_bulk_upsert",
    "save_skipped_tokens",
    "save_sync_run_metrics",
    "snapshot_raw_layer",
    "sync_odds",
    "time",
    "timezone",
    "tqdm",
    "upsert_skipped_tokens_batch",
    "upsert_token_sync_state_batch",
    "wait",
]
