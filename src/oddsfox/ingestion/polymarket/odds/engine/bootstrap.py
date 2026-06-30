from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List

from oddsfox.ingestion.polymarket.odds.deps import OddsSyncRuntime
from oddsfox.ingestion.polymarket.odds.planning import (
    iter_token_plans_paged,
    parse_cutoff_date,
)
from oddsfox.ingestion.polymarket.odds.support import (
    DEFAULT_WRITER_CHUNK_ROWS,
    DEFAULT_WRITER_FLUSH_ROWS,
    PlanningState,
    build_planning_context,
    log_planning_context,
    log_planning_state,
    planning_state_to_dict,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NormalizedSyncParams:
    window_seconds: int
    writer_chunk_rows: int
    writer_flush_rows: int
    min_split_window_seconds: int
    routine_interval_seconds: int
    empty_retry_base_seconds: int
    empty_retry_max_seconds: int
    error_retry_seconds: int


def normalize_sync_params(
    *,
    batch_size: int,
    window_hours: int,
    writer_chunk_rows: int,
    writer_flush_rows: int,
    min_split_window_minutes: int,
    routine_interval_hours: int,
    empty_retry_base_hours: int,
    empty_retry_max_hours: int,
    error_retry_minutes: int,
) -> NormalizedSyncParams:
    window_seconds = max(60, int(window_hours * 3600))
    normalized_writer_chunk_rows = max(
        1, int(writer_chunk_rows or batch_size or DEFAULT_WRITER_CHUNK_ROWS)
    )
    normalized_writer_flush_rows = max(
        normalized_writer_chunk_rows,
        int(writer_flush_rows or batch_size or DEFAULT_WRITER_FLUSH_ROWS),
    )
    return NormalizedSyncParams(
        window_seconds=window_seconds,
        writer_chunk_rows=normalized_writer_chunk_rows,
        writer_flush_rows=normalized_writer_flush_rows,
        min_split_window_seconds=max(60, int(min_split_window_minutes * 60)),
        routine_interval_seconds=max(0, int(routine_interval_hours * 3600)),
        empty_retry_base_seconds=max(0, int(empty_retry_base_hours * 3600)),
        empty_retry_max_seconds=max(0, int(empty_retry_max_hours * 3600)),
        error_retry_seconds=max(0, int(error_retry_minutes * 60)),
    )


def setup_guardrail(
    runtime: OddsSyncRuntime,
    *,
    progress_log_interval_seconds: int,
    no_progress_soft_timeout_seconds: int | None,
    no_progress_hard_timeout_seconds: int | None,
    progress_log_interval_tokens: int,
    progress_callback: Callable[[str, dict[str, Any]], None] | None,
):
    guardrail = runtime.progress_guardrail(
        asset="sync_odds",
        logger=logger,
        progress_log_interval_seconds=progress_log_interval_seconds,
        no_progress_soft_timeout_seconds=no_progress_soft_timeout_seconds,
        no_progress_hard_timeout_seconds=no_progress_hard_timeout_seconds,
        work_log_interval=progress_log_interval_tokens,
        progress_callback=progress_callback,
    )
    guardrail.record_progress(
        work_increment=0, phase="start", diagnostics={}, force_log=True
    )
    return guardrail


def make_persist_invalid_tokens_batch(
    runtime: OddsSyncRuntime,
    persisted_invalid_tokens: set[str],
) -> Callable[[List[tuple[str, str]]], None]:
    def persist_invalid_tokens_batch(token_reasons: List[tuple[str, str]]):
        new_records = [
            (token_id, reason)
            for token_id, reason in token_reasons
            if token_id not in persisted_invalid_tokens
        ]
        if not new_records:
            return
        runtime.save_skipped_tokens(new_records)
        persisted_invalid_tokens.update(token_id for token_id, _ in new_records)

    return persist_invalid_tokens_batch


def create_plan_iterator(
    plan_iterator_factory: Callable[..., Any],
    *,
    now_ts: int,
    clob_cutoff_date: str,
    fidelity: int,
    force: bool,
    rebuild_minutely: bool,
    overlap_minutes: int,
    skip_recent_minutes: int,
    market_page_size: int,
    reconcile_ledger: bool,
    short_range_first: bool,
    market_scope: str,
    ended_market_grace_days: int | None,
    min_volume: float | None,
    max_volume: float | None,
    minutely_backfill_days: int,
    empty_token_skip_budgets: Dict[str, int] | None,
    empty_token_skip_runs: int,
    on_invalid_tokens_batch: Callable[[List[tuple[str, str]]], None],
):
    return plan_iterator_factory(
        now_ts=now_ts,
        clob_cutoff_date=clob_cutoff_date,
        fidelity=fidelity,
        force=force,
        rebuild_minutely=rebuild_minutely,
        overlap_minutes=overlap_minutes,
        skip_recent_minutes=skip_recent_minutes,
        market_page_size=market_page_size,
        reconcile_ledger=reconcile_ledger,
        short_range_first=short_range_first,
        market_scope=market_scope,
        ended_market_grace_days=ended_market_grace_days,
        min_volume=min_volume,
        max_volume=max_volume,
        minutely_backfill_days=minutely_backfill_days,
        empty_token_skip_budgets=empty_token_skip_budgets,
        empty_token_skip_runs=empty_token_skip_runs,
        on_invalid_tokens_batch=on_invalid_tokens_batch,
    )


@dataclass
class PlanningBootstrap:
    raw_pre: Dict[str, Any]
    now_ts: int
    plan_iter: Any
    planning_state: PlanningState = field(default_factory=PlanningState)
    invalid_tokens: Dict[str, str] = field(default_factory=dict)
    persisted_invalid_tokens: set[str] = field(default_factory=set)
    persist_invalid_tokens_batch: Callable[[List[tuple[str, str]]], None] | None = None
    first_plan: Any | None = None
    candidate_tokens: int = 0
    candidate_markets: int = 0


def bootstrap_planning(
    runtime: OddsSyncRuntime,
    *,
    clob_cutoff_date: str,
    fidelity: int,
    force: bool,
    rebuild_minutely: bool,
    overlap_minutes: int,
    skip_recent_minutes: int,
    market_page_size: int,
    reconcile_ledger: bool,
    short_range_first: bool,
    market_scope: str,
    ended_market_grace_days: int | None,
    min_volume: float | None,
    max_volume: float | None,
    minutely_backfill_days: int,
    empty_token_skip_budgets: Dict[str, int] | None,
    empty_token_skip_runs: int,
    plan_iterator_factory: Callable[..., Any] = iter_token_plans_paged,
) -> PlanningBootstrap:
    runtime.ensure_duck_db()
    raw_pre = runtime.snapshot_raw_layer()
    now_ts = int(datetime.now(timezone.utc).timestamp())
    boot = PlanningBootstrap(raw_pre=raw_pre, now_ts=now_ts, plan_iter=None)
    boot.persisted_invalid_tokens = set()
    boot.persist_invalid_tokens_batch = make_persist_invalid_tokens_batch(
        runtime, boot.persisted_invalid_tokens
    )
    due_only = (
        not force and not rebuild_minutely and not int(minutely_backfill_days) > 0
    )
    effective_ended_grace = ended_market_grace_days if due_only else None
    cutoff_created_at = parse_cutoff_date(clob_cutoff_date).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    try:
        counts = runtime.count_candidate_market_tokens(
            cutoff_created_at=cutoff_created_at,
            market_scope=market_scope,
            ended_market_grace_days=effective_ended_grace,
            due_only=due_only,
            min_volume=min_volume,
            max_volume=max_volume,
        )
        boot.candidate_tokens = int(counts.get("candidate_tokens", 0) or 0)
        boot.candidate_markets = int(counts.get("candidate_markets", 0) or 0)
        logger.info(
            "Odds sync candidates (approx upper bound): ~%s tokens across ~%s markets "
            "(scope=%s, due_only=%s)",
            boot.candidate_tokens,
            boot.candidate_markets,
            market_scope,
            due_only,
        )
    except Exception:
        logger.warning(
            "Candidate token count failed; progress bar runs without a total",
            exc_info=True,
        )
    boot.plan_iter = create_plan_iterator(
        plan_iterator_factory,
        now_ts=now_ts,
        clob_cutoff_date=clob_cutoff_date,
        fidelity=fidelity,
        force=force,
        rebuild_minutely=rebuild_minutely,
        overlap_minutes=overlap_minutes,
        skip_recent_minutes=skip_recent_minutes,
        market_page_size=market_page_size,
        reconcile_ledger=reconcile_ledger,
        short_range_first=short_range_first,
        market_scope=market_scope,
        ended_market_grace_days=ended_market_grace_days,
        min_volume=min_volume,
        max_volume=max_volume,
        minutely_backfill_days=minutely_backfill_days,
        empty_token_skip_budgets=empty_token_skip_budgets,
        empty_token_skip_runs=empty_token_skip_runs,
        on_invalid_tokens_batch=boot.persist_invalid_tokens_batch,
    )
    boot.planning_state = PlanningState()
    boot.invalid_tokens = {}
    try:
        boot.first_plan = next(boot.plan_iter)
    except StopIteration as done:
        if done.value:
            boot.planning_state, boot.invalid_tokens = done.value
        boot.first_plan = None
    return boot


def build_noop_sync_result(
    *,
    runtime: OddsSyncRuntime,
    guardrail: Any,
    run_started: float,
    raw_pre: Dict[str, Any],
    planning_state: PlanningState,
    invalid_tokens: Dict[str, str],
    persist_invalid_tokens_batch: Callable[[List[tuple[str, str]]], None],
) -> Dict[str, Any]:
    if invalid_tokens:
        persist_invalid_tokens_batch(list(invalid_tokens.items()))
    log_planning_state(planning_state)
    planning_context = build_planning_context(
        raw_pre, planning_state, invalid_tokens=len(invalid_tokens)
    )
    log_planning_context(planning_context)
    noop_duration = round(max(0.001, runtime.time_mod.monotonic() - run_started), 3)
    raw_post = runtime.snapshot_raw_layer()
    return {
        "task": "sync_odds",
        "noop": True,
        "duration_seconds": noop_duration,
        "soft_warning_count": guardrail.snapshot()["soft_warning_count"],
        "max_idle_seconds": guardrail.snapshot()["max_idle_seconds"],
        "planning": planning_state_to_dict(planning_state),
        "planning_context": planning_context,
        "invalid_tokens": len(invalid_tokens),
        "totals": {
            "processed_tokens": 0,
            "rows": 0,
            "windows": 0,
            "empty": 0,
            "error": 0,
            "permanent_error": 0,
            "fully_checked": 0,
            "distinct_markets": 0,
        },
        "duckdb_raw_pre": raw_pre,
        "duckdb_raw_post": raw_post,
        "aborted": False,
        "abort_reason": None,
    }
