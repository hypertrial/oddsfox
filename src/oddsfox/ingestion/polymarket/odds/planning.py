from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple

from oddsfox.ingestion.polymarket.odds.process import is_probably_clob_token
from oddsfox.ingestion.polymarket.odds.support import (
    DEFAULT_EMPTY_TOKEN_SKIP_RUNS,
    PlanningState,
    TokenPlan,
)
from oddsfox.storage.duckdb import (
    TokenSyncSchedulerState,
    count_due_market_token_exclusions,
    get_token_sync_snapshot,
    iter_due_market_tokens,
    iter_markets_with_tokens,
)

logger = logging.getLogger(__name__)


def parse_created_at(raw_ts) -> Optional[datetime]:
    if not raw_ts:
        return None
    if isinstance(raw_ts, datetime):
        created_at = raw_ts
    else:
        clean_ts = str(raw_ts).replace("T", " ")
        if "." in clean_ts:
            clean_ts = clean_ts.split(".")[0]
        created_at = datetime.strptime(clean_ts, "%Y-%m-%d %H:%M:%S")
    if created_at.tzinfo is None:
        return created_at.replace(tzinfo=timezone.utc)
    return created_at.astimezone(timezone.utc)


def parse_cutoff_date(clob_cutoff_date: str) -> datetime:
    try:
        return datetime.strptime(clob_cutoff_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        logger.error(
            "Invalid clob_cutoff_date '%s'; using 2023-01-01", clob_cutoff_date
        )
        return datetime(2023, 1, 1, tzinfo=timezone.utc)


def build_single_token_plan(
    *,
    token_id: str,
    market_id: str,
    closed: bool,
    created_ts: int,
    latest_timestamps: Dict[str, int],
    fully_checked_tokens: set[str],
    persisted_skips: Dict[str, str],
    seen_tokens: set[str],
    now_ts: int,
    fidelity: int,
    force: bool,
    rebuild_minutely: bool,
    overlap_seconds: int,
    recent_seconds: int,
    minutely_backfill_floor_ts: int | None = None,
    empty_run_streak: int = 0,
    empty_token_skip_budgets: Optional[Dict[str, int]] = None,
    empty_token_skip_runs: int = DEFAULT_EMPTY_TOKEN_SKIP_RUNS,
) -> Tuple[Optional[TokenPlan], Optional[str], Optional[Tuple[str, str]]]:
    bypass_routine_skips = rebuild_minutely or minutely_backfill_floor_ts is not None
    if token_id in seen_tokens:
        return None, "dup_token", None
    seen_tokens.add(token_id)
    if not is_probably_clob_token(token_id):
        return None, "invalid_token", (token_id, "invalid token id format")
    if token_id in persisted_skips and not force and not bypass_routine_skips:
        return None, "persisted_skip", None
    if (
        closed
        and token_id in fully_checked_tokens
        and not force
        and not bypass_routine_skips
    ):
        return None, "closed_done", None
    if (
        empty_token_skip_budgets is not None
        and empty_token_skip_runs > 0
        and not force
        and not bypass_routine_skips
    ):
        skip_budget = int(empty_token_skip_budgets.get(token_id, 0))
        if skip_budget > 0:
            remaining = skip_budget - 1
            if remaining > 0:
                empty_token_skip_budgets[token_id] = remaining
            else:
                empty_token_skip_budgets.pop(token_id, None)
            return None, "empty_cache_skip", None
    latest_ts = latest_timestamps.get(token_id)
    if rebuild_minutely:
        start_ts = created_ts
    elif minutely_backfill_floor_ts is not None:
        start_ts = max(created_ts, int(minutely_backfill_floor_ts))
    elif latest_ts is not None:
        if (not force) and ((now_ts - int(latest_ts)) < recent_seconds):
            return None, "recent_skip", None
        start_ts = max(created_ts, int(latest_ts) - overlap_seconds)
    else:
        start_ts = created_ts
    if start_ts >= now_ts:
        return None, "already_current", None
    return (
        TokenPlan(
            token_id=token_id,
            market_id=market_id,
            is_closed=closed,
            created_at_ts=created_ts,
            start_ts=start_ts,
            end_ts=now_ts,
            fidelity=int(fidelity),
            empty_run_streak=max(0, int(empty_run_streak)),
        ),
        None,
        None,
    )


def build_token_plans(
    markets: List[Tuple[str, str, Optional[str], Optional[bool]]],
    latest_timestamps: Dict[str, int],
    fully_checked_tokens: set,
    persisted_skips: Dict[str, str],
    now_ts: int,
    clob_cutoff_date: str,
    fidelity: int,
    force: bool,
    rebuild_minutely: bool,
    overlap_minutes: int,
    skip_recent_minutes: int,
    empty_token_skip_budgets: Optional[Dict[str, int]] = None,
    empty_token_skip_runs: int = DEFAULT_EMPTY_TOKEN_SKIP_RUNS,
):
    cutoff_dt = parse_cutoff_date(clob_cutoff_date)
    overlap_seconds = max(0, int(overlap_minutes * 60))
    recent_seconds = max(0, int(skip_recent_minutes * 60))
    plans: List[TokenPlan] = []
    invalid_tokens: List[Tuple[str, str]] = []
    counters = {
        "pre_clob_markets": 0,
        "invalid_token": 0,
        "closed_done": 0,
        "persisted_skip": 0,
        "recent_skip": 0,
        "empty_cache_skip": 0,
        "already_current": 0,
        "dup_token": 0,
    }
    seen_tokens = set()
    for market_id, tokens_json, created_at_raw, is_closed in markets:
        created_at = parse_created_at(created_at_raw)
        if not created_at:
            continue
        if created_at < cutoff_dt:
            counters["pre_clob_markets"] += 1
            continue
        try:
            tokens = json.loads(tokens_json)
        except json.JSONDecodeError:
            continue
        if not isinstance(tokens, list):
            continue
        created_ts = int(created_at.timestamp())
        closed = bool(is_closed)
        for token_id in tokens:
            token_plan, skip_key, invalid = build_single_token_plan(
                token_id=str(token_id),
                market_id=market_id,
                closed=closed,
                created_ts=created_ts,
                latest_timestamps=latest_timestamps,
                fully_checked_tokens=fully_checked_tokens,
                persisted_skips=persisted_skips,
                seen_tokens=seen_tokens,
                now_ts=now_ts,
                fidelity=fidelity,
                force=force,
                rebuild_minutely=rebuild_minutely,
                overlap_seconds=overlap_seconds,
                recent_seconds=recent_seconds,
                empty_run_streak=0,
                empty_token_skip_budgets=empty_token_skip_budgets,
                empty_token_skip_runs=empty_token_skip_runs,
            )
            if skip_key:
                counters[skip_key] += 1
            if invalid:
                invalid_tokens.append(invalid)
            if token_plan:
                plans.append(token_plan)
    return plans, invalid_tokens, counters


def iter_token_plans_paged(
    *,
    now_ts: int,
    clob_cutoff_date: str,
    fidelity: int,
    force: bool,
    rebuild_minutely: bool,
    overlap_minutes: int,
    skip_recent_minutes: int,
    market_page_size: int,
    reconcile_ledger: bool = False,
    short_range_first: bool = True,
    market_scope: str = "all",
    ended_market_grace_days: int | None = None,
    min_volume: float | None = None,
    max_volume: float | None = None,
    minutely_backfill_days: int = 0,
    empty_token_skip_budgets: Optional[Dict[str, int]] = None,
    empty_token_skip_runs: int = DEFAULT_EMPTY_TOKEN_SKIP_RUNS,
    on_invalid_tokens_batch: Callable[[List[Tuple[str, str]]], None] | None = None,
    token_id_allowlist: set[str] | None = None,
    token_id_denylist: set[str] | None = None,
    iter_due_market_tokens_fn: Callable[..., object] = iter_due_market_tokens,
    iter_markets_with_tokens_fn: Callable[..., object] = iter_markets_with_tokens,
    get_token_sync_snapshot_fn: Callable[..., object] = get_token_sync_snapshot,
    count_due_market_token_exclusions_fn: Callable[..., object] | None = (
        count_due_market_token_exclusions
    ),
    token_sync_scheduler_state_cls: type = TokenSyncSchedulerState,
):
    cutoff_dt = parse_cutoff_date(clob_cutoff_date)
    overlap_seconds = max(0, int(overlap_minutes * 60))
    recent_seconds = max(0, int(skip_recent_minutes * 60))
    minutely_backfill = int(minutely_backfill_days) > 0
    minutely_backfill_floor_ts = (
        now_ts - int(minutely_backfill_days) * 86400 if minutely_backfill else None
    )
    due_only = not force and not rebuild_minutely and not minutely_backfill
    planning_state = PlanningState()
    invalid_tokens: Dict[str, str] = {}
    seen_tokens: set[str] = set()
    empty_token_skip_budgets = empty_token_skip_budgets or {}
    cutoff_created_at = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")
    effective_ended_grace = ended_market_grace_days if due_only else None
    if due_only and count_due_market_token_exclusions_fn is not None:
        exclusion_counts = count_due_market_token_exclusions_fn(
            cutoff_created_at=cutoff_created_at,
            market_scope=market_scope,
            ended_market_grace_days=effective_ended_grace,
            min_volume=min_volume,
            max_volume=max_volume,
        )
        planning_state.scope_skip += int(exclusion_counts.get("scope_skip", 0) or 0)
        planning_state.ended_market_skip += int(
            exclusion_counts.get("ended_market_skip", 0) or 0
        )
    row_pages = (
        iter_due_market_tokens_fn(
            page_size=market_page_size,
            cutoff_created_at=cutoff_created_at,
            market_scope=market_scope,
            ended_market_grace_days=effective_ended_grace,
            min_volume=min_volume,
            max_volume=max_volume,
        )
        if due_only
        else iter_markets_with_tokens_fn(
            page_size=market_page_size,
            cutoff_created_at=cutoff_created_at,
            json_array_only=True,
            market_scope=market_scope,
            ended_market_grace_days=None,
            min_volume=min_volume,
            max_volume=max_volume,
        )
    )
    for page_rows in row_pages:
        prepared_rows: List[Tuple[str, bool, int, List[str]]] = []
        page_token_ids: set[str] = set()
        page_invalid_tokens: Dict[str, str] = {}
        page_plans: List[TokenPlan] = []
        if due_only:
            for market_id, token_id, created_at_raw, is_closed in page_rows:
                created_at = parse_created_at(created_at_raw)
                if not created_at:
                    continue
                if created_at < cutoff_dt:
                    planning_state.pre_clob_markets += 1
                    continue
                clean_token = str(token_id).strip()
                if not clean_token:
                    continue
                created_ts = int(created_at.timestamp())
                prepared_rows.append(
                    (market_id, bool(is_closed), created_ts, [clean_token])
                )
                page_token_ids.add(clean_token)
        else:
            for market_id, tokens_json, created_at_raw, is_closed in page_rows:
                created_at = parse_created_at(created_at_raw)
                if not created_at:
                    continue
                if created_at < cutoff_dt:
                    planning_state.pre_clob_markets += 1
                    continue
                try:
                    tokens = json.loads(tokens_json)
                except json.JSONDecodeError:
                    continue
                if not isinstance(tokens, list) or not tokens:
                    continue
                clean_tokens = [
                    str(token_id) for token_id in tokens if token_id is not None
                ]
                if not clean_tokens:
                    continue
                created_ts = int(created_at.timestamp())
                prepared_rows.append(
                    (market_id, bool(is_closed), created_ts, clean_tokens)
                )
                page_token_ids.update(clean_tokens)
        if not prepared_rows:
            continue
        token_ids = list(page_token_ids)
        snapshot = (
            get_token_sync_snapshot_fn(
                token_ids,
                reconcile_with_history=True,
                repair_ledger=True,
                include_scheduler_state=True,
            )
            if reconcile_ledger
            else get_token_sync_snapshot_fn(token_ids, include_scheduler_state=True)
        )
        if len(snapshot) == 4:
            (
                latest_timestamps,
                fully_checked_tokens,
                persisted_skips,
                scheduler_states,
            ) = snapshot
        else:
            latest_timestamps, fully_checked_tokens, persisted_skips = snapshot
            scheduler_states = {}
        for market_id, closed, created_ts, page_tokens in prepared_rows:
            for token_id in page_tokens:
                scheduler_state = scheduler_states.get(
                    token_id, token_sync_scheduler_state_cls()
                )
                token_plan, skip_key, invalid = build_single_token_plan(
                    token_id=str(token_id),
                    market_id=market_id,
                    closed=closed,
                    created_ts=created_ts,
                    latest_timestamps=latest_timestamps,
                    fully_checked_tokens=fully_checked_tokens,
                    persisted_skips=persisted_skips,
                    seen_tokens=seen_tokens,
                    now_ts=now_ts,
                    fidelity=fidelity,
                    force=force,
                    rebuild_minutely=rebuild_minutely,
                    overlap_seconds=overlap_seconds,
                    recent_seconds=recent_seconds,
                    minutely_backfill_floor_ts=minutely_backfill_floor_ts,
                    empty_run_streak=scheduler_state.empty_run_streak,
                    empty_token_skip_budgets=empty_token_skip_budgets,
                    empty_token_skip_runs=empty_token_skip_runs,
                )
                if skip_key:
                    setattr(
                        planning_state, skip_key, getattr(planning_state, skip_key) + 1
                    )
                if invalid:
                    token_id_invalid, reason = invalid
                    invalid_tokens[token_id_invalid] = reason
                    page_invalid_tokens[token_id_invalid] = reason
                if token_plan:
                    if (
                        token_id_allowlist is not None
                        and token_plan.token_id not in token_id_allowlist
                    ):
                        planning_state.scope_skip += 1
                        continue
                    if (
                        token_id_denylist is not None
                        and token_plan.token_id in token_id_denylist
                    ):
                        planning_state.scope_skip += 1
                        continue
                    planning_state.plans += 1
                    page_plans.append(token_plan)
        if short_range_first and page_plans:
            page_plans.sort(
                key=lambda plan: (plan.end_ts - plan.start_ts, plan.created_at_ts)
            )
        for token_plan in page_plans:
            yield token_plan
        if on_invalid_tokens_batch and page_invalid_tokens:
            on_invalid_tokens_batch(list(page_invalid_tokens.items()))
    return planning_state, invalid_tokens


__all__ = [
    "build_single_token_plan",
    "build_token_plans",
    "iter_token_plans_paged",
    "parse_created_at",
    "parse_cutoff_date",
]
