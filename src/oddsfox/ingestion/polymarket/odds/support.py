from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)

DEFAULT_SKIP_RECENT_MINUTES = 15
DEFAULT_OVERLAP_MINUTES = 5
DEFAULT_WINDOW_HOURS = 168
DEFAULT_WRITER_CHUNK_ROWS = 50_000
DEFAULT_WRITER_FLUSH_ROWS = 250_000
DEFAULT_MIN_SPLIT_WINDOW_MINUTES = 30
DEFAULT_MARKET_PAGE_SIZE = 2_000
DEFAULT_EMPTY_TOKEN_SKIP_RUNS = 2
DEFAULT_ROUTINE_INTERVAL_HOURS = 6
DEFAULT_EMPTY_RETRY_BASE_HOURS = 24
DEFAULT_EMPTY_RETRY_MAX_HOURS = 168
DEFAULT_ERROR_RETRY_MINUTES = 30
DEFAULT_TRANSIENT_RETRIES = 2
DEFAULT_TRANSIENT_BACKOFF_SECONDS = 0.25
DEFAULT_AUTOTUNE_WINDOW_REQUESTS = 200
DEFAULT_AUTOTUNE_429_THRESHOLD = 0.03
DEFAULT_AUTOTUNE_ERROR_THRESHOLD = 0.01
MAX_WORKERS_CAP = 128
MAX_INFLIGHT_CAP = 4_096
MAX_FLUSH_ROWS_CAP = 2_000_000


@dataclass(frozen=True)
class TokenPlan:
    token_id: str
    market_id: str
    is_closed: bool
    created_at_ts: int
    start_ts: int
    end_ts: int
    fidelity: int
    empty_run_streak: int = 0


@dataclass
class WriterBuffers:
    odds_map: Dict[Tuple[str, int], float]
    state_buffer: List[
        Tuple[str, int | None, datetime | None, datetime | None, int | None, bool]
    ]
    skip_buffer: List[Tuple[str, str]]
    dirty_daily_keys: set[Tuple[str, date]] = field(default_factory=set)


@dataclass(frozen=True)
class InflightTokenFuture:
    plan: TokenPlan
    submitted_at: float


@dataclass
class PlanningState:
    plans: int = 0
    pre_clob_markets: int = 0
    invalid_token: int = 0
    closed_done: int = 0
    persisted_skip: int = 0
    recent_skip: int = 0
    empty_cache_skip: int = 0
    already_current: int = 0
    dup_token: int = 0
    scope_skip: int = 0
    ended_market_skip: int = 0


def planning_state_to_dict(planning_state: PlanningState) -> Dict[str, int]:
    return {
        "plans": planning_state.plans,
        "pre_clob_markets": planning_state.pre_clob_markets,
        "invalid_token": planning_state.invalid_token,
        "closed_done": planning_state.closed_done,
        "persisted_skip": planning_state.persisted_skip,
        "recent_skip": planning_state.recent_skip,
        "empty_cache_skip": planning_state.empty_cache_skip,
        "already_current": planning_state.already_current,
        "dup_token": planning_state.dup_token,
        "scope_skip": planning_state.scope_skip,
        "ended_market_skip": planning_state.ended_market_skip,
    }


def log_planning_state(planning_state: PlanningState) -> None:
    logger.info(
        "Token planning complete: %s plans | pre_clob_markets=%s invalid=%s closed_done=%s persisted_skip=%s recent_skip=%s empty_cache_skip=%s already_current=%s dup=%s scope_skip=%s ended_market_skip=%s",
        planning_state.plans,
        planning_state.pre_clob_markets,
        planning_state.invalid_token,
        planning_state.closed_done,
        planning_state.persisted_skip,
        planning_state.recent_skip,
        planning_state.empty_cache_skip,
        planning_state.already_current,
        planning_state.dup_token,
        planning_state.scope_skip,
        planning_state.ended_market_skip,
    )


def ratio_or_none(numerator: int | None, denominator: int | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return round(float(numerator) / float(denominator), 4)


def build_planning_context(
    raw_snapshot: Dict[str, Any],
    planning_state: PlanningState,
    *,
    invalid_tokens: int,
) -> Dict[str, Any]:
    market_tokens = raw_snapshot.get("market_tokens_distinct_tokens")
    history_tokens = raw_snapshot.get("odds_history_distinct_tokens")
    daily_tokens = raw_snapshot.get("token_odds_daily_distinct_tokens")
    ledger_tokens = raw_snapshot.get("ledger_distinct_tokens")
    fully_checked_tokens = raw_snapshot.get("ledger_fully_checked_tokens")
    skipped_tokens = raw_snapshot.get("token_sync_skips_distinct_tokens")

    return {
        "market_tokens_distinct_tokens": market_tokens,
        "odds_history_distinct_tokens": history_tokens,
        "token_odds_daily_distinct_tokens": daily_tokens,
        "ledger_distinct_tokens": ledger_tokens,
        "ledger_fully_checked_tokens": fully_checked_tokens,
        "token_sync_skips_distinct_tokens": skipped_tokens,
        "market_tokens_without_history": raw_snapshot.get(
            "market_tokens_without_history"
        ),
        "history_tokens_without_market_tokens": raw_snapshot.get(
            "history_tokens_without_market_tokens"
        ),
        "token_sync_skips_by_reason": raw_snapshot.get("token_sync_skips_by_reason"),
        "planned_tokens": planning_state.plans,
        "invalid_tokens": invalid_tokens,
        "planned_vs_market_tokens": ratio_or_none(planning_state.plans, market_tokens),
        "history_coverage_vs_market_tokens": ratio_or_none(
            history_tokens, market_tokens
        ),
        "daily_coverage_vs_market_tokens": ratio_or_none(daily_tokens, market_tokens),
        "ledger_coverage_vs_market_tokens": ratio_or_none(ledger_tokens, market_tokens),
        "fully_checked_vs_market_tokens": ratio_or_none(
            fully_checked_tokens, market_tokens
        ),
    }


def build_inflight_future_diagnostics(
    futures: Dict[object, InflightTokenFuture],
    *,
    now: float | None = None,
    max_items: int = 3,
) -> Dict[str, Any]:
    import time

    if not futures:
        return {"oldest_inflight_seconds": 0.0, "oldest_inflight": []}

    observed_at = time.monotonic() if now is None else float(now)
    oldest = sorted(
        futures.values(),
        key=lambda entry: entry.submitted_at,
    )[: max(1, max_items)]
    oldest_items = [
        {
            "token_id_prefix": entry.plan.token_id[:24],
            "market_id": entry.plan.market_id,
            "inflight_seconds": round(max(0.0, observed_at - entry.submitted_at), 3),
        }
        for entry in oldest
    ]
    return {
        "oldest_inflight_seconds": oldest_items[0]["inflight_seconds"],
        "oldest_inflight": oldest_items,
    }


def log_planning_context(planning_context: Dict[str, Any]) -> None:
    logger.info(
        "Token planning context: market_tokens=%s planned=%s odds_history=%s daily=%s ledger=%s fully_checked=%s skips=%s invalid=%s planned_vs_market=%s history_coverage=%s",
        planning_context.get("market_tokens_distinct_tokens"),
        planning_context.get("planned_tokens"),
        planning_context.get("odds_history_distinct_tokens"),
        planning_context.get("token_odds_daily_distinct_tokens"),
        planning_context.get("ledger_distinct_tokens"),
        planning_context.get("ledger_fully_checked_tokens"),
        planning_context.get("token_sync_skips_distinct_tokens"),
        planning_context.get("invalid_tokens"),
        planning_context.get("planned_vs_market_tokens"),
        planning_context.get("history_coverage_vs_market_tokens"),
    )


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
    "PlanningState",
    "TokenPlan",
    "WriterBuffers",
    "build_inflight_future_diagnostics",
    "build_planning_context",
    "log_planning_context",
    "log_planning_state",
    "planning_state_to_dict",
]
