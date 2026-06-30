from __future__ import annotations

from datetime import datetime, timedelta, timezone
from queue import Queue
from typing import Callable, Dict, List, Optional, Tuple

from oddsfox.ingestion.polymarket.odds.fetch import (
    BadRequestError,
    PermanentAPIError,
    fetch_token_history_with_retry,
)
from oddsfox.ingestion.polymarket.odds.support import (
    DEFAULT_EMPTY_RETRY_BASE_HOURS,
    DEFAULT_EMPTY_RETRY_MAX_HOURS,
    DEFAULT_ERROR_RETRY_MINUTES,
    DEFAULT_ROUTINE_INTERVAL_HOURS,
    DEFAULT_TRANSIENT_BACKOFF_SECONDS,
    DEFAULT_TRANSIENT_RETRIES,
    InflightTokenFuture,
    TokenPlan,
)
from oddsfox.resources.http import RateLimiter


def iter_windows(start_ts: int, end_ts: int, window_seconds: int):
    cursor = start_ts
    while cursor < end_ts:
        next_ts = min(end_ts, cursor + window_seconds)
        yield cursor, next_ts
        cursor = next_ts


def default_rate_limiter_factory(rps: int | None):
    if not rps:
        return None
    return RateLimiter(rps)


def checked_at_from_plan(plan: TokenPlan) -> datetime:
    return datetime.fromtimestamp(int(plan.end_ts), tz=timezone.utc)


def empty_retry_next_check(
    checked_at: datetime,
    *,
    empty_run_streak: int,
    base_seconds: int,
    max_seconds: int,
) -> datetime:
    multiplier = max(0, int(empty_run_streak) - 1)
    delay_seconds = max(0, int(base_seconds)) * (2**multiplier)
    if max_seconds > 0:
        delay_seconds = min(delay_seconds, max_seconds)
    return checked_at + timedelta(seconds=delay_seconds)


def is_interval_too_long_error(exc: BadRequestError) -> bool:
    body = getattr(exc, "body", "") or ""
    message = str(exc)
    return "interval is too long" in f"{body} {message}".lower()


def fetch_window_with_auto_split(
    client,
    token_id: str,
    start_ts: int,
    end_ts: int,
    fidelity: int,
    min_window_seconds: int,
    transient_retries: int = DEFAULT_TRANSIENT_RETRIES,
    transient_backoff_seconds: float = DEFAULT_TRANSIENT_BACKOFF_SECONDS,
    status_hook: Callable[[int], None] | None = None,
    fetch_token_history_fn: Callable[..., object] = fetch_token_history_with_retry,
) -> Optional[List[Tuple[str, int, float]]]:
    stack: List[Tuple[int, int]] = [(start_ts, end_ts)]
    out: List[Tuple[str, int, float]] = []
    while stack:
        s_ts, e_ts = stack.pop()
        if s_ts >= e_ts:
            continue
        try:
            chunk = fetch_token_history_fn(
                client,
                token_id,
                start_ts=s_ts,
                end_ts=e_ts,
                fidelity=fidelity,
                now_ts=e_ts,
                transient_retries=transient_retries,
                transient_backoff_base_seconds=transient_backoff_seconds,
                status_hook=status_hook,
            )
        except BadRequestError as exc:
            span = e_ts - s_ts
            if is_interval_too_long_error(exc) and span > min_window_seconds:
                mid = s_ts + (span // 2)
                stack.append((mid, e_ts))
                stack.append((s_ts, mid))
                continue
            raise
        if chunk is None:
            return None
        out.extend(chunk)
    return out


def sync_token_plan(
    plan: TokenPlan,
    client,
    write_queue: Queue,
    window_seconds: int,
    writer_chunk_rows: int,
    min_split_window_seconds: int,
    routine_interval_seconds: int = DEFAULT_ROUTINE_INTERVAL_HOURS * 3600,
    empty_retry_base_seconds: int = DEFAULT_EMPTY_RETRY_BASE_HOURS * 3600,
    empty_retry_max_seconds: int = DEFAULT_EMPTY_RETRY_MAX_HOURS * 3600,
    error_retry_seconds: int = DEFAULT_ERROR_RETRY_MINUTES * 60,
    transient_retries: int = DEFAULT_TRANSIENT_RETRIES,
    transient_backoff_seconds: float = DEFAULT_TRANSIENT_BACKOFF_SECONDS,
    status_hook: Callable[[int], None] | None = None,
    fetch_window_fn: Callable[..., object] = fetch_window_with_auto_split,
) -> Dict[str, int | bool]:
    client = client() if callable(client) else client
    token_id = plan.token_id
    checked_at = checked_at_from_plan(plan)
    rows_fetched = 0
    windows_processed = 0
    had_transient_error = False
    max_seen_ts = plan.start_ts
    max_contiguous_seen_ts = plan.start_ts
    contiguous_windows_ok = True
    contiguous_checked_until_ts = plan.start_ts
    odds_buffer: List[Tuple[str, int, float]] = []
    for window_start, window_end in iter_windows(
        plan.start_ts, plan.end_ts, window_seconds
    ):
        windows_processed += 1
        try:
            chunk = fetch_window_fn(
                client,
                token_id,
                window_start,
                window_end,
                plan.fidelity,
                min_split_window_seconds,
                transient_retries,
                transient_backoff_seconds,
                status_hook,
            )
        except (BadRequestError, PermanentAPIError) as exc:
            reason = str(exc)
            write_queue.put(("skipped_tokens", [(token_id, reason)]))
            write_queue.put(
                ("token_state", [(token_id, plan.end_ts, checked_at, None, 0, False)])
            )
            return {
                "rows": rows_fetched,
                "windows": windows_processed,
                "empty": rows_fetched == 0,
                "error": 1,
                "permanent_error": 1,
                "fully_checked": False,
            }
        if chunk is None:
            had_transient_error = True
            contiguous_windows_ok = False
            continue
        if contiguous_windows_ok:
            contiguous_checked_until_ts = window_end
        if not chunk:
            continue
        rows_fetched += len(chunk)
        window_max_ts = max(ts for _, ts, _ in chunk)
        max_seen_ts = max(max_seen_ts, window_max_ts)
        if contiguous_windows_ok:
            max_contiguous_seen_ts = max(max_contiguous_seen_ts, window_max_ts)
        odds_buffer.extend(chunk)
        if len(odds_buffer) >= writer_chunk_rows:
            write_queue.put(("odds", odds_buffer))
            odds_buffer = []
    if odds_buffer:
        write_queue.put(("odds", odds_buffer))
    if had_transient_error:
        if max_contiguous_seen_ts > plan.start_ts:
            cursor_ts: int | None = max_contiguous_seen_ts
        elif rows_fetched == 0 and contiguous_checked_until_ts > plan.start_ts:
            cursor_ts = contiguous_checked_until_ts
        else:
            cursor_ts = None
    elif rows_fetched > 0:
        cursor_ts = max_seen_ts
    else:
        cursor_ts = plan.end_ts
    fully_checked = bool(plan.is_closed and not had_transient_error)
    if had_transient_error:
        next_check_at = checked_at + timedelta(seconds=max(0, error_retry_seconds))
        empty_run_streak = 0
    elif rows_fetched > 0:
        next_check_at = (
            None
            if fully_checked
            else checked_at + timedelta(seconds=max(0, routine_interval_seconds))
        )
        empty_run_streak = 0
    else:
        empty_run_streak = int(plan.empty_run_streak) + 1
        next_check_at = (
            None
            if fully_checked
            else empty_retry_next_check(
                checked_at,
                empty_run_streak=empty_run_streak,
                base_seconds=empty_retry_base_seconds,
                max_seconds=empty_retry_max_seconds,
            )
        )
    write_queue.put(
        (
            "token_state",
            [
                (
                    token_id,
                    cursor_ts,
                    checked_at,
                    next_check_at,
                    empty_run_streak,
                    fully_checked,
                )
            ],
        )
    )
    return {
        "rows": rows_fetched,
        "windows": windows_processed,
        "empty": rows_fetched == 0,
        "error": 1 if had_transient_error else 0,
        "permanent_error": 0,
        "fully_checked": fully_checked,
    }


__all__ = [
    "InflightTokenFuture",
    "checked_at_from_plan",
    "default_rate_limiter_factory",
    "empty_retry_next_check",
    "fetch_window_with_auto_split",
    "iter_windows",
    "sync_token_plan",
]
