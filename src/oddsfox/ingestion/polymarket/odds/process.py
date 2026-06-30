"""
Token-level processing helpers for odds ingestion.
"""

import logging
import re
from datetime import datetime, timezone
from threading import Lock
from typing import Dict, List, Optional, Tuple

from oddsfox.ingestion.polymarket.odds.fetch import (
    BadRequestError,
    PermanentAPIError,
    fetch_token_history_with_retry,
)

logger = logging.getLogger(__name__)

MAX_RANGE_SECONDS = 7 * 24 * 3600  # cap explicit start/end window to 7 days
RECENT_MARKET_WINDOW_DAYS = 14  # treat recently created markets as higher fidelity
MIN_TOKEN_LENGTH = 30


def split_time_windows(start_ts: int, end_ts: int, max_span_seconds: int):
    """Yield (start, end) windows no larger than max_span_seconds."""
    cursor = start_ts
    while cursor < end_ts:
        window_end = min(end_ts, cursor + max_span_seconds)
        yield cursor, window_end
        cursor = window_end


def is_probably_clob_token(token_id: str) -> bool:
    """
    Heuristic filter to drop obvious placeholders/no-data tokens.

    Keeps only the high-signal placeholder checks so we still avoid known bad ids
    while allowing short/mixed tokens used in tests and some markets.
    """
    if not token_id:
        return False
    if token_id.startswith(("open_token", "closed_token", "closed_no_data")):
        return False

    # Disallow characters outside a safe alphanumeric/underscore set
    if not re.fullmatch(r"[0-9A-Za-z_]+", token_id):
        return False

    # If token is purely hex, require a reasonable length to avoid placeholders
    if re.fullmatch(r"[0-9a-fA-F]+", token_id):
        return len(token_id) >= MIN_TOKEN_LENGTH

    # Otherwise accept tokens that contain at least a digit or underscore
    # (rejects plain words like "short" while allowing ids like "t1" or "t_recent")
    return any(ch.isdigit() or ch == "_" for ch in token_id)


def process_token(
    token_id: str,
    latest_timestamps: dict,
    fully_checked_tokens: set,
    client,
    stats_lock: Lock,
    stats: dict,
    skip_recent_hours: int = 24,
    fidelity: int = 1440,
    force: bool = False,
    skip_registry: Optional[Dict[str, str]] = None,
    skip_lock: Optional[Lock] = None,
    max_range_seconds: int = MAX_RANGE_SECONDS,
) -> Tuple[Optional[List[Tuple]], Optional[int]]:
    """
    Process a single token and return its records and sync timestamp.
    Thread-safe worker function for parallel processing.
    """
    try:
        # Skip tokens that are fully checked (market closed, no new data expected)
        if token_id in fully_checked_tokens and not force:
            with stats_lock:
                stats["fully_checked"] = stats.get("fully_checked", 0) + 1
            start_ts = latest_timestamps.get(token_id)
            return [], start_ts if start_ts else int(
                datetime.now(timezone.utc).timestamp()
            )

        start_ts = latest_timestamps.get(token_id)
        now_ts = int(datetime.now(timezone.utc).timestamp())

        # Skip tokens that were recently synced (within skip_recent_hours)
        if start_ts and not force:
            hours_since_sync = (now_ts - start_ts) / 3600
            if hours_since_sync < skip_recent_hours:
                with stats_lock:
                    stats["skipped"] = stats.get("skipped", 0) + 1
                return [], start_ts  # Return empty but keep existing timestamp

        def _record_skip(reason: str):
            if skip_registry is not None:
                if skip_lock:
                    with skip_lock:
                        skip_registry[token_id] = reason
                else:
                    skip_registry[token_id] = reason

        # Fetch history with window chunking
        try:
            if start_ts:
                if start_ts >= now_ts:
                    logger.warning(
                        "Skipping token %s: start_ts (%s) >= now (%s)",
                        token_id,
                        start_ts,
                        now_ts,
                    )
                    return [], now_ts
                span = now_ts - start_ts
                if span > max_range_seconds:
                    records: List[Tuple] = []
                    for window_start, window_end in split_time_windows(
                        start_ts, now_ts, max_range_seconds
                    ):
                        chunk = fetch_token_history_with_retry(
                            client,
                            token_id,
                            start_ts=window_start,
                            end_ts=window_end,
                            fidelity=fidelity,
                            now_ts=now_ts,
                        )
                        if chunk is None:
                            return None, None
                        records.extend(chunk)
                else:
                    records = fetch_token_history_with_retry(
                        client,
                        token_id,
                        start_ts=start_ts,
                        end_ts=now_ts,
                        fidelity=fidelity,
                        now_ts=now_ts,
                    )
            else:
                records = fetch_token_history_with_retry(
                    client, token_id, interval="max", fidelity=fidelity, now_ts=now_ts
                )
        except BadRequestError as e:
            logger.error(f"Bad request for token {token_id}: {e}")
            _record_skip(str(e))
            with stats_lock:
                stats["permanent_error"] = stats.get("permanent_error", 0) + 1
            return [], now_ts
        except PermanentAPIError as e:
            logger.error(f"Permanent API error for token {token_id}: {e}")
            _record_skip(str(e))
            with stats_lock:
                stats["permanent_error"] = stats.get("permanent_error", 0) + 1
            return [], now_ts

        # For incremental sync, filter to only include records after start_ts
        if start_ts and records:
            records = [r for r in records if r[1] > start_ts]  # r[1] is timestamp

        if records is None:
            with stats_lock:
                stats["error"] = stats.get("error", 0) + 1
            return None, None

        if records:
            with stats_lock:
                stats["success"] = stats.get("success", 0) + 1
            max_ts = max(r[1] for r in records)
            return records, max_ts

        with stats_lock:
            stats["empty"] = stats.get("empty", 0) + 1
        # No records found. Update sync status to now so we don't check from scratch next time
        now_ts = int(datetime.now(timezone.utc).timestamp())
        return [], now_ts

    except Exception as e:
        logger.error(f"Unexpected error processing token {token_id}: {e}")
        with stats_lock:
            stats["error"] = stats.get("error", 0) + 1
        return None, None
