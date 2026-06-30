import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Dict, Iterable, List, Set, Tuple

from oddsfox.storage.duckdb.connection import (
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)

logger = logging.getLogger(__name__)
_TOKEN_STATE_CHUNK_SIZE = 2_000

_TAB_ODDS_HISTORY = polymarket_raw_tbl("odds_history")
_TAB_TOKEN_ODDS_DAILY = polymarket_raw_tbl("token_odds_daily")
_TAB_TOKEN_SYNC_LEDGER = polymarket_ops_tbl("token_sync_ledger")
_TAB_TOKEN_SYNC_SKIPS = polymarket_ops_tbl("token_sync_skips")

TokenSyncSnapshot = Tuple[Dict[str, int], Set[str], Dict[str, str]]
TokenSyncSnapshotWithScheduler = Tuple[
    Dict[str, int],
    Set[str],
    Dict[str, str],
    Dict[str, "TokenSyncSchedulerState"],
]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# Sentinel for monotonic ledger cursor merges when last_sync_timestamp is NULL.
_LEDGER_TS_SENTINEL = "CAST(-9223372036854775808 AS BIGINT)"

_SQL_UPSERT_LEDGER_LAST_SYNC = f"""
INSERT INTO {_TAB_TOKEN_SYNC_LEDGER} (clobTokenId, last_sync_timestamp)
VALUES (?, ?)
ON CONFLICT(clobTokenId) DO UPDATE SET
    last_sync_timestamp = GREATEST(
        COALESCE(token_sync_ledger.last_sync_timestamp, {_LEDGER_TS_SENTINEL}),
        COALESCE(excluded.last_sync_timestamp, {_LEDGER_TS_SENTINEL})
    )
"""

_SQL_UPSERT_LEDGER_STATE = f"""
INSERT INTO {_TAB_TOKEN_SYNC_LEDGER} (
    clobTokenId,
    last_sync_timestamp,
    last_checked_at,
    next_check_at,
    empty_run_streak,
    fully_checked
)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(clobTokenId) DO UPDATE SET
    last_sync_timestamp = GREATEST(
        COALESCE(token_sync_ledger.last_sync_timestamp, {_LEDGER_TS_SENTINEL}),
        COALESCE(excluded.last_sync_timestamp, {_LEDGER_TS_SENTINEL})
    ),
    last_checked_at = COALESCE(excluded.last_checked_at, token_sync_ledger.last_checked_at),
    next_check_at = CASE
        WHEN COALESCE(excluded.fully_checked, FALSE) THEN NULL
        ELSE COALESCE(excluded.next_check_at, token_sync_ledger.next_check_at)
    END,
    empty_run_streak = CASE
        WHEN COALESCE(excluded.fully_checked, FALSE) THEN 0
        ELSE COALESCE(excluded.empty_run_streak, token_sync_ledger.empty_run_streak, 0)
    END,
    fully_checked = COALESCE(token_sync_ledger.fully_checked, FALSE)
        OR COALESCE(excluded.fully_checked, FALSE)
"""

_SQL_UPSERT_TOKEN_SYNC_SKIP = f"""
INSERT INTO {_TAB_TOKEN_SYNC_SKIPS} (clobTokenId, reason)
VALUES (?, ?)
ON CONFLICT(clobTokenId) DO UPDATE SET
    reason = excluded.reason
"""


@dataclass(frozen=True)
class TokenSyncSchedulerState:
    last_checked_at: datetime | None = None
    next_check_at: datetime | None = None
    empty_run_streak: int = 0


def _epoch_to_utc_date(epoch_seconds: int) -> date:
    return datetime.fromtimestamp(int(epoch_seconds), tz=timezone.utc).date()


def _chunked(items: List[str], size: int) -> Iterable[List[str]]:
    if size <= 0:
        raise ValueError("size must be positive")
    for i in range(0, len(items), size):
        yield items[i : i + size]
