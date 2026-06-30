from datetime import datetime
from typing import Dict, List, Set, Tuple

import duckdb

from oddsfox.storage.duckdb.connection import ensure_duck_db, get_connection
from oddsfox.storage.duckdb.odds._common import (
    _SQL_UPSERT_LEDGER_LAST_SYNC,
    _SQL_UPSERT_LEDGER_STATE,
    _SQL_UPSERT_TOKEN_SYNC_SKIP,
    _TAB_ODDS_HISTORY,
    _TAB_TOKEN_SYNC_LEDGER,
    _TAB_TOKEN_SYNC_SKIPS,
    logger,
)


def upsert_ledger_last_sync_batch(
    token_timestamps: List[Tuple[str, int]],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Advance per-token sync cursors without clobbering other ledger columns (e.g. fully_checked)."""
    if not token_timestamps:
        return
    conn.executemany(_SQL_UPSERT_LEDGER_LAST_SYNC, token_timestamps)


def upsert_token_sync_state_batch(
    token_states: List[
        Tuple[
            str,
            int | None,
            datetime | None,
            datetime | None,
            int | None,
            bool,
        ]
    ],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Persist per-token scheduler state without regressing existing ledger progress."""
    if not token_states:
        return
    conn.executemany(_SQL_UPSERT_LEDGER_STATE, token_states)


def upsert_skipped_tokens_batch(
    token_reasons: List[Tuple[str, str]],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Persist or update skip reasons without resetting created_at on existing rows."""
    if not token_reasons:
        return
    conn.executemany(_SQL_UPSERT_TOKEN_SYNC_SKIP, token_reasons)


def get_latest_timestamps() -> Dict[str, int]:
    """
    Get the latest timestamp for each token, checking both data history and sync ledger.
    """
    ensure_duck_db()
    timestamps: Dict[str, int] = {}
    with get_connection() as conn:
        history_rows = conn.execute(
            f"SELECT clobTokenId, MAX(timestamp) FROM {_TAB_ODDS_HISTORY} GROUP BY clobTokenId"
        ).fetchall()
        for token_id, ts in history_rows:
            timestamps[token_id] = int(ts)

        ledger_rows = conn.execute(
            f"SELECT clobTokenId, last_sync_timestamp FROM {_TAB_TOKEN_SYNC_LEDGER}"
        ).fetchall()
        for token_id, ts in ledger_rows:
            if ts is None:
                continue
            ts = int(ts)
            if token_id in timestamps:
                timestamps[token_id] = max(timestamps[token_id], ts)
            else:
                timestamps[token_id] = ts

    logger.debug("DuckDB latest timestamps loaded for %d tokens", len(timestamps))
    return timestamps


def get_tokens_with_data() -> Set[str]:
    """Return token IDs that have odds data points in DuckDB."""
    ensure_duck_db()
    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT DISTINCT clobTokenId FROM {_TAB_ODDS_HISTORY}"
        ).fetchall()
        tokens = {row[0] for row in rows}
    logger.debug("DuckDB tokens with data: %d", len(tokens))
    return tokens


def get_fully_checked_tokens() -> Set[str]:
    """Return token IDs marked as fully checked in DuckDB."""
    ensure_duck_db()
    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT clobTokenId FROM {_TAB_TOKEN_SYNC_LEDGER} WHERE fully_checked = TRUE"
        ).fetchall()
        tokens = {row[0] for row in rows}
    logger.debug("DuckDB fully checked tokens: %d", len(tokens))
    return tokens


def get_skipped_tokens() -> Dict[str, str]:
    """Return tokens that should be skipped along with the persisted reason."""
    ensure_duck_db()
    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT clobTokenId, reason FROM {_TAB_TOKEN_SYNC_SKIPS}"
        ).fetchall()
        return {row[0]: row[1] for row in rows}


def save_skipped_tokens(token_reasons: List[Tuple[str, str]]):
    """Persist tokens that should be skipped (permanent client-side errors)."""
    if not token_reasons:
        return
    ensure_duck_db()
    with get_connection() as conn:
        upsert_skipped_tokens_batch(token_reasons, conn)
    logger.debug("Saved %d skipped tokens to DuckDB", len(token_reasons))


def mark_tokens_fully_checked(token_ids: List[str]):
    """Mark tokens as fully checked (closed markets, no new data expected)."""
    if not token_ids:
        return
    ensure_duck_db()
    with get_connection() as conn:
        conn.executemany(
            f"""
            INSERT INTO {_TAB_TOKEN_SYNC_LEDGER} (clobTokenId, fully_checked)
            VALUES (?, TRUE)
            ON CONFLICT(clobTokenId) DO UPDATE SET fully_checked = TRUE
            """,
            [(token_id,) for token_id in token_ids],
        )
    logger.debug("Marked %d tokens as fully checked in DuckDB", len(token_ids))


def reconcile_token_sync_ledger_from_history() -> Dict[str, int]:
    """
    Reconcile stale/missing ledger cursors from odds_history maxima.

    Returns:
      dict with keys:
        scanned_tokens: number of tokens with any odds history
        repaired_tokens: number of tokens with stale/missing ledger cursors
    """
    ensure_duck_db()
    with get_connection() as conn:
        scanned_row = conn.execute(
            f"SELECT COUNT(DISTINCT clobTokenId) FROM {_TAB_ODDS_HISTORY}"
        ).fetchone()
        scanned_tokens = (
            int(scanned_row[0]) if scanned_row and scanned_row[0] is not None else 0
        )

        repaired_row = conn.execute(
            f"""
            WITH history AS (
                SELECT clobTokenId, MAX(timestamp) AS max_history_ts
                FROM {_TAB_ODDS_HISTORY}
                GROUP BY clobTokenId
            )
            SELECT COUNT(*)
            FROM history h
            LEFT JOIN {_TAB_TOKEN_SYNC_LEDGER} l ON l.clobTokenId = h.clobTokenId
            WHERE l.last_sync_timestamp IS NULL OR h.max_history_ts > l.last_sync_timestamp
            """
        ).fetchone()
        repaired_tokens = (
            int(repaired_row[0]) if repaired_row and repaired_row[0] is not None else 0
        )

        conn.execute(
            f"""
            INSERT INTO {_TAB_TOKEN_SYNC_LEDGER} (clobTokenId, last_sync_timestamp)
            SELECT h.clobTokenId, h.max_history_ts
            FROM (
                SELECT clobTokenId, MAX(timestamp) AS max_history_ts
                FROM {_TAB_ODDS_HISTORY}
                GROUP BY clobTokenId
            ) h
            ON CONFLICT(clobTokenId) DO UPDATE SET
                last_sync_timestamp = GREATEST(
                    COALESCE(token_sync_ledger.last_sync_timestamp, CAST(-9223372036854775808 AS BIGINT)),
                    COALESCE(excluded.last_sync_timestamp, CAST(-9223372036854775808 AS BIGINT))
                )
            """
        )

    logger.debug(
        "Reconciled odds ledger from history: scanned_tokens=%s repaired_tokens=%s",
        scanned_tokens,
        repaired_tokens,
    )
    return {
        "scanned_tokens": scanned_tokens,
        "repaired_tokens": repaired_tokens,
    }


def save_sync_status_batch(token_timestamps: List[Tuple[str, int]]):
    """Update last sync timestamps for a batch of tokens in DuckDB."""
    if not token_timestamps:
        return
    ensure_duck_db()
    with get_connection() as conn:
        upsert_ledger_last_sync_batch(token_timestamps, conn)
    logger.debug("Saved sync status for %d tokens to DuckDB", len(token_timestamps))


def save_token_sync_state_batch(
    token_states: List[
        Tuple[
            str,
            int | None,
            datetime | None,
            datetime | None,
            int | None,
            bool,
        ]
    ],
):
    """Persist per-token scheduler state for routine odds syncing."""
    if not token_states:
        return
    ensure_duck_db()
    with get_connection() as conn:
        upsert_token_sync_state_batch(token_states, conn)
    logger.debug(
        "Saved token scheduler state for %d tokens to DuckDB", len(token_states)
    )
