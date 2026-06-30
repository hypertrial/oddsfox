from typing import Dict, List, Literal, Set, Tuple, overload

from oddsfox.storage.duckdb import odds as odds_barrel
from oddsfox.storage.duckdb.connection import ensure_duck_db, get_connection
from oddsfox.storage.duckdb.odds._common import (
    _TAB_ODDS_HISTORY,
    _TAB_TOKEN_SYNC_LEDGER,
    _TAB_TOKEN_SYNC_SKIPS,
    TokenSyncSchedulerState,
    TokenSyncSnapshot,
    TokenSyncSnapshotWithScheduler,
    _chunked,
)
from oddsfox.storage.duckdb.odds.odds_ledger import (
    upsert_ledger_last_sync_batch,
)


@overload
def get_token_sync_snapshot(  # pragma: no cover
    token_ids: List[str],
    *,
    reconcile_with_history: bool = False,
    repair_ledger: bool = False,
    include_scheduler_state: Literal[False] = False,
) -> TokenSyncSnapshot: ...


@overload
def get_token_sync_snapshot(  # pragma: no cover
    token_ids: List[str],
    *,
    reconcile_with_history: bool = False,
    repair_ledger: bool = False,
    include_scheduler_state: Literal[True],
) -> TokenSyncSnapshotWithScheduler: ...


def get_token_sync_snapshot(
    token_ids: List[str],
    *,
    reconcile_with_history: bool = False,
    repair_ledger: bool = False,
    include_scheduler_state: bool = False,
) -> TokenSyncSnapshot | TokenSyncSnapshotWithScheduler:
    """
    Fetch sync state for a bounded token set.

    Returns:
      latest_timestamps: ledger cursor when available, otherwise history max(timestamp)
      fully_checked_tokens: tokens with fully_checked = TRUE
      skipped_tokens: tokens in token_sync_skips
    """
    if not token_ids:
        return {}, set(), {}

    ensure_duck_db()
    latest_timestamps: Dict[str, int] = {}
    fully_checked_tokens: Set[str] = set()
    skipped_tokens: Dict[str, str] = {}
    scheduler_states: Dict[str, TokenSyncSchedulerState] = {}

    with get_connection() as conn:
        conn.execute(
            """
            CREATE TEMPORARY TABLE IF NOT EXISTS _token_snapshot_input (
                clobTokenId TEXT
            )
            """
        )
        for token_chunk in _chunked(token_ids, odds_barrel._TOKEN_STATE_CHUNK_SIZE):
            conn.execute("DELETE FROM _token_snapshot_input")
            conn.executemany(
                "INSERT INTO _token_snapshot_input (clobTokenId) VALUES (?)",
                [(token_id,) for token_id in token_chunk],
            )

            rows = conn.execute(
                f"""
                SELECT
                    t.clobTokenId,
                    l.last_sync_timestamp AS latest_ts,
                    COALESCE(l.fully_checked, FALSE) AS fully_checked,
                    l.last_checked_at,
                    l.next_check_at,
                    COALESCE(l.empty_run_streak, 0) AS empty_run_streak,
                    s.reason
                FROM _token_snapshot_input t
                LEFT JOIN {_TAB_TOKEN_SYNC_LEDGER} l ON l.clobTokenId = t.clobTokenId
                LEFT JOIN {_TAB_TOKEN_SYNC_SKIPS} s ON s.clobTokenId = t.clobTokenId
                """
            ).fetchall()

            missing_for_history: List[str] = []
            ledger_tokens: List[str] = []
            for (
                token_id,
                latest_ts,
                fully_checked,
                last_checked_at,
                next_check_at,
                empty_run_streak,
                reason,
            ) in rows:
                if latest_ts is not None:
                    latest_timestamps[token_id] = int(latest_ts)
                    ledger_tokens.append(token_id)
                else:
                    missing_for_history.append(token_id)
                if bool(fully_checked):
                    fully_checked_tokens.add(token_id)
                if reason:
                    skipped_tokens[token_id] = reason
                scheduler_states[token_id] = TokenSyncSchedulerState(
                    last_checked_at=last_checked_at,
                    next_check_at=next_check_at,
                    empty_run_streak=int(empty_run_streak or 0),
                )

            if reconcile_with_history and ledger_tokens:
                conn.execute(
                    """
                    CREATE TEMPORARY TABLE IF NOT EXISTS _token_snapshot_ledger (
                        clobTokenId TEXT
                    )
                    """
                )
                conn.execute("DELETE FROM _token_snapshot_ledger")
                conn.executemany(
                    "INSERT INTO _token_snapshot_ledger (clobTokenId) VALUES (?)",
                    [(token_id,) for token_id in ledger_tokens],
                )
                history_rows = conn.execute(
                    f"""
                    SELECT clobTokenId, MAX(timestamp) AS max_history_ts
                    FROM {_TAB_ODDS_HISTORY}
                    WHERE clobTokenId IN (SELECT clobTokenId FROM _token_snapshot_ledger)
                    GROUP BY clobTokenId
                    """
                ).fetchall()

                ledger_repairs: List[Tuple[str, int]] = []
                for token_id, history_ts in history_rows:
                    history_ts = int(history_ts)
                    ledger_ts = latest_timestamps.get(token_id)
                    if ledger_ts is None or history_ts <= int(ledger_ts):
                        continue
                    latest_timestamps[token_id] = history_ts
                    if repair_ledger:
                        ledger_repairs.append((token_id, history_ts))

                if ledger_repairs:
                    upsert_ledger_last_sync_batch(ledger_repairs, conn)

            if missing_for_history:
                conn.execute(
                    """
                    CREATE TEMPORARY TABLE IF NOT EXISTS _token_snapshot_missing (
                        clobTokenId TEXT
                    )
                    """
                )
                conn.execute("DELETE FROM _token_snapshot_missing")
                conn.executemany(
                    "INSERT INTO _token_snapshot_missing (clobTokenId) VALUES (?)",
                    [(token_id,) for token_id in missing_for_history],
                )
                history_rows = conn.execute(
                    f"""
                    SELECT clobTokenId, MAX(timestamp) AS max_history_ts
                    FROM {_TAB_ODDS_HISTORY}
                    WHERE clobTokenId IN (SELECT clobTokenId FROM _token_snapshot_missing)
                    GROUP BY clobTokenId
                    """
                ).fetchall()
                for token_id, latest_ts in history_rows:
                    latest_timestamps[token_id] = int(latest_ts)

    if include_scheduler_state:
        return latest_timestamps, fully_checked_tokens, skipped_tokens, scheduler_states
    return latest_timestamps, fully_checked_tokens, skipped_tokens
