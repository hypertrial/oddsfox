from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable, List, Tuple

from oddsfox.storage.duckdb.connection import (
    ensure_duck_db,
    get_connection,
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)
from oddsfox.storage.duckdb.dlt_batch import load_market_tokens_stage

logger = logging.getLogger(__name__)

_TAB_MARKETS = polymarket_raw_tbl("markets")
_TAB_MARKET_TOKENS = polymarket_raw_tbl("market_tokens")
_TAB_MARKET_METADATA_UNRESOLVED = polymarket_ops_tbl("market_metadata_unresolved")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _persist_market_tokens(conn, token_data: Iterable[Tuple]) -> None:
    token_data = list(token_data)
    if not token_data:
        return
    now = _utc_now()
    token_rows = (
        {"market_id": mid, "clobTokenIds": toks, "updated_at": now}
        for mid, toks in token_data
    )
    load_market_tokens_stage(list(token_rows), conn)


def save_market_tokens_batch(token_data: Iterable[Tuple]) -> None:
    """Persist CLOB token mappings without touching polymarket_raw.markets."""
    token_data = list(token_data)
    if not token_data:
        return
    ensure_duck_db()
    with get_connection() as conn:
        _persist_market_tokens(conn, token_data)


def delete_orphan_market_tokens() -> int:
    """Remove raw ``market_tokens`` rows with no parent row in ``markets`` (referential repair)."""
    ensure_duck_db()
    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            n = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {_TAB_MARKET_TOKENS} mt
                WHERE NOT EXISTS (SELECT 1 FROM {_TAB_MARKETS} m WHERE m.id = mt.market_id)
                """
            ).fetchone()[0]
            n = int(n)
            if n:
                conn.execute(
                    f"""
                    DELETE FROM {_TAB_MARKET_TOKENS} mt
                    WHERE NOT EXISTS (SELECT 1 FROM {_TAB_MARKETS} m WHERE m.id = mt.market_id)
                    """
                )
                logger.info(
                    "Removed %s market_tokens row(s) with no matching markets.id",
                    n,
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        return n


def save_tokens_batch(token_data: List[Tuple[str, str]]):
    if not token_data:
        return
    ensure_duck_db()
    mids = list({mid for mid, _ in token_data})
    placeholders = ",".join("?" * len(mids))
    with get_connection() as conn:
        valid = {
            r[0]
            for r in conn.execute(
                f"SELECT id FROM {_TAB_MARKETS} WHERE id IN ({placeholders})",
                mids,
            ).fetchall()
        }
        filtered = [(mid, toks) for mid, toks in token_data if mid in valid]
        dropped = len(token_data) - len(filtered)
        if dropped:
            logger.warning(
                "save_tokens_batch: skipping %s row(s) whose market_id is not in markets",
                dropped,
            )
        if not filtered:
            return
        now = _utc_now()
        rows = [
            {"market_id": mid, "clobTokenIds": toks, "updated_at": now}
            for mid, toks in filtered
        ]
        load_market_tokens_stage(rows, conn)


def save_slugs_batch(slug_data: List[Tuple[str, str]]):
    if not slug_data:
        return
    ensure_duck_db()
    with get_connection() as conn:
        conn.executemany(f"UPDATE {_TAB_MARKETS} SET slug = ? WHERE id = ?", slug_data)


def save_event_slugs_batch(event_slug_data: List[Tuple[str, str]]):
    if not event_slug_data:
        return
    ensure_duck_db()
    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            conn.executemany(
                f"UPDATE {_TAB_MARKETS} SET event_slug = ? WHERE id = ?",
                event_slug_data,
            )
            conn.executemany(
                f"""
                DELETE FROM {_TAB_MARKET_METADATA_UNRESOLVED}
                WHERE market_id = ? AND field_name = 'event_slug'
                """,
                [(market_id,) for _, market_id in event_slug_data],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


def save_end_dates_batch(end_date_data: List[Tuple[str, str]]):
    if not end_date_data:
        return
    ensure_duck_db()
    with get_connection() as conn:
        conn.executemany(
            f"UPDATE {_TAB_MARKETS} SET end_date = ? WHERE id = ?", end_date_data
        )


def mark_market_metadata_unresolved(
    rows: List[Tuple[str, str, str]],
    *,
    retry_after_hours: int = 168,
) -> None:
    if not rows:
        return
    ensure_duck_db()
    retry_hours = max(1, int(retry_after_hours))
    with get_connection() as conn:
        conn.executemany(
            f"""
            INSERT INTO {_TAB_MARKET_METADATA_UNRESOLVED} (
                market_id,
                field_name,
                reason,
                attempts,
                last_checked_at,
                next_retry_at
            )
            VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + (? * INTERVAL '1 hour'))
            ON CONFLICT(market_id, field_name) DO UPDATE SET
                reason = excluded.reason,
                attempts = COALESCE(attempts, 0) + 1,
                last_checked_at = excluded.last_checked_at,
                next_retry_at = excluded.next_retry_at
            """,
            [
                (market_id, field_name, reason, retry_hours)
                for market_id, field_name, reason in rows
            ],
        )


__all__ = [
    "delete_orphan_market_tokens",
    "mark_market_metadata_unresolved",
    "save_end_dates_batch",
    "save_event_slugs_batch",
    "save_market_tokens_batch",
    "save_slugs_batch",
    "save_tokens_batch",
]
