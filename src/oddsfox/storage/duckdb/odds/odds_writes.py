from typing import Dict, List, Tuple

import duckdb

from oddsfox.storage.duckdb.connection import ensure_duck_db, get_connection
from oddsfox.storage.duckdb.odds._common import (
    _TAB_ODDS_HISTORY,
    _utc_now,
    logger,
)


def save_odds_batch(records: List[Tuple[str, int, float]]):
    """Save a batch of odds history records into DuckDB."""
    if not records:
        return
    ensure_duck_db()
    ingested = _utc_now()
    rows = [(t, ts, p, ingested) for t, ts, p in records]
    with get_connection() as conn:
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO {_TAB_ODDS_HISTORY} (clobTokenId, timestamp, price, ingested_at)
            VALUES (?, ?, ?, ?)
            """,
            rows,
        )
    logger.debug("Saved %d odds records to DuckDB", len(records))


def save_odds_bulk_appender(
    records: List[Tuple[str, int, float]], conn: duckdb.DuckDBPyConnection
):
    """Save odds history records using DuckDB's Appender on an open connection."""
    if not records:
        return
    ingested = _utc_now()
    if hasattr(duckdb, "Appender"):
        appender = duckdb.Appender(conn, _TAB_ODDS_HISTORY)
        try:
            for token_id, timestamp, price in records:
                appender.append([token_id, timestamp, price, ingested])
        finally:
            appender.close()
    else:
        rows = [(t, ts, p, ingested) for t, ts, p in records]
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO {_TAB_ODDS_HISTORY} (clobTokenId, timestamp, price, ingested_at)
            VALUES (?, ?, ?, ?)
            """,
            rows,
        )
    logger.debug("Saved %d odds records to DuckDB", len(records))


def save_odds_bulk_upsert(
    records: List[Tuple[str, int, float]],
    conn: duckdb.DuckDBPyConnection,
    *,
    assume_deduped: bool = False,
):
    """
    Bulk upsert odds rows using a temporary staging table.

    This path is resilient to overlap-driven duplicates (same token/timestamp)
    and generally performs better than row-wise inserts for large minutely loads.
    """
    if not records:
        return

    # Defensive schema guard: callers may provide connections from mixed test/runtime
    # setups where odds_history wasn't initialized on this file yet.
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_TAB_ODDS_HISTORY} (
            clobTokenId TEXT,
            timestamp BIGINT,
            price DOUBLE,
            ingested_at TIMESTAMP,
            PRIMARY KEY (clobTokenId, timestamp)
        )
        """
    )
    conn.execute(
        f"ALTER TABLE {_TAB_ODDS_HISTORY} ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMP"
    )

    ingested = _utc_now()
    if assume_deduped:
        rows = [
            (token_id, int(timestamp), float(price), ingested)
            for token_id, timestamp, price in records
        ]
    else:
        # Ensure deterministic conflict behavior even if callers pass duplicates.
        dedup: Dict[Tuple[str, int], float] = {}
        for token_id, timestamp, price in records:
            dedup[(token_id, int(timestamp))] = float(price)
        rows = [
            (token_id, timestamp, price, ingested)
            for (token_id, timestamp), price in dedup.items()
        ]

    conn.execute(
        """
        CREATE TEMPORARY TABLE IF NOT EXISTS _odds_staging (
            clobTokenId TEXT,
            timestamp BIGINT,
            price DOUBLE,
            ingested_at TIMESTAMP
        )
        """
    )
    if hasattr(duckdb, "Appender"):
        appender = duckdb.Appender(conn, "_odds_staging")
        try:
            for token_id, timestamp, price, ing in rows:
                appender.append([token_id, timestamp, price, ing])
        finally:
            appender.close()
    else:
        conn.executemany(
            "INSERT INTO _odds_staging (clobTokenId, timestamp, price, ingested_at) VALUES (?, ?, ?, ?)",
            rows,
        )
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {_TAB_ODDS_HISTORY} (clobTokenId, timestamp, price, ingested_at)
        SELECT clobTokenId, timestamp, price, ingested_at
        FROM _odds_staging
        """
    )
    conn.execute("DELETE FROM _odds_staging")
    logger.debug("Upserted %d odds records to DuckDB", len(rows))
