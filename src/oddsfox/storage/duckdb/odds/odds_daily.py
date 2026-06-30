from datetime import date
from typing import List, Tuple

import duckdb

from oddsfox.storage.duckdb.connection import ensure_duck_db, get_connection
from oddsfox.storage.duckdb.odds._common import (
    _TAB_ODDS_HISTORY,
    _TAB_TOKEN_ODDS_DAILY,
    logger,
)


def refresh_token_odds_daily(
    token_dates: List[Tuple[str, date]],
    conn: duckdb.DuckDBPyConnection,
):
    """
    Rebuild daily OHLC rows from canonical minutely history for specific token-days

    This makes overlap re-fetches deterministic because the daily table is always
    derived from the full set of minutely records currently stored for that day.
    """
    if not token_dates:
        return

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_TAB_TOKEN_ODDS_DAILY} (
            clobTokenId TEXT,
            odds_date_utc DATE,
            open_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            close_price DOUBLE,
            avg_price DOUBLE,
            observed_points BIGINT,
            first_timestamp BIGINT,
            last_timestamp BIGINT,
            refreshed_at TIMESTAMP,
            PRIMARY KEY (clobTokenId, odds_date_utc)
        )
        """
    )
    conn.execute(
        f"ALTER TABLE {_TAB_TOKEN_ODDS_DAILY} ADD COLUMN IF NOT EXISTS refreshed_at TIMESTAMP"
    )
    conn.execute(
        """
        CREATE TEMPORARY TABLE IF NOT EXISTS _token_odds_daily_refresh (
            clobTokenId TEXT,
            odds_date_utc DATE
        )
        """
    )
    conn.execute("DELETE FROM _token_odds_daily_refresh")
    unique_keys = sorted(
        {(str(token_id), odds_date) for token_id, odds_date in token_dates}
    )
    conn.executemany(
        """
        INSERT INTO _token_odds_daily_refresh (clobTokenId, odds_date_utc)
        VALUES (?, ?)
        """,
        unique_keys,
    )
    conn.execute(
        f"""
        DELETE FROM {_TAB_TOKEN_ODDS_DAILY} d
        WHERE EXISTS (
            SELECT 1
            FROM _token_odds_daily_refresh r
            WHERE r.clobTokenId = d.clobTokenId
              AND r.odds_date_utc = d.odds_date_utc
        )
        """
    )
    conn.execute(
        f"""
        INSERT INTO {_TAB_TOKEN_ODDS_DAILY} (
            clobTokenId,
            odds_date_utc,
            open_price,
            high_price,
            low_price,
            close_price,
            avg_price,
            observed_points,
            first_timestamp,
            last_timestamp,
            refreshed_at
        )
        WITH scoped_history AS (
            SELECT
                h.clobTokenId,
                CAST(TIMESTAMP '1970-01-01' + h.timestamp * INTERVAL 1 SECOND AS DATE) AS odds_date_utc,
                h.timestamp,
                h.price
            FROM {_TAB_ODDS_HISTORY} h
            JOIN _token_odds_daily_refresh r
              ON r.clobTokenId = h.clobTokenId
             AND r.odds_date_utc = CAST(TIMESTAMP '1970-01-01' + h.timestamp * INTERVAL 1 SECOND AS DATE)
        ),
        ranked AS (
            SELECT
                clobTokenId,
                odds_date_utc,
                timestamp,
                price,
                row_number() OVER (
                    PARTITION BY clobTokenId, odds_date_utc
                    ORDER BY timestamp ASC, price ASC
                ) AS open_rank,
                row_number() OVER (
                    PARTITION BY clobTokenId, odds_date_utc
                    ORDER BY timestamp DESC, price DESC
                ) AS close_rank
            FROM scoped_history
        )
        SELECT
            clobTokenId,
            odds_date_utc,
            MAX(CASE WHEN open_rank = 1 THEN price END) AS open_price,
            MAX(price) AS high_price,
            MIN(price) AS low_price,
            MAX(CASE WHEN close_rank = 1 THEN price END) AS close_price,
            AVG(price) AS avg_price,
            COUNT(*) AS observed_points,
            MIN(timestamp) AS first_timestamp,
            MAX(timestamp) AS last_timestamp,
            CURRENT_TIMESTAMP AS refreshed_at
        FROM ranked
        GROUP BY 1, 2
        """
    )
    logger.debug("Refreshed %d daily odds keys from history", len(unique_keys))


def backfill_token_odds_daily_from_history() -> int:
    """Rebuild the full daily odds table from minutely history and return row count."""
    ensure_duck_db()
    with get_connection() as conn:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_TAB_TOKEN_ODDS_DAILY} (
                clobTokenId TEXT,
                odds_date_utc DATE,
                open_price DOUBLE,
                high_price DOUBLE,
                low_price DOUBLE,
                close_price DOUBLE,
                avg_price DOUBLE,
                observed_points BIGINT,
                first_timestamp BIGINT,
                last_timestamp BIGINT,
                refreshed_at TIMESTAMP,
                PRIMARY KEY (clobTokenId, odds_date_utc)
            )
            """
        )
        conn.execute(
            f"ALTER TABLE {_TAB_TOKEN_ODDS_DAILY} ADD COLUMN IF NOT EXISTS refreshed_at TIMESTAMP"
        )
        conn.execute(f"DELETE FROM {_TAB_TOKEN_ODDS_DAILY}")
        conn.execute(
            f"""
            INSERT INTO {_TAB_TOKEN_ODDS_DAILY} (
                clobTokenId,
                odds_date_utc,
                open_price,
                high_price,
                low_price,
                close_price,
                avg_price,
                observed_points,
                first_timestamp,
                last_timestamp,
                refreshed_at
            )
            WITH history AS (
                SELECT
                    clobTokenId,
                    CAST(TIMESTAMP '1970-01-01' + timestamp * INTERVAL 1 SECOND AS DATE) AS odds_date_utc,
                    timestamp,
                    price
                FROM {_TAB_ODDS_HISTORY}
            ),
            ranked AS (
                SELECT
                    clobTokenId,
                    odds_date_utc,
                    timestamp,
                    price,
                    row_number() OVER (
                        PARTITION BY clobTokenId, odds_date_utc
                        ORDER BY timestamp ASC, price ASC
                    ) AS open_rank,
                    row_number() OVER (
                        PARTITION BY clobTokenId, odds_date_utc
                        ORDER BY timestamp DESC, price DESC
                    ) AS close_rank
                FROM history
            )
            SELECT
                clobTokenId,
                odds_date_utc,
                MAX(CASE WHEN open_rank = 1 THEN price END) AS open_price,
                MAX(price) AS high_price,
                MIN(price) AS low_price,
                MAX(CASE WHEN close_rank = 1 THEN price END) AS close_price,
                AVG(price) AS avg_price,
                COUNT(*) AS observed_points,
                MIN(timestamp) AS first_timestamp,
                MAX(timestamp) AS last_timestamp,
                CURRENT_TIMESTAMP AS refreshed_at
            FROM ranked
            GROUP BY 1, 2
            """
        )
        row = conn.execute(f"SELECT COUNT(*) FROM {_TAB_TOKEN_ODDS_DAILY}").fetchone()
    count = int(row[0]) if row and row[0] is not None else 0
    logger.info("Backfilled token_odds_daily from minutely history: rows=%s", count)
    return count
