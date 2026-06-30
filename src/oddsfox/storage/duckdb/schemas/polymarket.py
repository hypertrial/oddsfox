"""Polymarket raw/ops DDL, primary keys, and indexes."""

from __future__ import annotations

import logging

import duckdb

from oddsfox.storage.duckdb.schemas.constants import (
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)

logger = logging.getLogger(__name__)


def ensure_polymarket_indexes(conn: duckdb.DuckDBPyConnection) -> None:
    """Create indexes; log warnings when DuckDB rejects a statement."""
    m = polymarket_raw_tbl("markets")
    oh = polymarket_raw_tbl("odds_history")
    tod = polymarket_raw_tbl("token_odds_daily")
    sk = polymarket_ops_tbl("token_sync_skips")
    wc_reg = polymarket_ops_tbl("wc2026_market_registry")
    index_statements = [
        f"CREATE INDEX IF NOT EXISTS idx_category ON {m}(category)",
        f"CREATE INDEX IF NOT EXISTS idx_volume ON {m}(volume)",
        f"CREATE INDEX IF NOT EXISTS idx_slug ON {m}(slug)",
        f"CREATE INDEX IF NOT EXISTS idx_event_slug ON {m}(event_slug)",
        f"CREATE INDEX IF NOT EXISTS idx_wc2026_registry_event_slug ON {wc_reg}(event_slug)",
        f"CREATE INDEX IF NOT EXISTS idx_odds_token ON {oh}(clobTokenId)",
        f"CREATE INDEX IF NOT EXISTS idx_odds_timestamp ON {oh}(timestamp)",
        f"CREATE INDEX IF NOT EXISTS idx_token_odds_daily_token ON {tod}(clobTokenId)",
        f"CREATE INDEX IF NOT EXISTS idx_token_odds_daily_date ON {tod}(odds_date_utc)",
        f"CREATE INDEX IF NOT EXISTS idx_token_skip_reason ON {sk}(clobTokenId)",
    ]
    try:
        has_dlt_markets = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_schema = 'polymarket_raw'
              AND table_name = 'markets'
              AND column_name = '_dlt_id'
            """
        ).fetchone()
    except Exception as exc:
        logger.warning(
            "Skipping dlt markets unique-index check after metadata query failed: %s",
            exc,
        )
        has_dlt_markets = None
    if has_dlt_markets and has_dlt_markets[0]:
        # dlt-owned markets tables declare primary_key=id in the resource but DuckDB
        # does not always materialize a UNIQUE/PK constraint; snapshot upserts need it.
        index_statements.insert(
            0, f"CREATE UNIQUE INDEX IF NOT EXISTS idx_markets_id ON {m}(id)"
        )
    for stmt in index_statements:
        try:
            conn.execute(stmt)
        except Exception as exc:
            logger.warning("Index creation skipped (%s): %s", stmt, exc)


def bootstrap_polymarket_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """CREATE TABLE IF NOT EXISTS for Polymarket core warehouse tables.

    ``polymarket_raw.markets`` is owned by the dlt landing asset, not bootstrap.
    """
    sm = polymarket_ops_tbl("scrape_metadata")
    mt = polymarket_raw_tbl("market_tokens")
    oh = polymarket_raw_tbl("odds_history")
    tod = polymarket_raw_tbl("token_odds_daily")
    led = polymarket_ops_tbl("token_sync_ledger")
    skip = polymarket_ops_tbl("token_sync_skips")
    mmu = polymarket_ops_tbl("market_metadata_unresolved")
    pre = polymarket_ops_tbl("pipeline_run_events")
    srm = polymarket_ops_tbl("sync_run_metrics")
    wc_reg = polymarket_ops_tbl("wc2026_market_registry")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {sm} (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {mt} (
            market_id TEXT PRIMARY KEY,
            clobTokenIds TEXT,
            updated_at TIMESTAMP
        )
        """
    )
    conn.execute(f"ALTER TABLE {mt} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {oh} (
            clobTokenId TEXT,
            timestamp BIGINT,
            price DOUBLE,
            ingested_at TIMESTAMP,
            PRIMARY KEY (clobTokenId, timestamp)
        )
        """
    )
    conn.execute(f"ALTER TABLE {oh} ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMP")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {tod} (
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
    conn.execute(f"ALTER TABLE {tod} ADD COLUMN IF NOT EXISTS refreshed_at TIMESTAMP")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {led} (
            clobTokenId TEXT PRIMARY KEY,
            last_sync_timestamp BIGINT,
            fully_checked BOOLEAN DEFAULT FALSE,
            last_checked_at TIMESTAMP,
            next_check_at TIMESTAMP,
            empty_run_streak INTEGER DEFAULT 0
        )
        """
    )
    conn.execute(
        f"ALTER TABLE {led} ADD COLUMN IF NOT EXISTS last_checked_at TIMESTAMP"
    )
    conn.execute(f"ALTER TABLE {led} ADD COLUMN IF NOT EXISTS next_check_at TIMESTAMP")
    conn.execute(
        f"ALTER TABLE {led} ADD COLUMN IF NOT EXISTS empty_run_streak INTEGER DEFAULT 0"
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {skip} (
            clobTokenId TEXT PRIMARY KEY,
            reason TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {mmu} (
            market_id TEXT,
            field_name TEXT,
            reason TEXT,
            attempts INTEGER DEFAULT 0,
            last_checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            next_retry_at TIMESTAMP,
            PRIMARY KEY (market_id, field_name)
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {pre} (
            run_id TEXT PRIMARY KEY,
            task_name TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL,
            metrics_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {srm} (
            task_name TEXT PRIMARY KEY,
            recorded_at TIMESTAMP NOT NULL,
            metrics_json TEXT NOT NULL,
            history_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {wc_reg} (
            market_id TEXT PRIMARY KEY,
            event_slug TEXT,
            event_id TEXT,
            source TEXT,
            refreshed_at TIMESTAMP
        )
        """
    )


def drop_legacy_bootstrap_markets_table_if_needed(
    conn: duckdb.DuckDBPyConnection,
) -> bool:
    """Drop bootstrap-owned ``markets`` so dlt can create its schema.

    Returns True when a legacy table was dropped.
    """
    m = polymarket_raw_tbl("markets")
    exists = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'polymarket_raw' AND table_name = 'markets'
        """
    ).fetchone()
    if not exists or not exists[0]:
        return False
    has_dlt_id = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = 'polymarket_raw'
          AND table_name = 'markets'
          AND column_name = '_dlt_id'
        """
    ).fetchone()
    if has_dlt_id and has_dlt_id[0]:
        return False
    logger.info(
        "Dropping legacy bootstrap %s so dlt can own polymarket_raw.markets",
        m,
    )
    conn.execute(f"DROP TABLE {m}")
    return True


def create_test_markets_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Empty app-schema markets table for dbt source tests and local CI.

    Production loads use dlt-owned ``polymarket_raw.markets``; the dlt asset drops
    this stub before landing when present.
    """
    m = polymarket_raw_tbl("markets")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {m} (
            id TEXT PRIMARY KEY,
            question TEXT,
            category TEXT,
            description TEXT,
            outcomes TEXT,
            volume DOUBLE,
            active BOOLEAN,
            closed BOOLEAN,
            created_at TIMESTAMP,
            scraped_at TIMESTAMP,
            end_date TIMESTAMP,
            slug TEXT,
            event_slug TEXT,
            event_id TEXT
        )
        """
    )


__all__ = [
    "bootstrap_polymarket_tables",
    "create_test_markets_table",
    "drop_legacy_bootstrap_markets_table_if_needed",
    "ensure_polymarket_indexes",
]
