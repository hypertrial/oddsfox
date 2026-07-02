"""Shared fixtures for storage/duckdb unit tests."""

from __future__ import annotations

import importlib
from datetime import datetime, timezone

import pytest

import oddsfox.storage.duckdb.markets as markets
from oddsfox.config._reload_settings import reload_all_settings_modules
from oddsfox.storage.duckdb.connection import (
    get_connection,
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)
from oddsfox.storage.duckdb.schemas.polymarket import create_test_markets_table

T_M = polymarket_raw_tbl("markets")
T_MT = polymarket_raw_tbl("market_tokens")
T_OH = polymarket_raw_tbl("odds_history")
T_TOD = polymarket_raw_tbl("token_odds_daily")
T_LED = polymarket_ops_tbl("token_sync_ledger")
T_SK = polymarket_ops_tbl("token_sync_skips")
T_PRE = polymarket_ops_tbl("pipeline_run_events")
T_UNR = polymarket_ops_tbl("market_metadata_unresolved")


@pytest.fixture
def duck(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "unit.duckdb"))
    import oddsfox.storage.duckdb.connection as connection

    reload_all_settings_modules()
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
    importlib.reload(connection)
    connection.ensure_duck_db()
    with get_connection() as conn:
        create_test_markets_table(conn)
    yield connection
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False


def _insert_minimal_market(conn, mid="m1", **kwargs):
    defaults = dict(
        id=mid,
        question="Q",
        category="c",
        description="d",
        outcomes="[]",
        volume=1.0,
        active=True,
        closed=False,
        created_at=datetime.now(timezone.utc),
        scraped_at=datetime.now(timezone.utc),
        end_date=None,
        slug=None,
        event_slug=None,
        event_id=None,
    )
    defaults.update(kwargs)
    conn.execute(
        f"""INSERT OR REPLACE INTO {T_M}
        (id, question, category, description, outcomes, volume, active, closed,
         created_at, scraped_at, end_date, slug, event_slug, event_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            defaults["id"],
            defaults["question"],
            defaults["category"],
            defaults["description"],
            defaults["outcomes"],
            defaults["volume"],
            defaults["active"],
            defaults["closed"],
            defaults["created_at"],
            defaults["scraped_at"],
            defaults["end_date"],
            defaults["slug"],
            defaults["event_slug"],
            defaults["event_id"],
        ],
    )


def _normalize_market_tuple(row: tuple) -> tuple:
    if len(row) == 14:
        normalized = row
    elif len(row) == 13:
        normalized = (*row, None)
    elif len(row) == 12:
        expanded = list(row)
        expanded.insert(10, None)
        normalized = (*expanded, None)
    elif len(row) == 11:
        expanded = list(row)
        expanded.insert(10, None)
        normalized = (*expanded, None, None)
    elif len(row) == 10:
        normalized = (*row, None, None, None, None)
    else:
        raise ValueError(f"Expected 10-14 columns for markets insert, got {len(row)}")

    rec = list(normalized)
    end_val = rec[10]
    if not end_val or (isinstance(end_val, str) and not end_val.strip()):
        rec[10] = None
    return tuple(rec)


def _insert_market_tuple(conn, row: tuple) -> None:
    rec = _normalize_market_tuple(row)
    conn.execute(
        f"""INSERT OR REPLACE INTO {T_M}
        (id, question, category, description, outcomes, volume, active, closed,
         created_at, scraped_at, end_date, slug, event_slug, event_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        list(rec),
    )


def _seed_markets(duck, market_rows=None, token_rows=None) -> None:
    """Seed markets via direct insert; persist tokens through save_market_tokens_batch."""
    with duck.get_connection() as conn:
        for row in market_rows or ():
            _insert_market_tuple(conn, row)
    if token_rows:
        markets.save_market_tokens_batch(token_rows)
