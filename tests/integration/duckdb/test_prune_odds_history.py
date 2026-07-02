"""Integration tests for scripts/prune_odds_history.py."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

import oddsfox.storage.duckdb.connection as connection
from oddsfox.storage.duckdb.connection import init_duck_db
from oddsfox.storage.duckdb.schemas.constants import polymarket_raw_tbl

_SCRIPTS_DIR = Path(__file__).resolve().parents[3] / "scripts"
sys.path.insert(0, str(_SCRIPTS_DIR))
from prune_odds_history import prune_odds_history  # noqa: E402

_ODDS_HISTORY = polymarket_raw_tbl("odds_history")


def _insert_odds_row(
    conn: duckdb.DuckDBPyConnection,
    token_id: str,
    epoch: int,
    price: float = 0.5,
) -> None:
    conn.execute(
        f"""
        INSERT INTO {_ODDS_HISTORY} (clobTokenId, timestamp, price, ingested_at)
        VALUES (?, ?, ?, ?)
        """,
        [token_id, epoch, price, datetime.now(timezone.utc)],
    )


def test_prune_odds_history_deletes_old_rows(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "oddsfox.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    connection.reset_duckdb_connection_state()
    init_duck_db()

    now = datetime.now(timezone.utc)
    old_epoch = int((now - timedelta(days=400)).timestamp())
    recent_epoch = int((now - timedelta(days=30)).timestamp())

    with duckdb.connect(str(db_path)) as conn:
        _insert_odds_row(conn, "token-old", old_epoch)
        _insert_odds_row(conn, "token-recent", recent_epoch)

        summary = prune_odds_history(conn, retention_days=365)

        remaining = conn.execute(
            f"SELECT clobTokenId, timestamp FROM {_ODDS_HISTORY} ORDER BY timestamp"
        ).fetchall()

    assert summary["deleted"] == 1
    assert summary["remaining"] == 1
    assert summary["total_before"] == 2
    assert remaining == [("token-recent", recent_epoch)]


def test_prune_odds_history_dry_run_leaves_rows_intact(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "oddsfox.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    connection.reset_duckdb_connection_state()
    init_duck_db()

    now = datetime.now(timezone.utc)
    old_epoch = int((now - timedelta(days=400)).timestamp())
    recent_epoch = int((now - timedelta(days=30)).timestamp())

    with duckdb.connect(str(db_path)) as conn:
        _insert_odds_row(conn, "token-old", old_epoch)
        _insert_odds_row(conn, "token-recent", recent_epoch)

        summary = prune_odds_history(conn, retention_days=365, dry_run=True)

        count = conn.execute(f"SELECT COUNT(*) FROM {_ODDS_HISTORY}").fetchone()[0]

    assert summary["deleted"] == 1
    assert summary["remaining"] == 2
    assert count == 2
