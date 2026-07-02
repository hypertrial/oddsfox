"""Tests for scripts/repair_polymarket_token_sync_ledger.py."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest


def _load_repair_module(monkeypatch):
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    import repair_polymarket_token_sync_ledger as repair

    monkeypatch.setattr(repair.duckdb, "__version__", "1.5.2", raising=False)
    return repair


def _create_ledger(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("create schema polymarket_ops")
    conn.execute(
        """
        create table polymarket_ops.token_sync_ledger (
            clobTokenId varchar primary key,
            last_sync_timestamp bigint,
            fully_checked boolean,
            last_checked_at timestamp,
            next_check_at timestamp,
            empty_run_streak integer
        )
        """
    )
    conn.execute(
        """
        insert into polymarket_ops.token_sync_ledger values
        ('tok-a', 10, true, '2026-01-01 00:00:00', null, 0),
        ('tok-b', 20, false, null, '2026-01-02 00:00:00', 2)
        """
    )


def test_repair_token_sync_ledger_rebuilds_transactionally(monkeypatch, tmp_path):
    repair = _load_repair_module(monkeypatch)
    db_path = tmp_path / "repair.duckdb"

    conn = duckdb.connect(str(db_path))
    try:
        _create_ledger(conn)
        conn.execute(
            "create index idx_ledger_checked on polymarket_ops.token_sync_ledger(fully_checked)"
        )

        summary = repair.repair_token_sync_ledger(conn)

        rows = conn.execute(
            """
            select clobTokenId, last_sync_timestamp, fully_checked, empty_run_streak
            from polymarket_ops.token_sync_ledger
            order by clobTokenId
            """
        ).fetchall()
        constraints = conn.execute(
            """
            select constraint_type, constraint_column_names
            from duckdb_constraints()
            where schema_name = 'polymarket_ops'
              and table_name = 'token_sync_ledger'
            """
        ).fetchall()
    finally:
        conn.close()

    assert summary == {"rows": 2, "removed_secondary_indexes": 1}
    assert rows == [("tok-a", 10, True, 0), ("tok-b", 20, False, 2)]
    assert ("PRIMARY KEY", ["clobTokenId"]) in constraints


def test_repair_token_sync_ledger_rejects_bad_schema_before_mutation(
    monkeypatch, tmp_path
):
    repair = _load_repair_module(monkeypatch)
    db_path = tmp_path / "bad-repair.duckdb"

    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("create schema polymarket_ops")
        conn.execute("create table polymarket_ops.token_sync_ledger (bad varchar)")

        with pytest.raises(RuntimeError, match="Unexpected polymarket_ops"):
            repair.repair_token_sync_ledger(conn)

        columns = conn.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = 'polymarket_ops'
              and table_name = 'token_sync_ledger'
            """
        ).fetchall()
    finally:
        conn.close()

    assert columns == [("bad",)]
