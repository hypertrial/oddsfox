"""Tests for scripts/compact_warehouse.py."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


def _load_compact_module():
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    import compact_warehouse

    return compact_warehouse


def _create_compactable_db(path: Path) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute("create schema polymarket_raw")
        conn.execute("create table polymarket_raw.markets (id varchar)")
        conn.execute("insert into polymarket_raw.markets values ('m1')")
        conn.execute(
            "create view polymarket_raw.market_ids as select id from polymarket_raw.markets"
        )
        conn.execute("checkpoint")
    finally:
        conn.close()


def test_compact_warehouse_dry_run_verifies_without_swap(monkeypatch, tmp_path):
    compact = _load_compact_module()
    db_path = tmp_path / "warehouse.duckdb"
    _create_compactable_db(db_path)
    original_size = db_path.stat().st_size

    monkeypatch.setattr(
        sys,
        "argv",
        ["compact_warehouse.py", "--duckdb-path", str(db_path), "--dry-run"],
    )

    assert compact.main() == 0

    tmp = db_path.with_name(db_path.stem + ".compact_tmp" + db_path.suffix)
    backup = db_path.with_name(db_path.name + compact.BACKUP_SUFFIX)
    assert db_path.exists()
    assert db_path.stat().st_size == original_size
    assert tmp.exists()
    assert not backup.exists()

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        assert conn.execute("select id from polymarket_raw.markets").fetchall() == [
            ("m1",)
        ]
    finally:
        conn.close()


def test_compact_warehouse_refuses_wal_without_touching_source(monkeypatch, tmp_path):
    compact = _load_compact_module()
    db_path = tmp_path / "warehouse.duckdb"
    _create_compactable_db(db_path)
    wal = db_path.with_name(db_path.name + ".wal")
    wal.write_text("", encoding="utf-8")
    original_size = db_path.stat().st_size

    monkeypatch.setattr(
        sys,
        "argv",
        ["compact_warehouse.py", "--duckdb-path", str(db_path), "--dry-run"],
    )

    assert compact.main() == 1

    tmp = db_path.with_name(db_path.stem + ".compact_tmp" + db_path.suffix)
    assert db_path.exists()
    assert db_path.stat().st_size == original_size
    assert not tmp.exists()
