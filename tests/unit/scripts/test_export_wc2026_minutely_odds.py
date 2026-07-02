"""Tests for scripts/export_wc2026_minutely_odds.py."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


def _load_export_module():
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    from export_wc2026_minutely_odds import export_minutely_odds_parquet, mart_exists

    return export_minutely_odds_parquet, mart_exists


def test_export_minutely_odds_parquet_round_trip(tmp_path: Path) -> None:
    export_minutely_odds_parquet, mart_exists = _load_export_module()

    db_path = tmp_path / "test.duckdb"
    out_path = tmp_path / "wc2026_token_minutely_odds.parquet"

    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("create schema polymarket_marts")
        conn.execute(
            """
            create table polymarket_marts.wc2026_token_minutely_odds (
                market_id varchar,
                outcome_index integer,
                clob_token_id varchar,
                question varchar,
                event_slug varchar,
                is_active boolean,
                is_closed boolean,
                market_volume_usd double,
                odds_timestamp timestamp,
                odds_timestamp_epoch bigint,
                price double
            )
            """
        )
        conn.executemany(
            """
            insert into polymarket_marts.wc2026_token_minutely_odds values
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "m1",
                    0,
                    "tok-a",
                    "Q1",
                    "wc2026-a",
                    True,
                    False,
                    1000.0,
                    "2026-06-01 12:00:00",
                    1780833600,
                    0.42,
                ),
                (
                    "m1",
                    1,
                    "tok-b",
                    "Q1",
                    "wc2026-a",
                    True,
                    False,
                    1000.0,
                    "2026-06-01 12:01:00",
                    1780833660,
                    0.58,
                ),
            ],
        )
        assert mart_exists(conn) is True
        row_count = export_minutely_odds_parquet(conn, out_path)
    finally:
        conn.close()

    assert row_count == 2
    assert out_path.is_file()

    verify = duckdb.connect()
    try:
        got = verify.execute(
            "select count(*) from read_parquet(?)",
            [str(out_path)],
        ).fetchone()
    finally:
        verify.close()

    assert got == (2,)


def test_export_missing_mart_raises(tmp_path: Path) -> None:
    export_minutely_odds_parquet, _ = _load_export_module()

    db_path = tmp_path / "empty.duckdb"
    out_path = tmp_path / "missing.parquet"

    conn = duckdb.connect(str(db_path))
    try:
        try:
            export_minutely_odds_parquet(conn, out_path)
        except LookupError as exc:
            assert "wc2026_token_minutely_odds" in str(exc)
        else:
            raise AssertionError("expected LookupError for missing mart")
    finally:
        conn.close()
