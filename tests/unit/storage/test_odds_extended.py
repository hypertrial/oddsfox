"""Cover storage/duckdb/odds package snapshot and timestamp merge branches."""

from __future__ import annotations

import importlib
import time

import duckdb
import pytest

import oddsfox.storage.duckdb.odds as odds_mod
from oddsfox.config._reload_settings import reload_all_settings_modules
from oddsfox.storage.duckdb.connection import (
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)

T_OH = polymarket_raw_tbl("odds_history")
T_TOD = polymarket_raw_tbl("token_odds_daily")
T_LED = polymarket_ops_tbl("token_sync_ledger")
T_SK = polymarket_ops_tbl("token_sync_skips")


@pytest.fixture
def duck(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "oe.duckdb"))
    import oddsfox.storage.duckdb.connection as connection

    reload_all_settings_modules()
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
    importlib.reload(connection)
    connection.ensure_duck_db()
    yield connection
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False


def test_get_latest_timestamps_ledger_beats_history(duck):
    with odds_mod.get_connection() as conn:
        conn.execute(
            f"INSERT OR REPLACE INTO {T_OH} (clobTokenId, timestamp, price) VALUES ('ab', 10, 0.5)"
        )
        conn.execute(
            f"INSERT INTO {T_LED} (clobTokenId, last_sync_timestamp) VALUES ('ab', 50)"
        )
    ts = odds_mod.get_latest_timestamps()
    assert ts["ab"] == 50


def test_get_latest_timestamps_ledger_null_skipped(duck):
    with odds_mod.get_connection() as conn:
        conn.execute(
            f"INSERT INTO {T_LED} (clobTokenId, last_sync_timestamp) VALUES ('nl', NULL)"
        )
    ts = odds_mod.get_latest_timestamps()
    assert "nl" not in ts


def test_get_latest_timestamps_history_only_and_empty_helpers(duck):
    odds_mod.save_odds_batch([("hist", 11, 0.4)])
    ts = odds_mod.get_latest_timestamps()
    assert ts["hist"] == 11
    assert odds_mod.get_skipped_tokens() == {}


def test_get_latest_timestamps_ledger_only(duck):
    with odds_mod.get_connection() as conn:
        conn.execute(
            f"INSERT INTO {T_LED} (clobTokenId, last_sync_timestamp) VALUES ('ledgeronly', 22)"
        )
    ts = odds_mod.get_latest_timestamps()
    assert ts["ledgeronly"] == 22


def test_save_helpers_empty_guards(duck):
    odds_mod.save_skipped_tokens([])
    odds_mod.mark_tokens_fully_checked([])
    odds_mod.save_odds_batch([])
    odds_mod.save_sync_status_batch([])
    odds_mod.save_token_sync_state_batch([])
    with odds_mod.get_connection() as conn:
        odds_mod.upsert_ledger_last_sync_batch([], conn)
        odds_mod.upsert_token_sync_state_batch([], conn)
        odds_mod.upsert_skipped_tokens_batch([], conn)


def test_refresh_token_odds_daily_empty_guard(duck):
    with odds_mod.get_connection() as conn:
        odds_mod.refresh_token_odds_daily([], conn)


def test_save_odds_bulk_upsert_no_appender(duck, monkeypatch):
    with odds_mod.get_connection() as conn:
        monkeypatch.delattr(duckdb, "Appender", raising=False)
        odds_mod.save_odds_bulk_upsert([("nx", 1, 0.1)], conn, assume_deduped=False)


def test_save_odds_bulk_appender_empty_and_assume_deduped(duck):
    with odds_mod.get_connection() as conn:
        odds_mod.save_odds_bulk_appender([], conn)
        odds_mod.save_odds_bulk_upsert([("dd", 1.0, 0.2)], conn, assume_deduped=True)


def test_save_odds_bulk_upsert_empty_guard(duck):
    with odds_mod.get_connection() as conn:
        odds_mod.save_odds_bulk_upsert([], conn)


def test_refresh_token_odds_daily_splits_utc_days(duck):
    with odds_mod.get_connection() as conn:
        odds_mod.save_odds_bulk_upsert(
            [
                ("daytok", 1710115199, 0.2),
                ("daytok", 1710115200, 0.8),
            ],
            conn,
            assume_deduped=False,
        )
        odds_mod.refresh_token_odds_daily(
            [
                ("daytok", odds_mod._epoch_to_utc_date(1710115199)),
                ("daytok", odds_mod._epoch_to_utc_date(1710115200)),
            ],
            conn,
        )
        rows = conn.execute(
            f"""
            SELECT odds_date_utc, open_price, close_price, observed_points
            FROM {T_TOD}
            WHERE clobTokenId = 'daytok'
            ORDER BY odds_date_utc
            """
        ).fetchall()
    assert len(rows) == 2
    assert rows[0][1:] == (0.2, 0.2, 1)
    assert rows[1][1:] == (0.8, 0.8, 1)


def test_backfill_token_odds_daily_replaces_existing_rows(duck):
    odds_mod.save_odds_batch([("bf", 1710000000, 0.1), ("bf", 1710000600, 0.9)])
    with odds_mod.get_connection() as conn:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {T_TOD}
            (clobTokenId, odds_date_utc, open_price, high_price, low_price, close_price, avg_price, observed_points, first_timestamp, last_timestamp)
            VALUES ('bf', DATE '2024-03-09', 0.0, 0.0, 0.0, 0.0, 0.0, 99, 1, 1)
            """
        )
    count = odds_mod.backfill_token_odds_daily_from_history()
    assert count >= 1
    with odds_mod.get_connection() as conn:
        row = conn.execute(
            f"""
            SELECT open_price, high_price, low_price, close_price, avg_price, observed_points
            FROM {T_TOD}
            WHERE clobTokenId = 'bf'
            """
        ).fetchone()
    assert row == (0.1, 0.9, 0.1, 0.9, 0.5, 2)


def test_get_token_sync_snapshot_repair_and_missing(duck):
    tid = "c" * 33 + "12"
    with odds_mod.get_connection() as conn:
        conn.execute(f"DELETE FROM {T_OH}")
        conn.execute(
            f"INSERT INTO {T_OH} (clobTokenId, timestamp, price) VALUES (?, 100, 0.5)",
            [tid],
        )
        conn.execute(
            f"INSERT INTO {T_LED} (clobTokenId, last_sync_timestamp) VALUES (?, 10)",
            [tid],
        )
    a, fully, skips = odds_mod.get_token_sync_snapshot(
        [tid], reconcile_with_history=True, repair_ledger=True
    )
    assert a.get(tid) == 100


def test_get_token_sync_snapshot_missing_only_history(duck):
    tid = "d" * 33 + "12"
    odds_mod.save_odds_batch([(tid, 77, 0.3)])
    a, _, _ = odds_mod.get_token_sync_snapshot(
        [tid], reconcile_with_history=True, repair_ledger=False
    )
    assert a[tid] >= 77


def test_get_token_sync_chunk_boundary(monkeypatch, duck):
    """Force multiple chunks through _chunked by lowering chunk size."""
    monkeypatch.setattr(odds_mod, "_TOKEN_STATE_CHUNK_SIZE", 1)
    t1 = "e" * 33 + "12"
    t2 = "f" * 33 + "12"
    odds_mod.save_odds_batch([(t1, 1, 0.1), (t2, 2, 0.2)])
    a, _, _ = odds_mod.get_token_sync_snapshot([t1, t2])
    assert len(a) == 2


def test_reconcile_ledger_empty_db(duck):
    with odds_mod.get_connection() as conn:
        conn.execute(f"DELETE FROM {T_OH}")
    odds_mod.reconcile_token_sync_ledger_from_history()


def test_reconcile_ledger_with_missing_rows_and_snapshot_without_repairs(duck):
    with odds_mod.get_connection() as conn:
        conn.execute(f"DELETE FROM {T_OH}")
        conn.execute(f"DELETE FROM {T_LED}")
        conn.execute(
            f"INSERT INTO {T_LED} (clobTokenId, last_sync_timestamp) VALUES ('nohist', NULL)"
        )
    summary = odds_mod.reconcile_token_sync_ledger_from_history()
    assert summary["scanned_tokens"] == 0
    assert summary["repaired_tokens"] == 0

    latest, fully, skips = odds_mod.get_token_sync_snapshot(
        ["nohist"], reconcile_with_history=True, repair_ledger=False
    )
    assert latest == {}
    assert fully == set()
    assert skips == {}


def test_get_token_sync_snapshot_falsey_flags_without_reason(duck):
    with odds_mod.get_connection() as conn:
        conn.execute(f"DELETE FROM {T_OH}")
        conn.execute(f"DELETE FROM {T_LED}")
        conn.execute(f"DELETE FROM {T_SK}")
        conn.execute(
            f"INSERT INTO {T_LED} (clobTokenId, last_sync_timestamp, fully_checked) VALUES ('plain', 10, FALSE)"
        )
    latest, fully, skips = odds_mod.get_token_sync_snapshot(
        ["plain"], reconcile_with_history=True, repair_ledger=False
    )
    assert latest["plain"] == 10
    assert fully == set()
    assert skips == {}


def test_reconcile_snapshot_without_repairs_when_history_not_newer(duck):
    tid = "g" * 33 + "12"
    with odds_mod.get_connection() as conn:
        conn.execute(f"DELETE FROM {T_OH}")
        conn.execute(f"DELETE FROM {T_LED}")
        conn.execute(
            f"INSERT INTO {T_OH} (clobTokenId, timestamp, price) VALUES (?, 50, 0.5)",
            [tid],
        )
        conn.execute(
            f"INSERT INTO {T_LED} (clobTokenId, last_sync_timestamp) VALUES (?, 75)",
            [tid],
        )
    latest, _, _ = odds_mod.get_token_sync_snapshot(
        [tid], reconcile_with_history=True, repair_ledger=False
    )
    assert latest[tid] == 75


def test_get_token_sync_snapshot_adds_fully_checked_and_skip_reason(duck):
    tid = "h" * 33 + "12"
    with odds_mod.get_connection() as conn:
        conn.execute(f"DELETE FROM {T_LED}")
        conn.execute(f"DELETE FROM {T_SK}")
        conn.execute(
            f"INSERT INTO {T_LED} (clobTokenId, last_sync_timestamp, fully_checked) VALUES (?, 10, TRUE)",
            [tid],
        )
        conn.execute(
            f"INSERT INTO {T_SK} (clobTokenId, reason) VALUES (?, 'skip')",
            [tid],
        )
    latest, fully, skips = odds_mod.get_token_sync_snapshot(
        [tid], reconcile_with_history=False, repair_ledger=False
    )
    assert latest[tid] == 10
    assert fully == {tid}
    assert skips == {tid: "skip"}


def test_get_token_sync_snapshot_can_include_scheduler_state(duck):
    tid = "hs" * 18
    with odds_mod.get_connection() as conn:
        conn.execute(f"DELETE FROM {T_LED}")
        conn.execute(
            f"""
            INSERT INTO {T_LED}
            (clobTokenId, last_sync_timestamp, fully_checked, last_checked_at, next_check_at, empty_run_streak)
            VALUES (?, 10, FALSE, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-02 00:00:00', 3)
            """,
            [tid],
        )
    latest, fully, skips, scheduler = odds_mod.get_token_sync_snapshot(
        [tid],
        include_scheduler_state=True,
    )
    assert latest[tid] == 10
    assert fully == set()
    assert skips == {}
    assert scheduler[tid].empty_run_streak == 3
    assert scheduler[tid].last_checked_at is not None
    assert scheduler[tid].next_check_at is not None


def test_get_token_sync_snapshot_reconcile_without_repair_updates_latest_only(duck):
    tid = "i" * 33 + "12"
    with odds_mod.get_connection() as conn:
        conn.execute(f"DELETE FROM {T_OH}")
        conn.execute(f"DELETE FROM {T_LED}")
        conn.execute(
            f"INSERT INTO {T_OH} (clobTokenId, timestamp, price) VALUES (?, 100, 0.5)",
            [tid],
        )
        conn.execute(
            f"INSERT INTO {T_LED} (clobTokenId, last_sync_timestamp) VALUES (?, 10)",
            [tid],
        )
    latest, _, _ = odds_mod.get_token_sync_snapshot(
        [tid], reconcile_with_history=True, repair_ledger=False
    )
    assert latest[tid] == 100
    with odds_mod.get_connection() as conn:
        ledger_ts = conn.execute(
            f"SELECT last_sync_timestamp FROM {T_LED} WHERE clobTokenId = ?",
            [tid],
        ).fetchone()[0]
    assert ledger_ts == 10


def test_save_odds_bulk_appender_and_upsert_with_fake_appender(monkeypatch, duck):
    appended = []

    class FakeAppender:
        def __init__(self, conn, table):
            self.table = table

        def append(self, row):
            appended.append((self.table, tuple(row)))

        def close(self):
            appended.append((self.table, "closed"))

    monkeypatch.setattr(duckdb, "Appender", FakeAppender, raising=False)
    with odds_mod.get_connection() as conn:
        odds_mod.save_odds_bulk_appender([("app", 1, 0.1)], conn)
        odds_mod.save_odds_bulk_upsert([("up", 2, 0.2)], conn, assume_deduped=False)
    hist_rows = [r for r in appended if r[0] == T_OH and r[1] != "closed"]
    assert len(hist_rows) == 1
    assert hist_rows[0][1][:3] == ("app", 1, 0.1)
    assert len(hist_rows[0][1]) == 4  # ingested_at
    stg_rows = [r for r in appended if r[0] == "_odds_staging" and r[1] != "closed"]
    assert stg_rows[0][1][:3] == ("up", 2, 0.2)
    assert len(stg_rows[0][1]) == 4


def test_save_sync_status_batch_preserves_fully_checked(duck):
    tid = "k" * 33 + "12"
    with odds_mod.get_connection() as conn:
        conn.execute(
            f"INSERT INTO {T_LED} (clobTokenId, last_sync_timestamp, fully_checked) VALUES (?, 1, TRUE)",
            [tid],
        )
    odds_mod.save_sync_status_batch([(tid, 999)])
    with odds_mod.get_connection() as conn:
        row = conn.execute(
            f"SELECT last_sync_timestamp, fully_checked FROM {T_LED} WHERE clobTokenId = ?",
            [tid],
        ).fetchone()
    assert row[0] == 999
    assert row[1] is True


def test_save_sync_status_batch_cursor_is_monotonic(duck):
    tid = "l" * 33 + "12"
    odds_mod.save_sync_status_batch([(tid, 100)])
    odds_mod.save_sync_status_batch([(tid, 50)])
    with odds_mod.get_connection() as conn:
        ts = conn.execute(
            f"SELECT last_sync_timestamp FROM {T_LED} WHERE clobTokenId = ?",
            [tid],
        ).fetchone()[0]
    assert ts == 100


def test_save_token_sync_state_batch_updates_scheduler_columns(duck):
    tid = "m" * 33 + "12"
    checked_at = odds_mod.datetime(2024, 1, 1, tzinfo=odds_mod.timezone.utc)
    next_check_at = odds_mod.datetime(2024, 1, 2, tzinfo=odds_mod.timezone.utc)
    odds_mod.save_token_sync_state_batch(
        [(tid, 100, checked_at, next_check_at, 2, False)]
    )
    with odds_mod.get_connection() as conn:
        row = conn.execute(
            f"""
            SELECT last_sync_timestamp, last_checked_at, next_check_at, empty_run_streak, fully_checked
            FROM {T_LED}
            WHERE clobTokenId = ?
            """,
            [tid],
        ).fetchone()
    assert row[0] == 100
    assert row[1] is not None
    assert row[2] is not None
    assert row[3] == 2
    assert row[4] is False


def test_save_token_sync_state_batch_clears_next_check_when_fully_checked(duck):
    tid = "n" * 33 + "12"
    checked_at = odds_mod.datetime(2024, 1, 1, tzinfo=odds_mod.timezone.utc)
    next_check_at = odds_mod.datetime(2024, 1, 2, tzinfo=odds_mod.timezone.utc)
    odds_mod.save_token_sync_state_batch(
        [(tid, 50, checked_at, next_check_at, 2, False)]
    )
    odds_mod.save_token_sync_state_batch([(tid, 75, checked_at, None, 0, True)])
    with odds_mod.get_connection() as conn:
        row = conn.execute(
            f"""
            SELECT last_sync_timestamp, next_check_at, empty_run_streak, fully_checked
            FROM {T_LED}
            WHERE clobTokenId = ?
            """,
            [tid],
        ).fetchone()
    assert row[0] == 75
    assert row[1] is None
    assert row[2] == 0
    assert row[3] is True


def test_save_skipped_tokens_preserves_created_at_on_reason_change(duck):
    tid = "p" * 33 + "12"
    odds_mod.save_skipped_tokens([(tid, "first")])
    with odds_mod.get_connection() as conn:
        created_before = conn.execute(
            f"SELECT created_at FROM {T_SK} WHERE clobTokenId = ?",
            [tid],
        ).fetchone()[0]
    time.sleep(0.02)
    odds_mod.save_skipped_tokens([(tid, "second")])
    with odds_mod.get_connection() as conn:
        created_after, reason = conn.execute(
            f"SELECT created_at, reason FROM {T_SK} WHERE clobTokenId = ?",
            [tid],
        ).fetchone()
    assert reason == "second"
    assert created_after == created_before


def test_reconcile_ledger_preserves_fully_checked(duck):
    tid = "q" * 33 + "12"
    with odds_mod.get_connection() as conn:
        conn.execute(
            f"INSERT INTO {T_LED} (clobTokenId, last_sync_timestamp, fully_checked) VALUES (?, 10, TRUE)",
            [tid],
        )
        conn.execute(
            f"INSERT INTO {T_OH} (clobTokenId, timestamp, price) VALUES (?, 100, 0.5)",
            [tid],
        )
    odds_mod.reconcile_token_sync_ledger_from_history()
    with odds_mod.get_connection() as conn:
        row = conn.execute(
            f"SELECT last_sync_timestamp, fully_checked FROM {T_LED} WHERE clobTokenId = ?",
            [tid],
        ).fetchone()
    assert row[0] == 100
    assert row[1] is True
