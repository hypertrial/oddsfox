"""Unit tests for Polymarket odds sync writer and auto-tune."""

from __future__ import annotations

from contextlib import contextmanager
from queue import Empty

import pytest

pytest.importorskip("duckdb")

from oddsfox.ingestion.polymarket.odds import sync as odds_sync


def test_flush_writer_buffers_writes_all_buffer_types(monkeypatch):
    class FakeConn:
        def __init__(self):
            self.executed = []
            self.executemany_calls = []

        def execute(self, sql):
            self.executed.append(sql)

        def executemany(self, sql, rows):
            self.executemany_calls.append((sql, list(rows)))

    fake_conn = FakeConn()
    stats = {
        "saved": 0,
        "saved_daily_rows": 0,
        "sync_rows": 0,
        "full_rows": 0,
        "skip_rows": 0,
    }
    buffers = odds_sync.WriterBuffers(
        odds_map={("tok", 1): 0.1},
        state_buffer=[("tok", 1, None, None, 0, True), ("tok", 2, None, None, 0, True)],
        skip_buffer=[("tok", "a"), ("tok", "b")],
    )
    saved = []
    monkeypatch.setattr(
        odds_sync,
        "save_odds_bulk_upsert",
        lambda rows, conn, assume_deduped: saved.extend(rows),
    )
    monkeypatch.setattr(odds_sync, "refresh_token_odds_daily", lambda keys, conn: None)

    odds_sync._flush_writer_buffers(fake_conn, buffers, stats, 1, force=True)

    assert saved == [("tok", 1, 0.1)]
    assert stats == {
        "saved": 1,
        "saved_daily_rows": 0,
        "sync_rows": 1,
        "full_rows": 1,
        "skip_rows": 1,
    }
    assert len(buffers.dirty_daily_keys) == 1
    assert buffers == odds_sync.WriterBuffers(
        odds_map={},
        state_buffer=[],
        skip_buffer=[],
        dirty_daily_keys=buffers.dirty_daily_keys,
    )
    assert fake_conn.executed[0] == "BEGIN"
    assert fake_conn.executed[-1] == "COMMIT"


def test_refresh_dirty_daily_keys_dedupes_until_final_refresh(monkeypatch):
    class FakeConn:
        def execute(self, sql):
            return None

    stats = {
        "saved": 0,
        "saved_daily_rows": 0,
        "sync_rows": 0,
        "full_rows": 0,
        "skip_rows": 0,
    }
    buffers = odds_sync.WriterBuffers(
        odds_map={("tok", 1): 0.1, ("tok", 2): 0.2},
        state_buffer=[],
        skip_buffer=[],
    )
    monkeypatch.setattr(
        odds_sync,
        "save_odds_bulk_upsert",
        lambda rows, conn, assume_deduped: None,
    )
    odds_sync._flush_writer_buffers(FakeConn(), buffers, stats, 1, force=True)
    buffers.odds_map[("tok", 3)] = 0.3
    odds_sync._flush_writer_buffers(FakeConn(), buffers, stats, 1, force=True)

    refreshed = []
    monkeypatch.setattr(
        odds_sync,
        "refresh_token_odds_daily",
        lambda keys, conn: refreshed.extend(keys),
    )
    odds_sync._refresh_dirty_daily_keys(FakeConn(), buffers, stats)

    assert len(refreshed) == 1
    assert stats["saved_daily_rows"] == 1
    assert buffers.dirty_daily_keys == set()


def test_flush_writer_buffers_sync_branch_keeps_max_timestamp(monkeypatch):
    class FakeConn:
        def execute(self, sql):
            return None

        def executemany(self, sql, rows):
            self.rows = list(rows)

    fake_conn = FakeConn()
    stats = {
        "saved": 0,
        "saved_daily_rows": 0,
        "sync_rows": 0,
        "full_rows": 0,
        "skip_rows": 0,
    }
    buffers = odds_sync.WriterBuffers(
        odds_map={},
        state_buffer=[
            ("tok", 5, None, None, 0, False),
            ("tok", 2, None, None, 0, False),
        ],
        skip_buffer=[],
    )
    monkeypatch.setattr(
        odds_sync, "save_odds_bulk_upsert", lambda *args, **kwargs: None
    )
    odds_sync._flush_writer_buffers(fake_conn, buffers, stats, 1, force=True)
    assert fake_conn.rows == [("tok", 5, None, None, 0, False)]
    assert stats["sync_rows"] == 1


def test_flush_writer_buffers_state_only_without_skips(monkeypatch):
    class FakeConn:
        def execute(self, sql):
            return None

        def executemany(self, sql, rows):
            self.rows = list(rows)

    fake_conn = FakeConn()
    stats = {
        "saved": 0,
        "saved_daily_rows": 0,
        "sync_rows": 0,
        "full_rows": 0,
        "skip_rows": 0,
    }
    buffers = odds_sync.WriterBuffers(
        odds_map={},
        state_buffer=[("tok", 9, None, None, 1, False)],
        skip_buffer=[],
    )
    monkeypatch.setattr(
        odds_sync, "save_odds_bulk_upsert", lambda *args, **kwargs: None
    )
    odds_sync._flush_writer_buffers(fake_conn, buffers, stats, 1, force=True)
    assert fake_conn.rows == [("tok", 9, None, None, 1, False)]
    assert stats["sync_rows"] == 1
    assert stats["skip_rows"] == 0


def test_flush_writer_buffers_skip_only(monkeypatch):
    class FakeConn:
        def execute(self, sql):
            return None

    fake_conn = FakeConn()
    stats = {
        "saved": 0,
        "saved_daily_rows": 0,
        "sync_rows": 0,
        "full_rows": 0,
        "skip_rows": 0,
    }
    buffers = odds_sync.WriterBuffers(
        odds_map={},
        state_buffer=[],
        skip_buffer=[("tok", "why")],
    )
    captured = []
    monkeypatch.setattr(
        odds_sync, "save_odds_bulk_upsert", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        odds_sync,
        "upsert_skipped_tokens_batch",
        lambda rows, conn: captured.extend(rows),
    )
    odds_sync._flush_writer_buffers(fake_conn, buffers, stats, 1, force=True)
    assert captured == [("tok", "why")]
    assert stats["skip_rows"] == 1


def test_flush_writer_buffers_fully_checked_only(monkeypatch):
    class FakeConn:
        def execute(self, sql):
            return None

        def executemany(self, sql, rows):
            self.rows = list(rows)

    fake_conn = FakeConn()
    stats = {
        "saved": 0,
        "saved_daily_rows": 0,
        "sync_rows": 0,
        "full_rows": 0,
        "skip_rows": 0,
    }
    buffers = odds_sync.WriterBuffers(
        odds_map={},
        state_buffer=[("tok", None, None, None, 0, True)],
        skip_buffer=[],
    )
    monkeypatch.setattr(
        odds_sync, "save_odds_bulk_upsert", lambda *args, **kwargs: None
    )
    odds_sync._flush_writer_buffers(fake_conn, buffers, stats, 1, force=True)
    assert fake_conn.rows == [("tok", None, None, None, 0, True)]
    assert stats["full_rows"] == 1


def test_apply_writer_item_non_odds_buffers():
    stats = {"invalid_ts_dropped": 0, "invalid_price_dropped": 0, "deduped": 0}
    buffers = odds_sync.WriterBuffers(odds_map={}, state_buffer=[], skip_buffer=[])
    odds_sync._apply_writer_item(
        ("token_state", [("tok", 1, None, None, 0, True)]),
        buffers,
        stats,
    )
    odds_sync._apply_writer_item(("skipped_tokens", [("tok", "why")]), buffers, stats)
    assert buffers.state_buffer == [("tok", 1, None, None, 0, True)]
    assert buffers.skip_buffer == [("tok", "why")]


def test_apply_writer_item_ignores_unknown_op():
    stats = {"invalid_ts_dropped": 0, "invalid_price_dropped": 0, "deduped": 0}
    buffers = odds_sync.WriterBuffers(odds_map={}, state_buffer=[], skip_buffer=[])
    odds_sync._apply_writer_item(("unknown", [("tok", 1)]), buffers, stats)
    assert buffers == odds_sync.WriterBuffers(
        odds_map={}, state_buffer=[], skip_buffer=[]
    )


def test_writer_loop_empty_poll_then_final_flush(monkeypatch):
    class FakeQueue:
        def __init__(self):
            self.calls = 0
            self.maxsize = 10

        def get(self, timeout=None):
            self.calls += 1
            if self.calls == 1:
                raise Empty()
            return None

        def qsize(self):
            return 0

        def task_done(self):
            return None

    class FakeConn:
        pass

    calls = []

    @contextmanager
    def fake_connection():
        yield FakeConn()

    monkeypatch.setattr(odds_sync, "get_connection", fake_connection)
    monkeypatch.setattr(
        odds_sync, "_dynamic_writer_flush_rows", lambda *args, **kwargs: 123
    )
    monkeypatch.setattr(
        odds_sync,
        "_flush_writer_buffers",
        lambda conn, buffers, stats, rows, force=False: calls.append((rows, force)),
    )

    odds_sync._writer_loop(
        FakeQueue(),
        100,
        {
            "saved": 0,
            "deduped": 0,
            "sync_rows": 0,
            "skip_rows": 0,
            "full_rows": 0,
            "invalid_ts_dropped": 0,
            "invalid_price_dropped": 0,
            "queue_high_watermark": 0,
        },
        [],
    )

    assert calls == [(123, False), (100, True)]


def test_writer_loop_marks_fatal_after_empty_flush_failure(monkeypatch):
    class FakeQueue:
        def __init__(self):
            self.items = [("odds", [("tok", 1, 0.1)]), None]
            self.maxsize = 10
            self.task_done_calls = 0
            self.get_calls = 0

        def get(self, timeout=None):
            self.get_calls += 1
            if self.get_calls == 1:
                raise Empty()
            return self.items.pop(0)

        def qsize(self):
            return 1

        def task_done(self):
            self.task_done_calls += 1

    @contextmanager
    def fake_connection():
        yield object()

    monkeypatch.setattr(odds_sync, "get_connection", fake_connection)
    monkeypatch.setattr(
        odds_sync, "_dynamic_writer_flush_rows", lambda *args, **kwargs: 1
    )
    monkeypatch.setattr(
        odds_sync,
        "_flush_writer_buffers",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("flush fail")),
    )
    applied = []
    monkeypatch.setattr(
        odds_sync, "_apply_writer_item", lambda *args, **kwargs: applied.append(args)
    )
    failures = []
    odds_sync._writer_loop(
        FakeQueue(),
        100,
        {
            "saved": 0,
            "deduped": 0,
            "sync_rows": 0,
            "skip_rows": 0,
            "full_rows": 0,
            "invalid_ts_dropped": 0,
            "invalid_price_dropped": 0,
            "queue_high_watermark": 0,
        },
        failures,
    )
    assert failures
    assert applied == []


def test_writer_loop_empty_poll_after_fatal_error_skips_flush(monkeypatch):
    class FakeQueue:
        def __init__(self):
            self.responses = [Empty(), Empty(), None]
            self.maxsize = 10
            self.task_done_calls = 0

        def get(self, timeout=None):
            response = self.responses.pop(0)
            if isinstance(response, Exception):
                raise response
            return response

        def qsize(self):
            return 0

        def task_done(self):
            self.task_done_calls += 1

    @contextmanager
    def fake_connection():
        yield object()

    calls = []
    monkeypatch.setattr(odds_sync, "get_connection", fake_connection)
    monkeypatch.setattr(
        odds_sync, "_dynamic_writer_flush_rows", lambda *args, **kwargs: 1
    )

    def bad_flush(*args, **kwargs):
        calls.append(kwargs.get("force", False))
        raise RuntimeError("flush fail")

    monkeypatch.setattr(odds_sync, "_flush_writer_buffers", bad_flush)
    failures = []
    odds_sync._writer_loop(
        FakeQueue(),
        100,
        {
            "saved": 0,
            "deduped": 0,
            "sync_rows": 0,
            "skip_rows": 0,
            "full_rows": 0,
            "invalid_ts_dropped": 0,
            "invalid_price_dropped": 0,
            "queue_high_watermark": 0,
        },
        failures,
    )
    assert len(failures) == 1
    assert calls == [False]


def test_maybe_auto_tune_rate_zero_and_small_delta():
    class ZeroLimiter:
        def get_rate(self):
            raise RuntimeError("bad")

    odds_sync._maybe_auto_tune_rps(
        limiter=ZeroLimiter(),
        runtime_status={"total": 100, "429": 0, "error": 0},
        tune_state={"last_total": 0, "last_429": 0, "last_error": 0},
        window_requests=1,
        threshold_429=0.0,
        threshold_error=0.0,
        min_rps=1,
        max_rps=10,
    )

    class SmallLimiter:
        def __init__(self):
            self.rate = 1.0
            self.set_calls = 0

        def get_rate(self):
            return self.rate

        def set_rate(self, value):
            self.set_calls += 1
            self.rate = value

    limiter = SmallLimiter()
    odds_sync._maybe_auto_tune_rps(
        limiter=limiter,
        runtime_status={"total": 100, "429": 0, "error": 0},
        tune_state={"last_total": 0, "last_429": 0, "last_error": 0},
        window_requests=1,
        threshold_429=0.99,
        threshold_error=0.99,
        min_rps=1,
        max_rps=2,
    )
    assert limiter.set_calls == 0


def test_maybe_auto_tune_increase_sets_rate():
    class Limiter:
        def __init__(self):
            self.rate = 10.0

        def get_rate(self):
            return self.rate

        def set_rate(self, value):
            self.rate = value

    limiter = Limiter()
    odds_sync._maybe_auto_tune_rps(
        limiter=limiter,
        runtime_status={"total": 100, "429": 0, "error": 0},
        tune_state={"last_total": 0, "last_429": 0, "last_error": 0},
        window_requests=1,
        threshold_429=1.0,
        threshold_error=1.0,
        min_rps=1,
        max_rps=50,
    )
    assert limiter.rate == 11.0


def test_maybe_auto_tune_delta_429_nonzero_keeps_rate(monkeypatch):
    class Limiter:
        def __init__(self):
            self.rate = 10.0
            self.set_calls = 0

        def get_rate(self):
            return self.rate

        def set_rate(self, value):
            self.set_calls += 1
            self.rate = value

    limiter = Limiter()
    odds_sync._maybe_auto_tune_rps(
        limiter=limiter,
        runtime_status={"total": 100, "429": 1, "error": 0},
        tune_state={"last_total": 0, "last_429": 0, "last_error": 0},
        window_requests=1,
        threshold_429=1.0,
        threshold_error=1.0,
        min_rps=1,
        max_rps=50,
    )
    assert limiter.set_calls == 0
