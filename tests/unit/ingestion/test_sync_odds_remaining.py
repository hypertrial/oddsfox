"""Targeted coverage for odds/sync.py, odds/fetch.py edges, and storage odds."""

from __future__ import annotations

import importlib
import json
from queue import Queue
from unittest.mock import MagicMock

import duckdb
import pytest

from oddsfox.config._reload_settings import reload_all_settings_modules

pytest.importorskip("duckdb")

from tests.unit.ingestion._odds_sync_harness import (
    FakeClock,
    NoThread,
    immediate_executor,
    patch_guardrail_clock,
)

from oddsfox.ingestion.polymarket.odds import sync as odds_sync
from oddsfox.ingestion.polymarket.odds.fetch import BadRequestError


def test_dynamic_writer_flush_rows_default_branch():
    q = Queue(maxsize=20)
    for _ in range(8):
        q.put(1)
    assert odds_sync._dynamic_writer_flush_rows(3000, q) == 3000


def test_fetch_window_stack_skip_zero_span():
    out = odds_sync._fetch_window_with_auto_split(
        MagicMock(),
        "a" * 33 + "12",
        10,
        10,
        1440,
        1,
    )
    assert out == []


def test_build_token_plans_skips_bad_created_at(monkeypatch):
    markets = [
        ("m1", '["t" * 33 + "12"]', None, False),
    ]
    plans, inv, ctr = odds_sync.build_token_plans(
        markets,
        latest_timestamps={},
        fully_checked_tokens=set(),
        persisted_skips={},
        now_ts=1_800_000_000,
        clob_cutoff_date="2020-01-01",
        fidelity=1440,
        force=True,
        rebuild_minutely=True,
        overlap_minutes=0,
        skip_recent_minutes=0,
    )
    assert plans == []


def test_build_token_plans_non_list_tokens():
    plans, inv, ctr = odds_sync.build_token_plans(
        [
            (
                "m1",
                '{"not": "list"}',
                "2024-01-01 00:00:00",
                False,
            ),
        ],
        latest_timestamps={},
        fully_checked_tokens=set(),
        persisted_skips={},
        now_ts=1_800_000_000,
        clob_cutoff_date="2020-01-01",
        fidelity=1440,
        force=True,
        rebuild_minutely=True,
        overlap_minutes=0,
        skip_recent_minutes=0,
    )
    assert plans == []


def test_iter_token_plans_paged_empty_tokens_list(monkeypatch):
    def pages():
        yield [
            (
                "m1",
                "[]",
                "2024-06-01 00:00:00",
                False,
            ),
        ]

    monkeypatch.setattr(odds_sync, "iter_markets_with_tokens", lambda **k: pages())
    monkeypatch.setattr(
        odds_sync,
        "get_token_sync_snapshot",
        lambda *a, **k: ({}, set(), {}),
    )
    gen = odds_sync.iter_token_plans_paged(
        now_ts=1_800_000_000,
        clob_cutoff_date="2020-01-01",
        fidelity=1440,
        force=True,
        rebuild_minutely=True,
        overlap_minutes=0,
        skip_recent_minutes=0,
        market_page_size=50,
        short_range_first=False,
    )
    assert list(gen) == []


def test_iter_token_plans_paged_reconcile_short_first_off(monkeypatch):
    tid = "b" * 33 + "12"

    def pages():
        yield [
            (
                "m1",
                json.dumps([tid]),
                "2024-06-01 00:00:00",
                False,
            ),
        ]

    monkeypatch.setattr(odds_sync, "iter_markets_with_tokens", lambda **k: pages())
    monkeypatch.setattr(
        odds_sync,
        "get_token_sync_snapshot",
        lambda *a, **k: ({tid: 1}, set(), {}),
    )
    seen = []

    def on_inv(batch):
        seen.extend(batch)

    gen = odds_sync.iter_token_plans_paged(
        now_ts=1_800_000_000,
        clob_cutoff_date="2020-01-01",
        fidelity=1440,
        force=True,
        rebuild_minutely=True,
        overlap_minutes=0,
        skip_recent_minutes=0,
        market_page_size=50,
        reconcile_ledger=True,
        short_range_first=False,
        on_invalid_tokens_batch=on_inv,
    )
    plans = list(gen)
    assert plans


def test_sync_token_plan_passes_extended_fetch_signature(monkeypatch):
    seen = {}

    def hook(status):
        return None

    def ft(*args):
        seen["args"] = args
        return [(args[1], 10, 0.5)]

    monkeypatch.setattr(odds_sync, "_fetch_window_with_auto_split", ft)
    q = Queue()
    plan = odds_sync.TokenPlan(
        token_id="c" * 33 + "12",
        market_id="m",
        is_closed=False,
        created_at_ts=1,
        start_ts=1,
        end_ts=20,
        fidelity=1440,
    )
    odds_sync._sync_token_plan(
        plan,
        MagicMock(),
        q,
        window_seconds=100,
        writer_chunk_rows=1000,
        min_split_window_seconds=1,
        status_hook=hook,
    )
    assert seen["args"][6] == odds_sync.DEFAULT_TRANSIENT_RETRIES
    assert seen["args"][7] == odds_sync.DEFAULT_TRANSIENT_BACKOFF_SECONDS
    assert seen["args"][8] is hook


def test_sync_token_plan_badrequest_skip(monkeypatch):
    def ft(*a, **k):
        raise BadRequestError("x", status=400, body="")

    monkeypatch.setattr(odds_sync, "_fetch_window_with_auto_split", ft)
    q = Queue()
    plan = odds_sync.TokenPlan(
        token_id="d" * 33 + "12",
        market_id="m",
        is_closed=False,
        created_at_ts=1,
        start_ts=1,
        end_ts=30,
        fidelity=1440,
    )
    odds_sync._sync_token_plan(
        plan,
        MagicMock(),
        q,
        window_seconds=50,
        writer_chunk_rows=1000,
        min_split_window_seconds=1,
    )


def test_sync_token_plan_cursor_transient_empty_rows(monkeypatch):
    def ft(*a, **k):
        return None

    monkeypatch.setattr(odds_sync, "_fetch_window_with_auto_split", ft)
    q = Queue()
    plan = odds_sync.TokenPlan(
        token_id="e" * 33 + "12",
        market_id="m",
        is_closed=False,
        created_at_ts=1,
        start_ts=1,
        end_ts=100,
        fidelity=1440,
    )
    result = odds_sync._sync_token_plan(
        plan,
        MagicMock(),
        q,
        window_seconds=40,
        writer_chunk_rows=10000,
        min_split_window_seconds=1,
    )
    assert result["error"] == 1
    assert result["permanent_error"] == 0
    item = q.get_nowait()
    assert item[0] == "token_state"
    token_id, cursor_ts, checked_at, next_check_at, empty_run_streak, fully_checked = (
        item[1][0]
    )
    assert token_id == plan.token_id
    assert cursor_ts is None
    assert next_check_at is not None
    assert next_check_at > checked_at
    assert empty_run_streak == 0
    assert fully_checked is False


def test_flush_writer_buffers_empty_buffers_noop():
    buf = odds_sync.WriterBuffers(odds_map={}, state_buffer=[], skip_buffer=[])
    odds_sync._flush_writer_buffers(
        MagicMock(),
        buf,
        {
            "saved": 0,
            "saved_daily_rows": 0,
            "sync_rows": 0,
            "full_rows": 0,
            "skip_rows": 0,
        },
        100,
        force=False,
    )


def test_flush_writer_buffers_rollback_on_error(monkeypatch):
    buf = odds_sync.WriterBuffers(
        odds_map={("t", 1): 0.5},
        state_buffer=[],
        skip_buffer=[],
    )
    bad = MagicMock()
    bad.execute.return_value = None
    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.odds.sync.save_odds_bulk_upsert",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("fail")),
    )
    with pytest.raises(RuntimeError):
        odds_sync._flush_writer_buffers(
            bad,
            buf,
            {
                "saved": 0,
                "saved_daily_rows": 0,
                "sync_rows": 0,
                "full_rows": 0,
                "skip_rows": 0,
            },
            1,
            force=True,
        )
    bad.execute.assert_any_call("ROLLBACK")


def test_apply_writer_invalid_ts_price_dedupe():
    st = {"invalid_ts_dropped": 0, "invalid_price_dropped": 0, "deduped": 0}
    buf = odds_sync.WriterBuffers(odds_map={}, state_buffer=[], skip_buffer=[])
    odds_sync._apply_writer_item(
        ("odds", [("t", 0, 0.5), ("t", 5, -1), ("t", 5, 1.5), ("t", 5, 0.5)]), buf, st
    )
    odds_sync._apply_writer_item(("odds", [("t", 5, 0.5)]), buf, st)
    assert st["invalid_ts_dropped"] >= 1
    assert st["invalid_price_dropped"] >= 2
    assert st["deduped"] >= 1


def test_maybe_auto_tune_rps_branches():
    class Limiter:
        def __init__(self):
            self.rate = 10.0

        def get_rate(self):
            return self.rate

        def set_rate(self, r):
            self.rate = r

    lim = Limiter()
    tune = {"last_total": 0, "last_429": 0, "last_error": 0}
    odds_sync._maybe_auto_tune_rps(
        limiter=None,
        runtime_status={"total": 0},
        tune_state=tune,
        window_requests=10,
        threshold_429=0.01,
        threshold_error=0.01,
        min_rps=1,
        max_rps=20,
    )
    odds_sync._maybe_auto_tune_rps(
        limiter=lim,
        runtime_status={"total": 5},
        tune_state=tune,
        window_requests=200,
        threshold_429=0.01,
        threshold_error=0.01,
        min_rps=1,
        max_rps=20,
    )
    odds_sync._maybe_auto_tune_rps(
        limiter=lim,
        runtime_status={"total": 250, "429": 50, "error": 0},
        tune_state={"last_total": 0, "last_429": 0, "last_error": 0},
        window_requests=50,
        threshold_429=0.01,
        threshold_error=0.01,
        min_rps=1,
        max_rps=20,
    )
    odds_sync._maybe_auto_tune_rps(
        limiter=lim,
        runtime_status={"total": 500, "429": 0, "error": 40},
        tune_state={"last_total": 250, "last_429": 50, "last_error": 0},
        window_requests=50,
        threshold_429=0.5,
        threshold_error=0.01,
        min_rps=1,
        max_rps=20,
    )
    odds_sync._maybe_auto_tune_rps(
        limiter=lim,
        runtime_status={"total": 800, "429": 50, "error": 40},
        tune_state={"last_total": 500, "last_429": 50, "last_error": 40},
        window_requests=50,
        threshold_429=0.99,
        threshold_error=0.99,
        min_rps=1,
        max_rps=100,
    )


def test_maybe_auto_tune_get_rate_fallback():
    class Lim:
        rate = 5.0

    tune = {"last_total": 0, "last_429": 0, "last_error": 0}
    odds_sync._maybe_auto_tune_rps(
        limiter=Lim(),
        runtime_status={"total": 300, "429": 0, "error": 0},
        tune_state=tune,
        window_requests=50,
        threshold_429=0.99,
        threshold_error=0.99,
        min_rps=1,
        max_rps=50,
    )


def test_sync_odds_no_plans_stopiteration(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "so.duckdb"))

    reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    conn._SCHEMA_LOGGED = False
    conn._SCHEMA_INITIALIZED = False
    importlib.reload(conn)
    conn.ensure_duck_db()

    def empty_plans():
        for _ in []:
            yield None
        return (odds_sync.PlanningState(), {"bad": "token"})

    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", lambda **k: empty_plans())
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda x: None)
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)
    odds_sync.sync_odds(
        max_workers=1,
        persist_run_metrics=True,
        client_factory=lambda: MagicMock(),
        rate_limiter_factory=lambda r: None,
    )


def test_sync_odds_worker_cap_and_rps_branches(monkeypatch):
    """Worker cap log + RPS branches without real writer/queue (avoids join deadlock)."""
    tid = "f" * 33 + "12"
    plan = odds_sync.TokenPlan(
        token_id=tid,
        market_id="m",
        is_closed=False,
        created_at_ts=1,
        start_ts=1,
        end_ts=10,
        fidelity=1440,
    )

    def plans():
        yield plan

    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", lambda **k: plans())
    monkeypatch.setattr(
        odds_sync,
        "_sync_token_plan",
        lambda *a, **k: {
            "rows": 0,
            "windows": 1,
            "empty": True,
            "error": 0,
            "permanent_error": 0,
            "fully_checked": False,
        },
    )
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(Queue, "join", lambda self: None)
    monkeypatch.setattr(odds_sync, "Thread", NoThread)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *a, **k: None)

    with immediate_executor():
        odds_sync.sync_odds(
            max_workers=odds_sync.MAX_WORKERS_CAP + 50,
            requests_per_second=None,
            auto_tune_max_rps=100,
            persist_run_metrics=True,
            empty_token_skip_runs=2,
            client_factory=lambda: MagicMock(),
            rate_limiter_factory=lambda r: odds_sync.RateLimiter(5),
        )


def test_writer_loop_fatal_flush_and_final(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "wl.duckdb"))

    reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    conn._SCHEMA_LOGGED = False
    conn._SCHEMA_INITIALIZED = False
    importlib.reload(conn)
    conn.ensure_duck_db()

    q: Queue = Queue()

    def bad_flush(*a, **k):
        raise RuntimeError("flush")

    monkeypatch.setattr(odds_sync, "_dynamic_writer_flush_rows", lambda *a, **k: 1)
    monkeypatch.setattr(odds_sync, "_flush_writer_buffers", bad_flush)
    failures: list = []
    stats = {
        "saved": 0,
        "sync_rows": 0,
        "full_rows": 0,
        "skip_rows": 0,
        "deduped": 0,
        "invalid_ts_dropped": 0,
        "invalid_price_dropped": 0,
        "queue_high_watermark": 0,
    }
    t = odds_sync.Thread(
        target=odds_sync._writer_loop,
        args=(q, 100, stats, failures),
    )
    t.start()
    q.put(("odds", [("t", 1, 0.5)]))
    q.put(None)
    t.join(timeout=5)
    assert failures


def test_save_odds_bulk_appender_with_appender(monkeypatch, tmp_path):
    from oddsfox.storage.duckdb import odds as odds_mod

    if not hasattr(duckdb, "Appender"):
        pytest.skip("DuckDB Appender not available")
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "app.duckdb"))

    reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    conn._SCHEMA_LOGGED = False
    conn._SCHEMA_INITIALIZED = False
    importlib.reload(conn)
    conn.ensure_duck_db()
    with odds_mod.get_connection() as c:
        odds_mod.save_odds_bulk_appender([("app", 3, 0.4)], c)


def test_save_odds_bulk_upsert_appender_staging(monkeypatch, tmp_path):
    from oddsfox.storage.duckdb import odds as odds_mod

    if not hasattr(duckdb, "Appender"):
        pytest.skip("Appender required")
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "stg.duckdb"))

    reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    conn._SCHEMA_LOGGED = False
    conn._SCHEMA_INITIALIZED = False
    importlib.reload(conn)
    conn.ensure_duck_db()
    with odds_mod.get_connection() as c:
        odds_mod.save_odds_bulk_upsert([("stg", 9, 0.7)] * 3, c, assume_deduped=False)


def test_sync_odds_soft_warning_then_progress_reset(monkeypatch):
    token_id = "f" * 33 + "12"
    clock = FakeClock()
    plan = odds_sync.TokenPlan(
        token_id=token_id,
        market_id="m",
        is_closed=False,
        created_at_ts=1,
        start_ts=1,
        end_ts=10,
        fidelity=1440,
    )

    def plans():
        yield plan
        return (odds_sync.PlanningState(plans=1), {})

    wait_state = {"calls": 0}

    def fake_wait(futures, return_when=None, timeout=None):
        del return_when, timeout
        wait_state["calls"] += 1
        if wait_state["calls"] == 1:
            clock.advance(1.1)
            return set(), set()
        return set(futures), set()

    patch_guardrail_clock(monkeypatch, clock)
    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync.time, "monotonic", clock)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", lambda **k: plans())
    monkeypatch.setattr(
        odds_sync,
        "_sync_token_plan",
        lambda *a, **k: {
            "rows": 3,
            "windows": 1,
            "empty": False,
            "error": 0,
            "permanent_error": 0,
            "fully_checked": False,
        },
    )
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(Queue, "join", lambda self: None)
    monkeypatch.setattr(odds_sync, "Thread", NoThread)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "wait", fake_wait)

    with immediate_executor():
        summary = odds_sync.sync_odds(
            max_workers=1,
            auto_tune_rps=False,
            persist_run_metrics=True,
            progress_poll_seconds=1,
            no_progress_soft_timeout_seconds=1,
            no_progress_hard_timeout_seconds=5,
            client_factory=lambda: object(),
            rate_limiter_factory=lambda r: None,
        )

    assert summary["aborted"] is False
    assert summary["soft_warning_count"] >= 1
    assert summary["totals"]["processed_tokens"] == 1
