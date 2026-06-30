"""Additional branch coverage for odds.sync helpers and edges."""

from __future__ import annotations

import json
from contextlib import contextmanager
from queue import Queue
from unittest.mock import MagicMock

import pytest

pytest.importorskip("duckdb")

from tests.unit.ingestion._odds_sync_harness import (
    NoThread,
    immediate_executor,
)

from oddsfox.ingestion.polymarket.odds import sync as odds_sync
from oddsfox.ingestion.polymarket.odds.fetch import (
    BadRequestError,
)


def test_dynamic_writer_flush_rows_qsize_exception():
    class BadQueue:
        maxsize = 100

        def qsize(self):
            raise RuntimeError("no size")

    assert odds_sync._dynamic_writer_flush_rows(2000, BadQueue()) == 2000


def test_dynamic_writer_flush_rows_utilization_branches():
    q = Queue(maxsize=20)
    for _ in range(16):  # 16/20 = 0.8
        q.put(1)
    out_high = odds_sync._dynamic_writer_flush_rows(8000, q)
    assert out_high == max(1000, 8000 // 4)

    q2 = Queue(maxsize=20)
    for _ in range(11):  # 11/20 = 0.55
        q2.put(1)
    out_mid = odds_sync._dynamic_writer_flush_rows(8000, q2)
    assert out_mid == max(1000, 8000 // 2)

    q3 = Queue(maxsize=100)
    for _ in range(5):  # 5/100 = 0.05
        q3.put(1)
    out_low = odds_sync._dynamic_writer_flush_rows(5000, q3)
    assert out_low == min(odds_sync.MAX_FLUSH_ROWS_CAP, 5000 * 2)


def test_fetch_window_auto_split_interval_branch(monkeypatch):
    calls = []

    def ft(*a, **k):
        start_ts = k["start_ts"]
        end_ts = k["end_ts"]
        token_id = a[1]
        min_window_seconds = 60  # must match test arg
        calls.append((start_ts, end_ts))
        span = end_ts - start_ts
        if span > min_window_seconds:
            raise BadRequestError("long", body="interval is too long", status=400)
        return [(token_id, end_ts - 1, 0.5)]

    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.odds.sync.fetch_token_history_with_retry",
        ft,
    )
    tid = "z" * 33 + "12"
    out = odds_sync._fetch_window_with_auto_split(
        MagicMock(),
        tid,
        0,
        200,
        1440,
        60,
    )
    assert out and len(calls) >= 2


def test_fetch_window_returns_none_on_transient_chunk(monkeypatch):
    def ft(*a, **k):
        return None

    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.odds.sync.fetch_token_history_with_retry",
        ft,
    )
    tid = "y" * 33 + "12"
    assert (
        odds_sync._fetch_window_with_auto_split(MagicMock(), tid, 0, 50, 1440, 10)
        is None
    )


def test_sync_token_plan_typeerror_propagates(monkeypatch):
    def ft(*a, **k):
        raise TypeError("bad signature")

    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.odds.sync._fetch_window_with_auto_split",
        ft,
    )
    q = Queue()
    plan = odds_sync.TokenPlan(
        token_id="q" * 33 + "12",
        market_id="m",
        is_closed=False,
        created_at_ts=1,
        start_ts=1,
        end_ts=50,
        fidelity=1440,
    )
    with pytest.raises(TypeError, match="bad signature"):
        odds_sync._sync_token_plan(
            plan,
            MagicMock(),
            q,
            window_seconds=100,
            writer_chunk_rows=1000,
            min_split_window_seconds=1,
        )


def test_sync_token_plan_permanent_error_puts_skip(monkeypatch):
    def ft(*a, **k):
        raise BadRequestError("bad", status=400, body="not interval")

    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.odds.sync._fetch_window_with_auto_split",
        ft,
    )
    q = Queue()
    plan = odds_sync.TokenPlan(
        token_id="p" * 33 + "12",
        market_id="m",
        is_closed=True,
        created_at_ts=1,
        start_ts=1,
        end_ts=50,
        fidelity=1440,
    )
    odds_sync._sync_token_plan(
        plan,
        MagicMock(),
        q,
        window_seconds=100,
        writer_chunk_rows=1000,
        min_split_window_seconds=1,
    )
    items = []
    while not q.empty():
        items.append(q.get_nowait())
    assert items


def test_sync_token_plan_transient_and_cursor_branches(monkeypatch):
    """had_transient_error True with max_contiguous; empty chunk continuation."""

    def ft(*a, **k):
        return None  # transient

    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.odds.sync._fetch_window_with_auto_split",
        ft,
    )
    q = Queue()
    plan = odds_sync.TokenPlan(
        token_id="r" * 33 + "12",
        market_id="m",
        is_closed=False,
        created_at_ts=1,
        start_ts=1,
        end_ts=4000,
        fidelity=1440,
    )
    odds_sync._sync_token_plan(
        plan,
        MagicMock(),
        q,
        window_seconds=2000,
        writer_chunk_rows=10000,
        min_split_window_seconds=1,
    )


def test_build_single_token_plan_all_skips():
    now = 1_800_000_000
    tok_dup = "d" * 33 + "12"
    seen = {tok_dup}
    _, sk, _ = odds_sync.build_single_token_plan(
        token_id=tok_dup,
        market_id="m",
        closed=False,
        created_ts=1,
        latest_timestamps={},
        fully_checked_tokens=set(),
        persisted_skips={},
        seen_tokens=seen,
        now_ts=now,
        fidelity=1440,
        force=False,
        rebuild_minutely=False,
        overlap_seconds=0,
        recent_seconds=0,
    )
    assert sk == "dup_token"

    bad = "short"
    _, sk2, inv = odds_sync.build_single_token_plan(
        token_id=bad,
        market_id="m",
        closed=False,
        created_ts=1,
        latest_timestamps={},
        fully_checked_tokens=set(),
        persisted_skips={},
        seen_tokens=set(),
        now_ts=now,
        fidelity=1440,
        force=False,
        rebuild_minutely=False,
        overlap_seconds=0,
        recent_seconds=0,
    )
    assert sk2 == "invalid_token" and inv

    tok = "e" * 33 + "12"
    _, sk3, _ = odds_sync.build_single_token_plan(
        token_id=tok,
        market_id="m",
        closed=False,
        created_ts=1,
        latest_timestamps={},
        fully_checked_tokens=set(),
        persisted_skips={tok: "x"},
        seen_tokens=set(),
        now_ts=now,
        fidelity=1440,
        force=False,
        rebuild_minutely=False,
        overlap_seconds=0,
        recent_seconds=0,
    )
    assert sk3 == "persisted_skip"


def test_build_token_plans_json_and_type_skips():
    markets = [
        ("m", "not-json", "2024-01-01 00:00:00", False),
        ("m2", json.dumps("x"), "2024-01-01 00:00:00", False),
        ("m3", json.dumps([]), "2024-01-01 00:00:00", False),
    ]
    plans, invalid, ctr = odds_sync.build_token_plans(
        markets,
        {},
        set(),
        {},
        1_800_000_000,
        "2020-01-01",
        1,
        True,
        True,
        0,
        0,
    )
    assert isinstance(plans, list)
    assert isinstance(ctr, dict)


def test_iter_token_plans_paged_reconcile_and_invalid_batch(monkeypatch):
    page = [
        (
            "mx",
            json.dumps(["f" * 33 + "12"]),
            "2024-06-01 00:00:00",
            False,
        )
    ]

    def iter_kw(**kwargs):
        yield page

    monkeypatch.setattr(odds_sync, "iter_markets_with_tokens", iter_kw)

    monkeypatch.setattr(
        odds_sync,
        "get_token_sync_snapshot",
        lambda ids, **kw: ({}, set(), {}),
    )
    seen_batches = []

    def on_inv(batch):
        seen_batches.append(batch)

    gen = odds_sync.iter_token_plans_paged(
        now_ts=1_900_000_000,
        clob_cutoff_date="2020-01-01",
        fidelity=1440,
        force=True,
        rebuild_minutely=True,
        overlap_minutes=0,
        skip_recent_minutes=0,
        market_page_size=10,
        reconcile_ledger=True,
        on_invalid_tokens_batch=on_inv,
    )
    plans = list(gen)
    assert isinstance(plans, list)


def test_maybe_auto_tune_rps_increase_and_getattr_rate(monkeypatch):
    class Lim:
        rate = 10.0

        def get_rate(self):
            return self.rate

        def set_rate(self, r):
            self.rate = r

    lim = Lim()
    st = {"last_total": 0, "last_429": 0, "last_error": 0}
    odds_sync._maybe_auto_tune_rps(
        limiter=lim,
        runtime_status={"total": 500, "429": 0, "error": 0},
        tune_state=st,
        window_requests=1,
        threshold_429=0.99,
        threshold_error=0.99,
        min_rps=1,
        max_rps=50,
    )


def test_maybe_auto_tune_get_rate_exception_uses_attr():
    class Lim:
        rate = 5.0

        def get_rate(self):
            raise RuntimeError("x")

        def set_rate(self, r):
            self.rate = r

    lim = Lim()
    odds_sync._maybe_auto_tune_rps(
        limiter=lim,
        runtime_status={"total": 50, "429": 10, "error": 0},
        tune_state={"last_total": 0, "last_429": 0, "last_error": 0},
        window_requests=1,
        threshold_429=0.0,
        threshold_error=0.99,
        min_rps=1,
        max_rps=20,
    )


def test_writer_loop_fatal_flush_and_final_error(monkeypatch, tmp_path):
    q: Queue = Queue()
    stats = {
        "saved": 0,
        "deduped": 0,
        "sync_rows": 0,
        "skip_rows": 0,
        "full_rows": 0,
        "invalid_ts_dropped": 0,
        "invalid_price_dropped": 0,
        "queue_high_watermark": 0,
    }
    fails: list = []

    class BadConn:
        def execute(self, *a, **k):
            raise RuntimeError("flush")

        def executemany(self, *a, **k):
            raise RuntimeError("x")

    @contextmanager
    def bad_gc():
        yield BadConn()

    monkeypatch.setattr(odds_sync, "get_connection", bad_gc)
    q.put(("odds", [("t" * 35, 1, 0.5)]))
    q.put(None)
    odds_sync._writer_loop(q, 1, stats, fails)
    assert fails


def test_flush_writer_buffers_early_exits():
    buf = odds_sync.WriterBuffers(odds_map={}, state_buffer=[], skip_buffer=[])
    st = {
        k: 0
        for k in (
            "saved",
            "deduped",
            "sync_rows",
            "skip_rows",
            "full_rows",
            "invalid_ts_dropped",
            "invalid_price_dropped",
        )
    }
    odds_sync._flush_writer_buffers(MagicMock(), buf, st, 1000, force=False)


def test_sync_odds_worker_cap_warning(monkeypatch):
    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        odds_sync,
        "snapshot_raw_layer",
        lambda: {
            "market_tokens_distinct_tokens": 0,
            "odds_history_distinct_tokens": 0,
            "token_odds_daily_distinct_tokens": 0,
            "ledger_distinct_tokens": 0,
            "ledger_fully_checked_tokens": 0,
            "token_sync_skips_distinct_tokens": 0,
            "market_tokens_without_history": 0,
            "history_tokens_without_market_tokens": 0,
            "token_sync_skips_by_reason": {},
        },
    )
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", lambda **kw: iter(()))
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(Queue, "join", lambda self: None)
    odds_sync.sync_odds(
        max_workers=9999,
        market_page_size=100,
        persist_run_metrics=False,
        auto_tune_rps=False,
        requests_per_second=5,
    )


def test_sync_odds_effective_max_rps_respects_configured_rps_when_auto_tune_max_none(
    monkeypatch,
):
    plans = [
        odds_sync.TokenPlan(
            token_id="b" * 33 + "12",
            market_id="m",
            is_closed=False,
            created_at_ts=1,
            start_ts=1,
            end_ts=100,
            fidelity=1440,
        )
    ]

    def plan_iter(**kw):
        for p in plans:
            yield p

    captured_max_rps: list[int] = []

    def capture_tune(*, max_rps, **kwargs):
        captured_max_rps.append(int(max_rps))

    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
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
    monkeypatch.setattr(Queue, "join", lambda self: None)
    monkeypatch.setattr(odds_sync, "Thread", NoThread)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *a, **k: None)
    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.odds.engine.pool.maybe_auto_tune_rps",
        capture_tune,
    )

    class Lim:
        rate = 40.0

        def get_rate(self):
            return self.rate

        def set_rate(self, r):
            self.rate = r

    with immediate_executor():
        odds_sync.sync_odds(
            max_workers=10,
            market_page_size=100,
            persist_run_metrics=False,
            auto_tune_rps=True,
            requests_per_second=40,
            auto_tune_max_rps=None,
            rate_limiter_factory=lambda rps: Lim(),
        )

    assert captured_max_rps
    assert captured_max_rps[0] >= 40


def test_sync_odds_auto_tune_max_rps_branch(monkeypatch):
    plans = [
        odds_sync.TokenPlan(
            token_id="a" * 33 + "12",
            market_id="m",
            is_closed=False,
            created_at_ts=1,
            start_ts=1,
            end_ts=100,
            fidelity=1440,
        )
    ]

    def plan_iter(**kw):
        for p in plans:
            yield p

    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
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
    monkeypatch.setattr(Queue, "join", lambda self: None)
    monkeypatch.setattr(odds_sync, "Thread", NoThread)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *a, **k: None)

    class Lim:
        rate = 5.0

        def get_rate(self):
            return self.rate

        def set_rate(self, r):
            self.rate = r

    with immediate_executor():
        odds_sync.sync_odds(
            max_workers=1,
            market_page_size=100,
            persist_run_metrics=False,
            auto_tune_rps=True,
            requests_per_second=3,
            auto_tune_max_rps=20,
            rate_limiter_factory=lambda rps: Lim(),
        )


def test_sync_odds_run_summary_includes_planning_context(monkeypatch):
    captured = {}
    plan = odds_sync.TokenPlan(
        token_id="p" * 33 + "12",
        market_id="m",
        is_closed=False,
        created_at_ts=1,
        start_ts=1,
        end_ts=10,
        fidelity=1440,
    )

    def plan_iter(**kw):
        yield plan
        return (odds_sync.PlanningState(plans=1), {})

    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        odds_sync,
        "snapshot_raw_layer",
        lambda: {
            "market_tokens_distinct_tokens": 2,
            "odds_history_distinct_tokens": 1,
            "token_odds_daily_distinct_tokens": 1,
            "ledger_distinct_tokens": 1,
            "ledger_fully_checked_tokens": 0,
            "token_sync_skips_distinct_tokens": 0,
            "market_tokens_without_history": 1,
            "history_tokens_without_market_tokens": 0,
            "token_sync_skips_by_reason": {},
        },
    )
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(
        odds_sync,
        "_sync_token_plan",
        lambda *a, **k: {
            "rows": 1,
            "windows": 1,
            "empty": False,
            "error": 0,
            "permanent_error": 0,
            "fully_checked": False,
        },
    )
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(
        odds_sync,
        "save_sync_run_metrics",
        lambda task, metrics, **kwargs: captured.update(
            {"task": task, "metrics": metrics}
        ),
    )
    monkeypatch.setattr(Queue, "join", lambda self: None)
    monkeypatch.setattr(odds_sync, "Thread", NoThread)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *a, **k: None)

    with immediate_executor():
        result = odds_sync.sync_odds(
            max_workers=1,
            market_page_size=100,
            persist_run_metrics=True,
            auto_tune_rps=False,
            requests_per_second=5,
            client_factory=lambda: MagicMock(),
            rate_limiter_factory=lambda r: None,
        )

    assert result["planning_context"]["planned_vs_market_tokens"] == 0.5
    assert captured["metrics"]["planning"]["plans"] == 1
    assert captured["metrics"]["planning_context"]["market_tokens_distinct_tokens"] == 2
