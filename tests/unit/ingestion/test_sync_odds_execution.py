"""Drive odds.sync_odds main loop with executor/writer patched for coverage."""

from __future__ import annotations

from queue import Queue
from unittest.mock import patch

import pytest

pytest.importorskip("duckdb")

from tests.integration.ingestion._odds_sync_harness import (
    FakeClock,
    ImmediatePoolNoShutdown,
    NeverDonePool,
    NoShutdownNeverDonePool,
    NoThread,
    immediate_executor,
    make_token_plan,
    patch_guardrail_clock,
    raw_snapshot,
)

from oddsfox.config._reload_settings import reload_all_settings_modules
from oddsfox.ingestion.polymarket.odds import sync as odds_sync
from oddsfox.storage.duckdb.connection import polymarket_raw_tbl

_T_OH = polymarket_raw_tbl("odds_history")


def _plan():
    return make_token_plan()


def test_sync_odds_runs_main_loop(monkeypatch):
    plans = [_plan(), _plan()]

    def plan_iter(**kwargs):
        for p in plans:
            yield p

    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "snapshot_raw_layer", raw_snapshot)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)

    def fake_sync_plan(plan, *a, **k):
        return {
            "rows": 0,
            "windows": 1,
            "empty": True,
            "error": 0,
            "permanent_error": 0,
            "fully_checked": False,
        }

    monkeypatch.setattr(odds_sync, "_sync_token_plan", fake_sync_plan)

    monkeypatch.setattr(odds_sync, "Thread", NoThread)

    def fake_writer_loop(*a, **k):
        return None

    monkeypatch.setattr(odds_sync, "_writer_loop", fake_writer_loop)
    # Fake writer thread never drains the queue; real sync_odds calls join() which
    # would deadlock without task_done — skip join when testing the executor path.
    monkeypatch.setattr(Queue, "join", lambda self: None)

    with immediate_executor():
        odds_sync.sync_odds(
            max_workers=2,
            batch_size=1000,
            writer_flush_rows=1000,
            market_page_size=100,
            persist_run_metrics=True,
            auto_tune_rps=False,
            requests_per_second=5,
        )


def test_sync_odds_pool_without_shutdown_completes(monkeypatch):
    plans = [_plan()]

    def plan_iter(**kwargs):
        del kwargs
        for p in plans:
            yield p

    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "snapshot_raw_layer", raw_snapshot)
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
    monkeypatch.setattr(odds_sync, "Thread", NoThread)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *a, **k: None)
    monkeypatch.setattr(Queue, "join", lambda self: None)

    with patch.object(odds_sync, "ThreadPoolExecutor", ImmediatePoolNoShutdown):
        summary = odds_sync.sync_odds(
            max_workers=1,
            auto_tune_rps=False,
            persist_run_metrics=False,
        )

    assert summary["aborted"] is False


def test_writer_loop_smoke(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "wl.duckdb"))
    import importlib

    import oddsfox.storage.duckdb.connection as connection

    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    q = Queue()
    q.put(None)
    stats = {
        "saved": 0,
        "saved_daily_rows": 0,
        "deduped": 0,
        "sync_rows": 0,
        "skip_rows": 0,
        "full_rows": 0,
        "invalid_ts_dropped": 0,
        "invalid_price_dropped": 0,
        "queue_high_watermark": 0,
    }
    odds_sync._writer_loop(q, 1000, stats, [])


def test_flush_writer_buffers_rollback(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "rb.duckdb"))
    import importlib

    import oddsfox.storage.duckdb.connection as connection

    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    buf = odds_sync.WriterBuffers(
        odds_map={("t", 1): 0.5},
        state_buffer=[("t", 1, None, None, 0, False)],
        skip_buffer=[],
    )
    st = {
        "saved": 0,
        "saved_daily_rows": 0,
        "deduped": 0,
        "sync_rows": 0,
        "skip_rows": 0,
        "full_rows": 0,
        "invalid_ts_dropped": 0,
        "invalid_price_dropped": 0,
    }

    class BadConn:
        def execute(self, *a, **k):
            raise RuntimeError("rollback path")

        def executemany(self, *a, **k):
            raise RuntimeError("x")

    with pytest.raises(RuntimeError):
        odds_sync._flush_writer_buffers(BadConn(), buf, st, 1, force=True)


def test_flush_writer_buffers_atomic_rollback_odds_and_state(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "atomic.duckdb"))
    import importlib

    import oddsfox.storage.duckdb.connection as connection

    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    token_id = "k" * 33 + "12"
    buf = odds_sync.WriterBuffers(
        odds_map={(token_id, 1): 0.5},
        state_buffer=[(token_id, 1, None, None, 0, False)],
        skip_buffer=[],
    )
    st = {
        "saved": 0,
        "saved_daily_rows": 0,
        "deduped": 0,
        "sync_rows": 0,
        "skip_rows": 0,
        "full_rows": 0,
        "invalid_ts_dropped": 0,
        "invalid_price_dropped": 0,
    }

    def fail_state_upsert(_rows, _conn):
        raise RuntimeError("state upsert failed")

    def simple_odds_upsert(records, conn, assume_deduped=False):
        del assume_deduped
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO {_T_OH}
            (clobTokenId, timestamp, price, ingested_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [(token_id, ts, price) for token_id, ts, price in records],
        )

    with connection.get_connection() as conn:
        with pytest.raises(RuntimeError, match="state upsert failed"):
            odds_sync._flush_writer_buffers(
                conn,
                buf,
                st,
                1,
                force=True,
                save_odds_bulk_upsert_fn=simple_odds_upsert,
                upsert_token_sync_state_batch_fn=fail_state_upsert,
            )

    with connection.get_connection() as conn:
        odds_count = conn.execute(f"SELECT count(*) FROM {_T_OH}").fetchone()[0]
    assert odds_count == 0
    assert st["sync_rows"] == 0


def test_sync_odds_no_progress_hard_timeout_aborts_and_persists_metrics(monkeypatch):
    plan = _plan()
    captured_metrics = {}
    clock = FakeClock()

    def plan_iter(**kwargs):
        del kwargs
        yield plan
        return (odds_sync.PlanningState(plans=1), {})

    def fake_wait(*args, **kwargs):
        del args, kwargs
        clock.advance(1.1)
        return set(), set()

    patch_guardrail_clock(monkeypatch, clock)
    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "snapshot_raw_layer", raw_snapshot)
    monkeypatch.setattr(odds_sync.time, "monotonic", clock)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "Thread", NoThread)
    monkeypatch.setattr(Queue, "join", lambda self: None)
    monkeypatch.setattr(
        Queue,
        "put_nowait",
        lambda self, item: (_ for _ in ()).throw(RuntimeError("queue full")),
    )
    monkeypatch.setattr(odds_sync, "ThreadPoolExecutor", NeverDonePool)
    monkeypatch.setattr(odds_sync, "wait", fake_wait)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(
        odds_sync,
        "save_sync_run_metrics",
        lambda task, metrics: captured_metrics.update(metrics),
    )

    with pytest.raises(odds_sync.NoProgressTimeoutError):
        odds_sync.sync_odds(
            max_workers=1,
            auto_tune_rps=False,
            persist_run_metrics=True,
            progress_poll_seconds=1,
            no_progress_soft_timeout_seconds=None,
            no_progress_hard_timeout_seconds=1,
            client_factory=lambda: object(),
            rate_limiter_factory=lambda r: None,
        )

    assert captured_metrics.get("aborted") is True
    assert captured_metrics.get("abort_reason")
    assert captured_metrics.get("max_idle_seconds", 0) >= 1
    assert "soft_warning_count" in captured_metrics


def test_sync_odds_no_progress_timeout_without_shutdown_method(monkeypatch):
    plan = _plan()
    clock = FakeClock()

    def plan_iter(**kwargs):
        del kwargs
        yield plan
        return (odds_sync.PlanningState(plans=1), {})

    def fake_wait(*args, **kwargs):
        del args, kwargs
        clock.advance(1.1)
        return set(), set()

    patch_guardrail_clock(monkeypatch, clock)
    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "snapshot_raw_layer", raw_snapshot)
    monkeypatch.setattr(odds_sync.time, "monotonic", clock)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "Thread", NoThread)
    monkeypatch.setattr(Queue, "join", lambda self: None)
    monkeypatch.setattr(odds_sync, "ThreadPoolExecutor", NoShutdownNeverDonePool)
    monkeypatch.setattr(odds_sync, "wait", fake_wait)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)

    with pytest.raises(odds_sync.NoProgressTimeoutError):
        odds_sync.sync_odds(
            max_workers=1,
            auto_tune_rps=False,
            persist_run_metrics=True,
            progress_poll_seconds=1,
            no_progress_soft_timeout_seconds=None,
            no_progress_hard_timeout_seconds=1,
            client_factory=lambda: object(),
            rate_limiter_factory=lambda r: None,
        )


def test_sync_odds_timeout_transient_finishes_without_abort(monkeypatch):
    plan = _plan()

    def plan_iter(**kwargs):
        del kwargs
        yield plan
        return (odds_sync.PlanningState(plans=1), {})

    def fake_sync_plan(*args, **kwargs):
        del args, kwargs
        return {
            "rows": 0,
            "windows": 1,
            "empty": True,
            "error": 1,
            "permanent_error": 0,
            "fully_checked": False,
        }

    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "snapshot_raw_layer", raw_snapshot)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(odds_sync, "_sync_token_plan", fake_sync_plan)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "Thread", NoThread)
    monkeypatch.setattr(Queue, "join", lambda self: None)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)

    with immediate_executor():
        summary = odds_sync.sync_odds(
            max_workers=1,
            auto_tune_rps=False,
            persist_run_metrics=True,
            progress_poll_seconds=1,
            client_factory=lambda: object(),
            rate_limiter_factory=lambda r: None,
        )

    assert summary["aborted"] is False
    assert summary["totals"]["processed_tokens"] == 1
    assert summary["totals"]["error"] == 1
    assert summary["totals"]["permanent_error"] == 0


def test_sync_odds_waiting_heartbeat_includes_oldest_inflight(monkeypatch):
    plan = _plan()
    seen_payloads = []
    wait_state = {"calls": 0}
    clock = FakeClock()

    def plan_iter(**kwargs):
        del kwargs
        yield plan
        return (odds_sync.PlanningState(plans=1), {})

    def fake_wait(futures, return_when=None, timeout=None):
        del return_when, timeout
        wait_state["calls"] += 1
        if wait_state["calls"] == 1:
            clock.advance(1.1)
            return set(), set()
        return set(futures), set()

    def fake_sync_plan(*args, **kwargs):
        del args, kwargs
        return {
            "rows": 3,
            "windows": 1,
            "empty": False,
            "error": 0,
            "permanent_error": 0,
            "fully_checked": False,
        }

    patch_guardrail_clock(monkeypatch, clock)
    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "snapshot_raw_layer", raw_snapshot)
    monkeypatch.setattr(odds_sync.time, "monotonic", clock)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(odds_sync, "_sync_token_plan", fake_sync_plan)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "Thread", NoThread)
    monkeypatch.setattr(Queue, "join", lambda self: None)
    monkeypatch.setattr(odds_sync, "wait", fake_wait)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)

    with immediate_executor():
        odds_sync.sync_odds(
            max_workers=1,
            auto_tune_rps=False,
            persist_run_metrics=True,
            progress_poll_seconds=1,
            progress_log_interval_seconds=1,
            progress_callback=lambda phase, payload: seen_payloads.append(
                (phase, payload)
            ),
            client_factory=lambda: object(),
            rate_limiter_factory=lambda r: None,
        )

    heartbeat_payloads = [
        payload
        for phase, payload in seen_payloads
        if phase == "guardrail" and payload.get("phase") == "waiting_for_token_futures"
    ]
    assert heartbeat_payloads
    diagnostics = heartbeat_payloads[0]["diagnostics"]
    assert diagnostics["oldest_inflight_seconds"] >= 1.0
    assert diagnostics["oldest_inflight"][0]["market_id"] == "m"
    assert diagnostics["oldest_inflight"][0]["token_id_prefix"] == plan.token_id[:24]


def test_build_inflight_future_diagnostics_empty():
    diagnostics = odds_sync._build_inflight_future_diagnostics({})
    assert diagnostics == {
        "oldest_inflight_seconds": 0.0,
        "oldest_inflight": [],
    }
