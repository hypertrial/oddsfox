from __future__ import annotations

import importlib
from queue import Queue
from unittest.mock import MagicMock, patch

import pytest
from tests.integration.ingestion._odds_sync_harness import make_runtime

from oddsfox.config._reload_settings import reload_all_settings_modules

pytest.importorskip("duckdb")

from oddsfox.ingestion.polymarket.odds import sync as odds_sync
from oddsfox.ingestion.polymarket.odds.fetch import BadRequestError
from oddsfox.storage.duckdb.connection import (
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)

_TOD = polymarket_raw_tbl("token_odds_daily")
_T_LED = polymarket_ops_tbl("token_sync_ledger")


def test_parse_created_at_variants():
    from datetime import datetime, timezone

    assert odds_sync._parse_created_at(None) is None
    assert (
        odds_sync._parse_created_at(datetime(2020, 1, 1, tzinfo=timezone.utc))
        is not None
    )
    assert odds_sync._parse_created_at("2020-01-01T12:30:45.123Z") is not None
    assert odds_sync._parse_created_at("2020-01-01 12:30:45") is not None


def test_iter_windows_and_rate_factory():
    assert list(odds_sync.iter_windows(0, 100, 40))[:2]
    assert odds_sync._default_rate_limiter_factory(None) is None
    assert odds_sync._default_rate_limiter_factory(5) is not None


def test_dynamic_writer_flush_rows():
    q = Queue(maxsize=10)
    for i in range(9):
        q.put(i)
    assert odds_sync._dynamic_writer_flush_rows(4000, q) < 4000


def test_dynamic_writer_flush_rows_no_maxsize():
    q = Queue()
    assert odds_sync._dynamic_writer_flush_rows(2000, q) == 2000


def test_empty_retry_next_check_supports_uncapped_and_capped_delay():
    checked_at = odds_sync.datetime(2024, 1, 1, tzinfo=odds_sync.timezone.utc)
    uncapped = odds_sync._empty_retry_next_check(
        checked_at,
        empty_run_streak=3,
        base_seconds=10,
        max_seconds=0,
    )
    capped = odds_sync._empty_retry_next_check(
        checked_at,
        empty_run_streak=3,
        base_seconds=10,
        max_seconds=20,
    )
    assert (uncapped - checked_at).total_seconds() == 40
    assert (capped - checked_at).total_seconds() == 20


def test_is_interval_too_long_error():
    e = BadRequestError("x", body="Interval is too long", status=400)
    assert odds_sync._is_interval_too_long_error(e) is True


def test_parse_cutoff_invalid():
    assert odds_sync._parse_cutoff_date("not-a-date").year == 2023


def test_build_single_token_plan_keys():
    now = 1_700_000_000
    tok = "t" * 33 + "12"
    seen = set()
    budgets = {tok: 1}
    plan, skip, inv = odds_sync.build_single_token_plan(
        token_id=tok,
        market_id="m",
        closed=False,
        created_ts=1_600_000_000,
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
        empty_token_skip_budgets=budgets,
        empty_token_skip_runs=1,
    )
    assert skip == "empty_cache_skip"

    tok2 = "u" * 33 + "12"
    plan2, sk2, _ = odds_sync.build_single_token_plan(
        token_id=tok2,
        market_id="m",
        closed=True,
        created_ts=1_600_000_000,
        latest_timestamps={},
        fully_checked_tokens={tok2},
        persisted_skips={},
        seen_tokens=set(),
        now_ts=now,
        fidelity=1440,
        force=False,
        rebuild_minutely=False,
        overlap_seconds=0,
        recent_seconds=999999999,
        empty_token_skip_budgets=None,
        empty_token_skip_runs=0,
    )
    assert sk2 == "closed_done" or plan2 is None


def test_iter_token_plans_paged_uses_current_market_iterator_signature(monkeypatch):
    seen = {}

    def iter_side(**kwargs):
        seen.update(kwargs)
        return iter(())

    monkeypatch.setattr(odds_sync, "iter_markets_with_tokens", iter_side)
    monkeypatch.setattr(
        odds_sync, "get_token_sync_snapshot", lambda *a, **k: ({}, set(), {})
    )
    gen = odds_sync.iter_token_plans_paged(
        now_ts=1_800_000_000,
        clob_cutoff_date="2024-01-01",
        fidelity=1440,
        force=True,
        rebuild_minutely=True,
        overlap_minutes=0,
        skip_recent_minutes=0,
        market_page_size=100,
    )
    assert list(gen) == []
    assert seen["json_array_only"] is True


def test_sync_token_plan_mocked():
    q = Queue()
    tid = "v" * 33 + "12"
    plan = odds_sync.TokenPlan(
        token_id=tid,
        market_id="m",
        is_closed=False,
        created_at_ts=1,
        start_ts=1,
        end_ts=100,
        fidelity=1440,
    )

    def fetch_stub(*a, **k):
        return [(tid, 50, 0.4)]

    with patch(
        "oddsfox.ingestion.polymarket.odds.sync._fetch_window_with_auto_split",
        side_effect=fetch_stub,
    ):
        odds_sync._sync_token_plan(
            plan,
            MagicMock(),
            q,
            window_seconds=200,
            writer_chunk_rows=10,
            min_split_window_seconds=1,
        )


def test_reconcile_odds_ledger_mocked(monkeypatch):
    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.odds.engine.ledger.ensure_duck_db",
        lambda: None,
    )
    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.odds.engine.ledger.snapshot_raw_layer",
        lambda: {"markets_rows": 1},
    )
    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.odds.engine.ledger.reconcile_token_sync_ledger_from_history",
        lambda: {"scanned_tokens": 1, "repaired_tokens": 0},
    )
    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.odds.engine.ledger.save_sync_run_metrics",
        lambda *a, **k: None,
    )
    odds_sync.reconcile_odds_ledger(persist_run_metrics=True)
    odds_sync.reconcile_odds_ledger(persist_run_metrics=False)


def test_init_db(monkeypatch):
    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    odds_sync.init_db()


def test_sync_odds_no_tokens_path(monkeypatch):
    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", lambda **kw: iter(()))
    monkeypatch.setattr(odds_sync, "save_sync_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    odds_sync.sync_odds(max_workers=1, market_page_size=100, persist_run_metrics=True)


def test_build_planning_context_uses_raw_snapshot():
    planning_state = odds_sync.PlanningState(plans=6, closed_done=2, recent_skip=1)
    context = odds_sync._build_planning_context(
        {
            "market_tokens_distinct_tokens": 10,
            "odds_history_distinct_tokens": 7,
            "token_odds_daily_distinct_tokens": 5,
            "ledger_distinct_tokens": 8,
            "ledger_fully_checked_tokens": 2,
            "token_sync_skips_distinct_tokens": 1,
            "market_tokens_without_history": 3,
            "history_tokens_without_market_tokens": 0,
            "token_sync_skips_by_reason": {"invalid token id format": 1},
        },
        planning_state,
        invalid_tokens=1,
    )
    assert context["planned_tokens"] == 6
    assert context["history_coverage_vs_market_tokens"] == 0.7
    assert context["token_sync_skips_by_reason"] == {"invalid token id format": 1}


def test_sync_odds_persists_planning_context(monkeypatch):
    captured = {}

    def empty_plans():
        if False:
            yield None
        return (odds_sync.PlanningState(plans=0), {"bad": "reason"})

    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        odds_sync,
        "snapshot_raw_layer",
        lambda: {
            "market_tokens_distinct_tokens": 4,
            "odds_history_distinct_tokens": 3,
            "token_odds_daily_distinct_tokens": 2,
            "ledger_distinct_tokens": 3,
            "ledger_fully_checked_tokens": 1,
            "token_sync_skips_distinct_tokens": 0,
            "market_tokens_without_history": 1,
            "history_tokens_without_market_tokens": 0,
            "token_sync_skips_by_reason": {},
        },
    )
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", lambda **kw: empty_plans())
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    monkeypatch.setattr(
        odds_sync,
        "save_sync_run_metrics",
        lambda task, metrics, **kwargs: captured.update(
            {"task": task, "metrics": metrics}
        ),
    )
    result = odds_sync.sync_odds(
        max_workers=1, market_page_size=100, persist_run_metrics=True
    )
    assert result["planning_context"]["market_tokens_distinct_tokens"] == 4
    assert captured["metrics"]["planning_context"]["planned_tokens"] == 0
    assert captured["metrics"]["invalid_tokens"] == 1


def test_writer_buffers_apply_and_flush(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "w.duckdb"))
    import oddsfox.storage.duckdb.connection as connection

    reload_all_settings_modules()
    connection._SCHEMA_INITIALIZED = False
    connection._SCHEMA_LOGGED = False
    importlib.reload(connection)
    connection.ensure_duck_db()

    buf = odds_sync.WriterBuffers(odds_map={}, state_buffer=[], skip_buffer=[])
    st = {
        "saved": 0,
        "saved_daily_rows": 0,
        "sync_rows": 0,
        "skip_rows": 0,
        "full_rows": 0,
        "deduped": 0,
        "invalid_ts_dropped": 0,
        "invalid_price_dropped": 0,
    }
    odds_sync._apply_writer_item(
        ("odds", [("t", 1, 0.5), ("t", 0, 0.5), ("t", 2, 2.0)]), buf, st
    )
    odds_sync._apply_writer_item(
        ("token_state", [("t", 1, None, None, 0, True)]),
        buf,
        st,
    )
    odds_sync._apply_writer_item(("skipped_tokens", [("t", "r")]), buf, st)

    with connection.get_connection() as conn:
        odds_sync._flush_writer_buffers(conn, buf, st, 1, force=True)
        odds_sync._refresh_dirty_daily_keys(conn, buf, st)
        daily_rows = conn.execute(f"select count(*) from {_TOD}").fetchone()[0]
    assert daily_rows == 1


def test_flush_writer_preserves_fully_checked_on_cursor_update(monkeypatch, tmp_path):
    """Cursor-only flushes must not clear fully_checked (operational upsert, not row replace)."""
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "fc.duckdb"))
    import oddsfox.storage.duckdb.connection as connection

    reload_all_settings_modules()
    connection._SCHEMA_INITIALIZED = False
    connection._SCHEMA_LOGGED = False
    importlib.reload(connection)
    connection.ensure_duck_db()

    tid = "j" * 33 + "12"
    with connection.get_connection() as conn:
        conn.execute(
            f"INSERT INTO {_T_LED} (clobTokenId, last_sync_timestamp, fully_checked) VALUES (?, 10, TRUE)",
            [tid],
        )

    buf = odds_sync.WriterBuffers(
        odds_map={},
        state_buffer=[(tid, 99, None, None, 0, False)],
        skip_buffer=[],
    )
    st = {
        "saved": 0,
        "saved_daily_rows": 0,
        "sync_rows": 0,
        "skip_rows": 0,
        "full_rows": 0,
    }
    with connection.get_connection() as conn:
        odds_sync._flush_writer_buffers(conn, buf, st, 1, force=True)

    with connection.get_connection() as conn:
        row = conn.execute(
            f"SELECT last_sync_timestamp, fully_checked FROM {_T_LED} WHERE clobTokenId = ?",
            [tid],
        ).fetchone()
    assert row[0] == 99
    assert row[1] is True


def test_fetch_window_split_raises_when_span_small(monkeypatch):
    def ft(*a, **k):
        raise BadRequestError("e", body="interval is too long", status=400)

    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.odds.sync.fetch_token_history_with_retry",
        ft,
    )
    with pytest.raises(BadRequestError):
        odds_sync._fetch_window_with_auto_split(
            MagicMock(),
            "w" * 33 + "12",
            0,
            30,
            1440,
            60,
        )


def test_sync_odds_rejects_fidelity_below_one():
    with pytest.raises(ValueError, match="at least 1"):
        odds_sync.sync_odds(max_workers=1, fidelity=0, persist_run_metrics=False)


def test_sync_odds_rejects_unknown_keyword():
    with pytest.raises(TypeError, match="unexpected keyword"):
        odds_sync.sync_odds(max_workers=1, unknown_option=object())


def test_sync_odds_runtime_instances_do_not_mutate_shared_modules():
    from oddsfox.ingestion.polymarket.odds import planning

    original_iter_due = planning.iter_due_market_tokens
    seen: list[str] = []

    def runtime_for(label: str):
        return make_runtime(
            ensure_duck_db=lambda: seen.append(f"{label}:ensure"),
            snapshot_raw_layer=lambda: {"label": label},
            iter_due_market_tokens=lambda **_kwargs: iter(()),
            iter_markets_with_tokens=lambda **_kwargs: iter(()),
            save_sync_run_metrics=lambda *args, **_kwargs: seen.append(
                f"{label}:metrics:{args[0]}"
            ),
        )

    odds_sync.sync_odds(max_workers=1, runtime=runtime_for("a"))
    odds_sync.sync_odds(max_workers=1, runtime=runtime_for("b"))

    assert seen == [
        "a:ensure",
        "a:metrics:sync_odds",
        "b:ensure",
        "b:metrics:sync_odds",
    ]
    assert planning.iter_due_market_tokens is original_iter_due


def test_sync_odds_accepts_explicit_plan_iterator_with_runtime():
    seen: list[str] = []
    runtime = make_runtime(
        ensure_duck_db=lambda: seen.append("ensure"),
        snapshot_raw_layer=lambda: {"ok": True},
        save_sync_run_metrics=lambda *args, **_kwargs: seen.append(args[0]),
    )

    def empty_plan_iterator(**_kwargs):
        if False:
            yield None
        return (odds_sync.PlanningState(plans=0), {})

    result = odds_sync.sync_odds(
        max_workers=1,
        runtime=runtime,
        plan_iterator_factory=empty_plan_iterator,
    )

    assert result["noop"] is True
    assert seen == ["ensure", "sync_odds"]


def test_odds_sync_all_excludes_private_helpers():
    assert [name for name in odds_sync.__all__ if name.startswith("_")] == []


def test_odds_sync_runtime_flat_property_accessors():
    from oddsfox.ingestion.polymarket.odds.deps import (
        replace_odds_sync_runtime,
    )

    runtime = make_runtime()
    flat_to_nested = {
        "fetch_window_with_auto_split_impl": runtime.execution.fetch_window_with_auto_split_impl,
        "fetch_token_history_with_retry": runtime.execution.fetch_token_history_with_retry,
        "default_rate_limiter_factory": runtime.execution.default_rate_limiter_factory,
        "sync_token_plan": runtime.execution.sync_token_plan,
        "get_connection": runtime.writer.get_connection,
        "refresh_token_odds_daily": runtime.writer.refresh_token_odds_daily,
        "save_odds_bulk_upsert": runtime.writer.save_odds_bulk_upsert,
        "upsert_skipped_tokens_batch": runtime.writer.upsert_skipped_tokens_batch,
        "upsert_token_sync_state_batch": runtime.writer.upsert_token_sync_state_batch,
        "dynamic_writer_flush_rows": runtime.writer.dynamic_writer_flush_rows,
        "flush_writer_buffers": runtime.writer.flush_writer_buffers,
        "apply_writer_item": runtime.writer.apply_writer_item,
        "writer_loop": runtime.writer.writer_loop,
        "reconcile_token_sync_ledger_from_history": (
            runtime.engine.reconcile_token_sync_ledger_from_history
        ),
    }
    for name, nested_value in flat_to_nested.items():
        assert getattr(runtime, name) is nested_value

    sentinel = object()
    updated = replace_odds_sync_runtime(runtime, sync_token_plan=sentinel)
    assert updated.execution.sync_token_plan is sentinel
    updated = replace_odds_sync_runtime(runtime, writer_loop=sentinel)
    assert updated.writer.writer_loop is sentinel

    with pytest.raises(AttributeError, match="Unknown OddsSyncRuntime field"):
        replace_odds_sync_runtime(runtime, not_a_field=sentinel)
