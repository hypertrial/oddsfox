"""Integration tests for sync_odds orchestration paths."""

from __future__ import annotations

from queue import Queue

import pytest

pytest.importorskip("duckdb")

from tests.unit.ingestion._odds_sync_harness import (
    FakePbar,
    ImmediatePool,
    ImmediateThread,
    make_plan,
)

from oddsfox.ingestion.polymarket.odds import sync as odds_sync


def test_sync_odds_covers_invalid_token_persist_worker_cache_and_budget_updates(
    monkeypatch,
):
    created_clients = []
    saved_skips = []
    budgets = {"tok_error": 5, "tok_rows": 5}

    plans = [
        make_plan("tok_error"),
        make_plan("tok_rows"),
        make_plan("tok_empty"),
        make_plan("tok_perm"),
    ]

    def plan_iter(**kwargs):
        kwargs["on_invalid_tokens_batch"]([])
        kwargs["on_invalid_tokens_batch"]([("dup", "first")])
        for plan in plans:
            yield plan
        return (
            odds_sync.PlanningState(plans=len(plans), invalid_token=1),
            {"dup": "first", "tail": "second"},
        )

    def fake_sync_plan(plan, get_worker_client, write_queue, *rest):
        get_worker_client()
        get_worker_client()
        status_hook = rest[-1]
        if plan.token_id == "tok_error":
            status_hook(429)
            status_hook(500)
            raise RuntimeError("boom")
        if plan.token_id == "tok_rows":
            status_hook(200)
            return {
                "rows": 2,
                "windows": 1,
                "empty": False,
                "error": 0,
                "permanent_error": 0,
                "fully_checked": False,
            }
        if plan.token_id == "tok_empty":
            status_hook(200)
            return {
                "rows": 0,
                "windows": 1,
                "empty": True,
                "error": 0,
                "permanent_error": 0,
                "fully_checked": False,
            }
        status_hook(500)
        return {
            "rows": 0,
            "windows": 1,
            "empty": True,
            "error": 1,
            "permanent_error": 0,
            "fully_checked": False,
        }

    class FakeThread:
        def __init__(self, target=None, args=(), daemon=None):
            self.target = target
            self.args = args

        def start(self):
            return None

        def join(self, timeout=None):
            return None

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
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(odds_sync, "_sync_token_plan", fake_sync_plan)
    monkeypatch.setattr(
        odds_sync, "save_sync_run_metrics", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        odds_sync, "save_skipped_tokens", lambda rows: saved_skips.extend(rows)
    )
    monkeypatch.setattr(odds_sync, "Thread", FakeThread)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *args, **kwargs: None)
    monkeypatch.setattr(odds_sync, "ThreadPoolExecutor", ImmediatePool)
    monkeypatch.setattr(
        odds_sync, "wait", lambda futures, return_when=None: (set(futures), set())
    )
    monkeypatch.setattr(odds_sync, "tqdm", FakePbar)
    monkeypatch.setattr(Queue, "join", lambda self: None)

    class Limiter:
        def __init__(self):
            self.rate = 5.0

        def get_rate(self):
            return self.rate

        def set_rate(self, new_rate):
            self.rate = new_rate

    odds_sync.sync_odds(
        max_workers=2,
        auto_tune_rps=True,
        auto_tune_window_requests=1,
        persist_run_metrics=True,
        empty_token_skip_runs=3,
        empty_token_skip_budgets=budgets,
        client_factory=lambda: created_clients.append(object()) or created_clients[-1],
        rate_limiter_factory=lambda r: Limiter(),
    )

    assert len(created_clients) == 1
    assert saved_skips == [("dup", "first"), ("tail", "second")]
    assert budgets == {"tok_error": 5, "tok_rows": 5}


def test_sync_odds_no_plan_duplicate_invalid_tokens_short_circuits(monkeypatch):
    saved = []

    def plan_iter(**kwargs):
        kwargs["on_invalid_tokens_batch"]([("dup", "reason")])
        if False:
            yield None
        return (odds_sync.PlanningState(), {"dup": "reason"})

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
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(
        odds_sync, "save_skipped_tokens", lambda rows: saved.extend(rows)
    )
    monkeypatch.setattr(
        odds_sync, "save_sync_run_metrics", lambda *args, **kwargs: None
    )

    odds_sync.sync_odds(
        max_workers=1,
        auto_tune_rps=False,
        persist_run_metrics=True,
        client_factory=lambda: object(),
        rate_limiter_factory=lambda r: None,
    )

    assert saved == [("dup", "reason")]


def test_sync_odds_final_rate_absent_when_limiter_has_no_rate_attr(monkeypatch):
    plan = make_plan("tok_final")

    def plan_iter(**kwargs):
        yield plan
        return (odds_sync.PlanningState(plans=1), {})

    class Limiter:
        def get_rate(self):
            return 5.0

        def set_rate(self, value):
            self.last = value

    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(
        odds_sync,
        "_sync_token_plan",
        lambda *args, **kwargs: {
            "rows": 0,
            "windows": 1,
            "empty": True,
            "error": 0,
            "permanent_error": 0,
            "fully_checked": False,
        },
    )
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        odds_sync,
        "Thread",
        type(
            "T",
            (),
            {
                "__init__": lambda self, **k: None,
                "start": lambda self: None,
                "join": lambda self, timeout=None: None,
            },
        ),
    )
    monkeypatch.setattr(odds_sync, "ThreadPoolExecutor", ImmediatePool)
    monkeypatch.setattr(
        odds_sync, "wait", lambda futures, return_when=None: (set(futures), set())
    )
    monkeypatch.setattr(odds_sync, "tqdm", FakePbar)
    monkeypatch.setattr(Queue, "join", lambda self: None)
    monkeypatch.setattr(
        odds_sync, "save_sync_run_metrics", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *args, **kwargs: None)

    odds_sync.sync_odds(
        max_workers=1,
        auto_tune_rps=False,
        persist_run_metrics=True,
        client_factory=lambda: object(),
        rate_limiter_factory=lambda r: Limiter(),
    )


def test_sync_odds_empty_results_with_zero_skip_runs_do_not_update_budget(monkeypatch):
    plan = make_plan("tok_zero")
    budgets = {"tok_zero": 7}

    def plan_iter(**kwargs):
        yield plan
        return (odds_sync.PlanningState(plans=1), {})

    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(
        odds_sync,
        "_sync_token_plan",
        lambda *args, **kwargs: {
            "rows": 0,
            "windows": 1,
            "empty": True,
            "error": 0,
            "permanent_error": 0,
            "fully_checked": False,
        },
    )
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        odds_sync,
        "Thread",
        type(
            "T",
            (),
            {
                "__init__": lambda self, **k: None,
                "start": lambda self: None,
                "join": lambda self, timeout=None: None,
            },
        ),
    )
    monkeypatch.setattr(odds_sync, "ThreadPoolExecutor", ImmediatePool)
    monkeypatch.setattr(
        odds_sync, "wait", lambda futures, return_when=None: (set(futures), set())
    )
    monkeypatch.setattr(odds_sync, "tqdm", FakePbar)
    monkeypatch.setattr(Queue, "join", lambda self: None)
    monkeypatch.setattr(
        odds_sync, "save_sync_run_metrics", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *args, **kwargs: None)

    odds_sync.sync_odds(
        max_workers=1,
        auto_tune_rps=False,
        persist_run_metrics=True,
        empty_token_skip_runs=0,
        empty_token_skip_budgets=budgets,
        client_factory=lambda: object(),
        rate_limiter_factory=lambda r: None,
    )

    assert budgets["tok_zero"] == 7


def test_sync_odds_raises_when_writer_thread_records_failure(monkeypatch):
    plan = make_plan("tok_writer")

    def plan_iter(**kwargs):
        yield plan
        return (odds_sync.PlanningState(plans=1), {})

    def writer_loop(_queue, _rows, _stats, failures):
        failures.append(RuntimeError("writer failed"))

    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(
        odds_sync,
        "_sync_token_plan",
        lambda *args, **kwargs: {
            "rows": 0,
            "windows": 1,
            "empty": True,
            "error": 0,
            "permanent_error": 0,
            "fully_checked": False,
        },
    )
    monkeypatch.setattr(odds_sync, "_writer_loop", writer_loop)
    monkeypatch.setattr(odds_sync, "Thread", ImmediateThread)
    monkeypatch.setattr(odds_sync, "ThreadPoolExecutor", ImmediatePool)
    monkeypatch.setattr(
        odds_sync, "wait", lambda futures, return_when=None: (set(futures), set())
    )
    monkeypatch.setattr(odds_sync, "tqdm", FakePbar)
    monkeypatch.setattr(Queue, "join", lambda self: None)
    monkeypatch.setattr(
        odds_sync, "save_sync_run_metrics", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *args, **kwargs: None)

    with pytest.raises(RuntimeError, match="writer failed"):
        odds_sync.sync_odds(
            max_workers=1,
            auto_tune_rps=False,
            persist_run_metrics=True,
            client_factory=lambda: object(),
            rate_limiter_factory=lambda r: None,
        )


def test_sync_odds_progress_bar_total_and_markets_postfix(monkeypatch):
    FakePbar.last_instance = None
    plans = [make_plan("tok_a"), make_plan("tok_b")]

    def plan_iter(**kwargs):
        for plan in plans:
            yield plan
        return (odds_sync.PlanningState(plans=len(plans)), {})

    def fake_sync_plan(plan, get_worker_client, write_queue, *rest):
        del get_worker_client, write_queue, rest
        return {
            "rows": 1,
            "windows": 1,
            "empty": False,
            "error": 0,
            "permanent_error": 0,
            "fully_checked": False,
        }

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
    monkeypatch.setattr(
        odds_sync,
        "count_candidate_market_tokens",
        lambda **kwargs: {"candidate_tokens": 42, "candidate_markets": 10},
    )
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(odds_sync, "_sync_token_plan", fake_sync_plan)
    monkeypatch.setattr(
        odds_sync, "save_sync_run_metrics", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *args, **kwargs: None)
    monkeypatch.setattr(odds_sync, "Thread", ImmediateThread)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *args, **kwargs: None)
    monkeypatch.setattr(odds_sync, "ThreadPoolExecutor", ImmediatePool)
    monkeypatch.setattr(
        odds_sync, "wait", lambda futures, return_when=None: (set(futures), set())
    )
    monkeypatch.setattr(odds_sync, "tqdm", FakePbar)
    monkeypatch.setattr(Queue, "join", lambda self: None)

    result = odds_sync.sync_odds(
        max_workers=1,
        auto_tune_rps=False,
        persist_run_metrics=False,
        client_factory=lambda: object(),
        rate_limiter_factory=lambda r: None,
    )

    pbar = FakePbar.last_instance
    assert pbar is not None
    assert pbar.total == 42
    assert pbar.updated == 2
    assert pbar.postfix is not None
    assert "markets" in pbar.postfix
    assert pbar.postfix["markets"] == "1/10"
    assert result["totals"]["distinct_markets"] == 1


def test_sync_odds_continues_when_candidate_count_fails(monkeypatch):
    FakePbar.last_instance = None
    plan = make_plan("tok_a")

    def plan_iter(**kwargs):
        yield plan
        return (odds_sync.PlanningState(plans=1), {})

    def fake_sync_plan(plan, get_worker_client, write_queue, *rest):
        del get_worker_client, write_queue, rest
        return {
            "rows": 0,
            "windows": 0,
            "empty": True,
            "error": 0,
            "permanent_error": 0,
            "fully_checked": False,
        }

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

    def _boom(**kwargs):
        del kwargs
        raise RuntimeError("count failed")

    monkeypatch.setattr(odds_sync, "count_candidate_market_tokens", _boom)
    monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    monkeypatch.setattr(odds_sync, "_sync_token_plan", fake_sync_plan)
    monkeypatch.setattr(
        odds_sync, "save_sync_run_metrics", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *args, **kwargs: None)
    monkeypatch.setattr(odds_sync, "Thread", ImmediateThread)
    monkeypatch.setattr(odds_sync, "_writer_loop", lambda *args, **kwargs: None)
    monkeypatch.setattr(odds_sync, "ThreadPoolExecutor", ImmediatePool)
    monkeypatch.setattr(
        odds_sync, "wait", lambda futures, return_when=None: (set(futures), set())
    )
    monkeypatch.setattr(odds_sync, "tqdm", FakePbar)
    monkeypatch.setattr(Queue, "join", lambda self: None)

    odds_sync.sync_odds(
        max_workers=1,
        auto_tune_rps=False,
        persist_run_metrics=False,
        client_factory=lambda: object(),
        rate_limiter_factory=lambda r: None,
    )

    pbar = FakePbar.last_instance
    assert pbar is not None
    assert pbar.total is None
