"""Unit tests for Polymarket odds sync planning."""

from __future__ import annotations

import pytest

pytest.importorskip("duckdb")

from oddsfox.ingestion.polymarket.odds import sync as odds_sync


def test_build_single_token_plan_budget_and_latest_branches():
    tok = "b" * 33 + "12"
    budgets = {tok: 2}
    plan, skip, _ = odds_sync.build_single_token_plan(
        token_id=tok,
        market_id="m",
        closed=False,
        created_ts=100,
        latest_timestamps={},
        fully_checked_tokens=set(),
        persisted_skips={},
        seen_tokens=set(),
        now_ts=200,
        fidelity=1440,
        force=False,
        rebuild_minutely=False,
        overlap_seconds=0,
        recent_seconds=0,
        empty_token_skip_budgets=budgets,
        empty_token_skip_runs=2,
    )
    assert plan is None
    assert skip == "empty_cache_skip"
    assert budgets[tok] == 1

    tok2 = "c" * 33 + "12"
    plan, skip, _ = odds_sync.build_single_token_plan(
        token_id=tok2,
        market_id="m",
        closed=False,
        created_ts=100,
        latest_timestamps={tok2: 150},
        fully_checked_tokens=set(),
        persisted_skips={},
        seen_tokens=set(),
        now_ts=200,
        fidelity=1440,
        force=False,
        rebuild_minutely=False,
        overlap_seconds=20,
        recent_seconds=10,
        empty_token_skip_budgets={},
        empty_token_skip_runs=2,
    )
    assert skip is None
    assert plan is not None
    assert plan.start_ts == 130

    tok3 = "d" * 33 + "12"
    _, skip, _ = odds_sync.build_single_token_plan(
        token_id=tok3,
        market_id="m",
        closed=False,
        created_ts=100,
        latest_timestamps={tok3: 195},
        fully_checked_tokens=set(),
        persisted_skips={},
        seen_tokens=set(),
        now_ts=200,
        fidelity=1440,
        force=False,
        rebuild_minutely=False,
        overlap_seconds=0,
        recent_seconds=10,
        empty_token_skip_budgets={},
        empty_token_skip_runs=0,
    )
    assert skip == "recent_skip"

    tok4 = "e" * 33 + "12"
    _, skip, _ = odds_sync.build_single_token_plan(
        token_id=tok4,
        market_id="m",
        closed=False,
        created_ts=250,
        latest_timestamps={},
        fully_checked_tokens=set(),
        persisted_skips={},
        seen_tokens=set(),
        now_ts=200,
        fidelity=1440,
        force=True,
        rebuild_minutely=False,
        overlap_seconds=0,
        recent_seconds=0,
        empty_token_skip_budgets={},
        empty_token_skip_runs=0,
    )
    assert skip == "already_current"


def test_build_token_plans_counts_preclob_invalid_and_valid():
    valid = "v" * 33 + "12"
    plans, invalid, counters = odds_sync.build_token_plans(
        [
            ("old", '["x"]', "2020-01-01 00:00:00", False),
            ("bad", '["short"]', "2024-01-01 00:00:00", False),
            ("good", f'["{valid}"]', "2024-01-01 00:00:00", False),
        ],
        latest_timestamps={},
        fully_checked_tokens=set(),
        persisted_skips={},
        now_ts=1_800_000_000,
        clob_cutoff_date="2023-01-01",
        fidelity=1440,
        force=True,
        rebuild_minutely=True,
        overlap_minutes=0,
        skip_recent_minutes=0,
    )
    assert counters["pre_clob_markets"] == 1
    assert counters["invalid_token"] == 1
    assert invalid == [("short", "invalid token id format")]
    assert len(plans) == 1


def test_iter_token_plans_paged_collects_invalids_and_done_value(monkeypatch):
    valid = "w" * 33 + "12"

    def pages():
        yield [
            ("pre", f'["{valid}"]', "2022-01-01 00:00:00", False),
            ("badjson", "{bad", "2024-01-01 00:00:00", False),
            ("noneonly", "[null]", "2024-01-01 00:00:00", False),
            ("mixed", f'[null, "short", "{valid}"]', "2024-01-01 00:00:00", False),
        ]

    monkeypatch.setattr(odds_sync, "iter_markets_with_tokens", lambda **kwargs: pages())
    monkeypatch.setattr(
        odds_sync,
        "get_token_sync_snapshot",
        lambda token_ids, **kwargs: ({}, set(), {}),
    )

    invalid_batches = []
    gen = odds_sync.iter_token_plans_paged(
        now_ts=1_900_000_000,
        clob_cutoff_date="2023-01-01",
        fidelity=1440,
        force=True,
        rebuild_minutely=True,
        overlap_minutes=0,
        skip_recent_minutes=0,
        market_page_size=10,
        on_invalid_tokens_batch=invalid_batches.append,
    )

    yielded = []
    done_value = None
    while True:
        try:
            yielded.append(next(gen))
        except StopIteration as done:
            done_value = done.value
            break

    planning_state, invalid_tokens = done_value
    assert len(yielded) == 1
    assert planning_state.pre_clob_markets == 1
    assert planning_state.invalid_token == 1
    assert invalid_batches == [[("short", "invalid token id format")]]
    assert invalid_tokens == {"short": "invalid token id format"}


def test_iter_token_plans_paged_skips_unparseable_created_at(monkeypatch):
    def pages():
        yield [("badtime", '["tok"]', None, False)]

    monkeypatch.setattr(odds_sync, "iter_markets_with_tokens", lambda **kwargs: pages())
    monkeypatch.setattr(
        odds_sync,
        "get_token_sync_snapshot",
        lambda token_ids, **kwargs: ({}, set(), {}),
    )
    assert (
        list(
            odds_sync.iter_token_plans_paged(
                now_ts=1_900_000_000,
                clob_cutoff_date="2023-01-01",
                fidelity=1440,
                force=True,
                rebuild_minutely=True,
                overlap_minutes=0,
                skip_recent_minutes=0,
                market_page_size=10,
            )
        )
        == []
    )


def test_iter_token_plans_paged_due_only_uses_due_iterator_and_scheduler_state(
    monkeypatch,
):
    token_id = "d" * 33 + "12"
    called = {"due": 0, "full": 0}

    def due_pages(**kwargs):
        called["due"] += 1
        yield [("m1", token_id, "2024-01-01 00:00:00", False)]

    def full_pages(**kwargs):
        called["full"] += 1
        yield []

    monkeypatch.setattr(odds_sync, "iter_due_market_tokens", due_pages)
    monkeypatch.setattr(odds_sync, "iter_markets_with_tokens", full_pages)
    monkeypatch.setattr(
        odds_sync,
        "get_token_sync_snapshot",
        lambda *args, **kwargs: (
            {token_id: 100},
            set(),
            {},
            {
                token_id: odds_sync.TokenSyncSchedulerState(
                    empty_run_streak=2,
                )
            },
        ),
    )
    plans = list(
        odds_sync.iter_token_plans_paged(
            now_ts=1_900_000_000,
            clob_cutoff_date="2023-01-01",
            fidelity=1440,
            force=False,
            rebuild_minutely=False,
            overlap_minutes=0,
            skip_recent_minutes=0,
            market_page_size=10,
        )
    )
    assert called == {"due": 1, "full": 0}
    assert len(plans) == 1
    assert plans[0].token_id == token_id
    assert plans[0].empty_run_streak == 2


def test_iter_token_plans_paged_due_only_skips_bad_rows(monkeypatch):
    token_id = "e" * 33 + "12"

    def due_pages(**kwargs):
        yield [
            ("badtime", token_id, None, False),
            ("old", token_id, "2022-01-01 00:00:00", False),
            ("blank", "   ", "2024-01-01 00:00:00", False),
            ("good", token_id, "2024-01-01 00:00:00", False),
        ]

    monkeypatch.setattr(odds_sync, "iter_due_market_tokens", due_pages)
    monkeypatch.setattr(
        odds_sync, "iter_markets_with_tokens", lambda **kwargs: iter(())
    )
    monkeypatch.setattr(
        odds_sync,
        "get_token_sync_snapshot",
        lambda *args, **kwargs: ({}, set(), {}, {}),
    )
    gen = odds_sync.iter_token_plans_paged(
        now_ts=1_900_000_000,
        clob_cutoff_date="2023-01-01",
        fidelity=1440,
        force=False,
        rebuild_minutely=False,
        overlap_minutes=0,
        skip_recent_minutes=0,
        market_page_size=10,
    )
    plans = []
    done_value = None
    while True:
        try:
            plans.append(next(gen))
        except StopIteration as done:
            done_value = done.value
            break
    planning_state, _ = done_value
    assert len(plans) == 1
    assert plans[0].market_id == "good"
    assert planning_state.pre_clob_markets == 1


def test_build_single_token_plan_minutely_backfill_floor():
    tok = "f" * 33 + "12"
    now_ts = 2_000_000_000
    floor_ts = now_ts - 45 * 86400
    plan, skip, _ = odds_sync.build_single_token_plan(
        token_id=tok,
        market_id="m",
        closed=False,
        created_ts=100,
        latest_timestamps={tok: now_ts - 3600},
        fully_checked_tokens=set(),
        persisted_skips={tok: "old"},
        seen_tokens=set(),
        now_ts=now_ts,
        fidelity=1,
        force=False,
        rebuild_minutely=False,
        overlap_seconds=60,
        recent_seconds=999999,
        minutely_backfill_floor_ts=floor_ts,
    )
    assert skip is None
    assert plan is not None
    assert plan.start_ts == floor_ts
    assert plan.end_ts == now_ts
    assert plan.fidelity == 1


def test_iter_token_plans_paged_minutely_backfill_uses_full_iterator(monkeypatch):
    captured = {}

    def markets_pages(**kwargs):
        captured.update(kwargs)
        return iter(())

    monkeypatch.setattr(odds_sync, "iter_due_market_tokens", lambda **kwargs: iter(()))
    monkeypatch.setattr(odds_sync, "iter_markets_with_tokens", markets_pages)
    gen = odds_sync.iter_token_plans_paged(
        now_ts=2_000_000_000,
        clob_cutoff_date="2023-01-01",
        fidelity=1,
        force=False,
        rebuild_minutely=False,
        overlap_minutes=0,
        skip_recent_minutes=0,
        market_page_size=10,
        minutely_backfill_days=45,
        min_volume=100_000.0,
    )
    try:
        while True:
            next(gen)
    except StopIteration:
        pass
    assert captured.get("min_volume") == 100_000.0
    assert captured.get("json_array_only") is True
