from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from oddsfox.orchestration import assets_polymarket as assets_mod
from oddsfox.orchestration import config as orch_config
from oddsfox.orchestration.assets import (
    polymarket_markets_raw_dlt,
    polymarket_odds_repair,
    polymarket_token_odds_history,
    polymarket_token_odds_history_minutely,
    polymarket_wc2026_registry,
)


def test_get_polymarket_dlt_pipeline_uses_path_cache(monkeypatch):
    created = []

    class FakeDlt:
        class destinations:
            @staticmethod
            def duckdb(*, credentials):
                return {"credentials": credentials}

        @staticmethod
        def pipeline(**kwargs):
            created.append(kwargs)
            return object()

    monkeypatch.setattr(assets_mod, "active_duckdb_path", lambda: "/tmp/cache.duckdb")
    monkeypatch.setattr(assets_mod, "dlt", FakeDlt)
    assets_mod._DLT_PIPELINE_BY_PATH.clear()

    first = assets_mod.get_polymarket_dlt_pipeline()
    second = assets_mod.get_polymarket_dlt_pipeline()

    assert first is second
    assert len(created) == 1


def test_dlt_asset_clears_pending_packages_and_indexes(monkeypatch):
    pipeline = MagicMock(has_pending_data=True)
    conn = MagicMock()
    source = object()
    fake_dlt = MagicMock()
    fake_dlt.run.return_value = iter(["event"])

    @contextmanager
    def connection():
        yield conn

    monkeypatch.setattr(assets_mod, "get_polymarket_dlt_pipeline", lambda: pipeline)
    monkeypatch.setattr(assets_mod, "collect_raw_markets", lambda: [{"id": "raw"}])
    monkeypatch.setattr(
        assets_mod,
        "normalize_market_payloads_for_dlt",
        lambda rows: [{"id": rows[0]["id"]}],
    )
    monkeypatch.setattr(
        assets_mod, "polymarket_markets_source", lambda *, rows=(): source
    )
    monkeypatch.setattr(assets_mod, "get_connection", connection)
    ensure_indexes = MagicMock()
    monkeypatch.setattr(assets_mod, "ensure_polymarket_indexes", ensure_indexes)

    fn = polymarket_markets_raw_dlt.op.compute_fn.decorated_fn
    assert list(fn(MagicMock(), fake_dlt)) == ["event"]

    pipeline.drop_pending_packages.assert_called_once()
    fake_dlt.run.assert_called_once()
    ensure_indexes.assert_called_once_with(conn)


def test_wc2026_registry_skips_when_snapshot_already_refreshed(monkeypatch):
    monkeypatch.setattr(
        assets_mod,
        "get_sync_run_metrics",
        lambda task: {"registry_refreshed": True, "task": task},
    )
    monkeypatch.setattr(assets_mod, "snapshot_raw_layer", lambda **_kwargs: {"x": 1})

    fn = polymarket_wc2026_registry.op.compute_fn.decorated_fn
    result = fn(MagicMock(), orch_config.Wc2026RegistryConfig())

    run_summary = result.metadata["run_summary"].value
    assert run_summary["skipped"] is True
    assert run_summary["reason"] == "snapshot_refreshed_registry"


def test_wc2026_registry_runs_sync_when_snapshot_did_not_refresh(monkeypatch):
    captured = {}

    def sync_wc2026_registry(**kwargs):
        kwargs["progress_callback"]("registry_probe", {"ok": True})
        captured.update(kwargs)
        return {"registry_rows_upserted": 1}

    monkeypatch.setattr(assets_mod, "get_sync_run_metrics", lambda _task: None)
    monkeypatch.setattr(assets_mod.ops, "sync_wc2026_registry", sync_wc2026_registry)
    monkeypatch.setattr(assets_mod, "snapshot_raw_layer", lambda **_kwargs: {})
    monkeypatch.setattr(assets_mod, "delta_raw_layer", lambda _pre, _post: {})

    fn = polymarket_wc2026_registry.op.compute_fn.decorated_fn
    result = fn(
        MagicMock(),
        orch_config.Wc2026RegistryConfig(
            max_event_pages=3,
            max_pages_without_progress=2,
            keyset_closed=None,
            keyset_tag_slugs=["world-cup"],
            keyset_volume_min=None,
        ),
    )

    assert captured["max_event_pages"] == 3
    assert captured["max_pages_without_progress"] == 2
    assert captured["keyset_closed"] is None
    assert captured["keyset_tag_slugs"] == ["world-cup"]
    assert captured["keyset_volume_min"] is None
    assert result.metadata["run_summary"].value == {"registry_rows_upserted": 1}


def test_wc2026_registry_force_refresh_bypasses_snapshot_metric_check(monkeypatch):
    checked_metrics = []
    monkeypatch.setattr(
        assets_mod,
        "get_sync_run_metrics",
        lambda task: checked_metrics.append(task) or {"registry_refreshed": True},
    )
    monkeypatch.setattr(
        assets_mod.ops,
        "sync_wc2026_registry",
        lambda **_kwargs: {"registry_rows_upserted": 1},
    )
    monkeypatch.setattr(assets_mod, "snapshot_raw_layer", lambda **_kwargs: {})
    monkeypatch.setattr(assets_mod, "delta_raw_layer", lambda _pre, _post: {})

    fn = polymarket_wc2026_registry.op.compute_fn.decorated_fn
    result = fn(MagicMock(), orch_config.Wc2026RegistryConfig(force_refresh=True))

    assert checked_metrics == []
    assert result.metadata["run_summary"].value == {"registry_rows_upserted": 1}


def test_materialize_odds_sync_metadata_and_plan_iterator(monkeypatch):
    captured = {}

    def sync_odds(**kwargs):
        captured.update(kwargs)
        return {
            "planning": {"plans": 1},
            "planning_context": {"markets": 1},
            "totals": {"rows": 2},
        }

    monkeypatch.setattr(assets_mod.ops, "sync_odds", sync_odds)
    monkeypatch.setattr(assets_mod, "snapshot_raw_layer", lambda **_kwargs: {"rows": 0})
    monkeypatch.setattr(assets_mod, "delta_raw_layer", lambda _pre, _post: {})

    plan_iterator = object()
    result = assets_mod._materialize_odds_sync(
        MagicMock(),
        orch_config.OddsSyncConfig(min_volume=10.0, max_volume=20.0),
        plan_iterator_factory=plan_iterator,
    )

    assert captured["plan_iterator_factory"] is plan_iterator
    assert result.metadata["min_volume"].value == 10.0
    assert result.metadata["max_volume"].value == 20.0


def test_odds_sync_helper_builds_kwargs_and_metadata():
    progress = MagicMock()
    plan_iterator = object()
    config = orch_config.OddsSyncConfig(
        workers=3,
        min_volume=10.0,
        max_volume=20.0,
        market_scope="wc2026",
    )

    kwargs = assets_mod._build_odds_sync_kwargs(
        config,
        progress,
        plan_iterator_factory=plan_iterator,
    )
    metadata = assets_mod._odds_sync_metadata(
        config,
        {
            "planning": {"plans": 1},
            "planning_context": {"markets": 2},
            "totals": {"rows": 3},
        },
        {},
    )

    assert kwargs["max_workers"] == 3
    assert kwargs["progress_callback"] is progress
    assert kwargs["plan_iterator_factory"] is plan_iterator
    assert kwargs["market_scope"] == "wc2026"
    assert metadata["workers"].value == 3
    assert metadata["min_volume"].value == 10.0
    assert metadata["max_volume"].value == 20.0
    assert metadata["totals"].value == {"rows": 3}


def test_run_with_guardrail_thread_success_and_error(monkeypatch):
    class ImmediateThread:
        def __init__(self, target, daemon):
            self.target = target
            self.daemon = daemon
            self.started = False

        def start(self):
            self.started = True
            self.target()

        def is_alive(self):
            return False

        def join(self, timeout=None):
            del timeout

    guardrail = MagicMock()
    monkeypatch.setattr(assets_mod.ops, "Thread", ImmediateThread)

    assert assets_mod._run_with_guardrail_thread(
        guardrail,
        "phase",
        lambda: {"ok": True},
        poll_seconds=0,
    ) == {"ok": True}
    guardrail.record_progress.assert_called_with(
        work_increment=0,
        phase="phase_complete",
        diagnostics={"worker_alive": False},
        force_log=True,
    )

    def raise_boom():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        assets_mod._run_with_guardrail_thread(
            guardrail,
            "phase",
            raise_boom,
            poll_seconds=0,
        )


def test_run_with_guardrail_thread_checks_while_worker_alive(monkeypatch):
    class PollingThread:
        def __init__(self, target, daemon):
            self.target = target
            self.daemon = daemon
            self.alive_checks = 0

        def start(self):
            self.target()

        def is_alive(self):
            self.alive_checks += 1
            return self.alive_checks <= 2

        def join(self, timeout=None):
            del timeout

    guardrail = MagicMock()
    monkeypatch.setattr(assets_mod.ops, "Thread", PollingThread)

    assert (
        assets_mod._run_with_guardrail_thread(
            guardrail,
            "slow_phase",
            lambda: {},
            poll_seconds=0,
        )
        == {}
    )
    guardrail.check.assert_called_once_with(
        phase="slow_phase",
        diagnostics={"worker_alive": True},
    )


def test_odds_assets_delegate_to_materializer(monkeypatch):
    calls = []

    def materialize(_context, config, **kwargs):
        calls.append((type(config), kwargs))
        return "ok"

    monkeypatch.setattr(assets_mod, "_materialize_odds_sync", materialize)

    ctx = MagicMock()
    assert (
        polymarket_token_odds_history.op.compute_fn.decorated_fn(
            ctx, orch_config.OddsSyncConfig()
        )
        == "ok"
    )
    assert (
        polymarket_token_odds_history_minutely.op.compute_fn.decorated_fn(
            ctx, orch_config.MinutelyOddsSyncConfig()
        )
        == "ok"
    )
    assert calls[0][0] is orch_config.OddsSyncConfig
    assert calls[1][0] is orch_config.MinutelyOddsSyncConfig


def test_repair_asset_returns_reconcile_metadata(monkeypatch):
    monkeypatch.setattr(assets_mod, "snapshot_raw_layer", lambda **_kwargs: {"x": 1})
    monkeypatch.setattr(
        assets_mod, "delta_raw_layer", lambda _pre, _post: {"x": {"before": 1}}
    )
    monkeypatch.setattr(
        assets_mod.ops,
        "reconcile_odds_ledger",
        lambda **kwargs: {"persist": kwargs["persist_run_metrics"]},
    )

    fn = polymarket_odds_repair.op.compute_fn.decorated_fn
    result = fn(MagicMock(), orch_config.RepairConfig(persist_run_metrics=False))

    assert result.metadata["reconcile"].value == {"persist": False}
