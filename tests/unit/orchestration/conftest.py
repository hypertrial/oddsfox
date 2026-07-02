"""Autouse guards for orchestration unit tests (avoid real sleeps and dbt stream blocking)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from oddsfox.orchestration import dbt_build as dbt_build_mod
from oddsfox.orchestration import pipeline_ops as pipeline_ops_mod
from oddsfox.orchestration import polymarket_ops as polymarket_ops_mod
from oddsfox.resources.progress_guardrails import ProgressGuardrail
from tests.unit.orchestration.orchestration_test_support import (
    _FakeClock,
    _ImmediateThread,
)


def _patch_progress_guardrail_module(monkeypatch, module, clock: _FakeClock) -> None:
    class _ClockedProgressGuardrail(ProgressGuardrail):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("clock", clock)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(module, "ProgressGuardrail", _ClockedProgressGuardrail)


def _stub_registry(**kwargs):
    cb = kwargs.get("progress_callback")
    if cb:
        cb("registry_probe", {"ok": True})
    return {"registry_rows_upserted": 0}


def _stub_sync_markets(**kwargs):
    cb = kwargs.get("progress_callback")
    if cb:
        cb("probe_markets", {"page": 1})
    return {"task": "sync_markets", "total_fetched": 0}


def _stub_sync_odds(**kwargs):
    cb = kwargs.get("progress_callback")
    if cb:
        cb("probe_odds", {"token": 1})
    return {"task": "sync_odds", "noop": True}


@pytest.fixture(autouse=True)
def orchestration_test_guards(request, monkeypatch, tmp_path, reset_connection_globals):
    """Keep orchestration unit tests off real wall-clock sleeps and blocking dbt polls."""
    del reset_connection_globals
    if request.node.get_closest_marker("facade") is not None:
        yield
        return

    db_path = tmp_path / "orchestration.duckdb"
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.delenv("DUCKDB_PATH", raising=False)

    from oddsfox.config._reload_settings import reload_all_settings_modules

    reload_all_settings_modules()

    import oddsfox.storage.duckdb.connection as connection

    connection.reset_duckdb_connection_state()
    connection.ensure_duck_db()

    clock = _FakeClock()
    for module in (
        pipeline_ops_mod,
        polymarket_ops_mod,
        dbt_build_mod,
    ):
        if hasattr(module, "ProgressGuardrail"):
            _patch_progress_guardrail_module(monkeypatch, module, clock)

    import oddsfox.orchestration.assets as assets_mod

    _patch_progress_guardrail_module(monkeypatch, assets_mod, clock)

    monkeypatch.setattr(polymarket_ops_mod, "sync_wc2026_registry", _stub_registry)
    monkeypatch.setattr(polymarket_ops_mod, "sync_markets", _stub_sync_markets)
    monkeypatch.setattr(polymarket_ops_mod, "sync_odds", _stub_sync_odds)
    monkeypatch.setattr(
        polymarket_ops_mod,
        "backfill_market_metadata",
        lambda **kwargs: {"task": "backfill_market_metadata", "skipped": True},
    )
    monkeypatch.setattr(
        pipeline_ops_mod,
        "reconcile_odds_ledger",
        lambda **_kwargs: {},
    )
    monkeypatch.setattr(dbt_build_mod, "Thread", _ImmediateThread)

    with patch("time.sleep", lambda *_a, **_k: None):
        yield

    connection.reset_duckdb_connection_state()


@pytest.fixture
def reset_connection_globals():
    import oddsfox.storage.duckdb.connection as connection

    connection.reset_duckdb_connection_state()
    yield
    connection.reset_duckdb_connection_state()


def pytest_collection_modifyitems(items):
    for item in items:
        item.add_marker(pytest.mark.orchestration)
