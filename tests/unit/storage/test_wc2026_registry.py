"""Storage tests for polymarket_ops.wc2026_market_registry."""

from __future__ import annotations

import importlib

import pytest

from oddsfox.config._reload_settings import reload_all_settings_modules
from oddsfox.storage.duckdb.wc2026_registry import (
    RegistryRow,
    clear_registry,
    get_registry_market_ids,
    registry_market_count,
    upsert_registry_rows,
)


@pytest.fixture
def duck(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "registry.duckdb"))
    import oddsfox.storage.duckdb.connection as connection

    reload_all_settings_modules()
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
    importlib.reload(connection)
    connection.ensure_duck_db()
    clear_registry()
    yield connection
    clear_registry()


def test_upsert_empty_rows_is_noop(duck):
    assert upsert_registry_rows([]) == 0


def test_upsert_and_query_registry(duck):
    n = upsert_registry_rows(
        [
            RegistryRow("m1", "2026-fifa-world-cup-winner-595", "e1", "events_api"),
            RegistryRow("m2", "2026-fifa-world-cup-winner-595", "e1", "events_api"),
        ]
    )
    assert n == 2
    assert registry_market_count() == 2
    assert get_registry_market_ids() == ["m1", "m2"]
