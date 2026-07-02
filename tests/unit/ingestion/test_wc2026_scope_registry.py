"""Unit tests for WC2026 scope registry and discovery."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

from tests.unit.ingestion.wc2026_scope_test_support import slug_only_cfg

from oddsfox.ingestion.polymarket import wc2026_scope as scope_mod
from oddsfox.ingestion.polymarket.wc2026_scope import (
    Wc2026EventsScanResult,
    Wc2026ScopeConfig,
    collect_wc2026_markets_from_events,
    load_wc2026_config,
    refresh_registry_and_collect_markets_from_events,
    refresh_registry_from_events,
)
from oddsfox.ingestion.polymarket.wc2026_scope import (
    registry as scope_registry_mod,
)


def test_refresh_registry_from_events(monkeypatch, tmp_path):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "registry.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    client = MagicMock()
    client.get.return_value = {
        "events": [
            {
                "id": "ev1",
                "slug": "2026-fifa-world-cup-winner-595",
                "markets": [{"id": "m100"}, {"id": "m101"}],
            },
            {
                "id": "ev2",
                "slug": "premier-league-2026",
                "markets": [{"id": "m999"}],
            },
        ],
        "next_cursor": None,
    }
    summary = refresh_registry_from_events(client, config=slug_only_cfg(), max_pages=5)
    assert summary["registry_rows_upserted"] == 2
    assert "2026-fifa-world-cup-winner-595" in summary["discovered_event_slugs"]
    assert summary["by_source"] == {"events_api": 2}

    from oddsfox.storage.duckdb.wc2026_registry import get_registry_market_ids

    assert sorted(get_registry_market_ids()) == ["m100", "m101"]


def test_refresh_registry_and_collect_single_events_pass(monkeypatch, tmp_path):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "combined.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    client = MagicMock()
    client.get.return_value = {
        "events": [
            {
                "id": "ev1",
                "slug": "2026-fifa-world-cup-winner-595",
                "markets": [{"id": "m100"}, {"id": "m101"}],
            },
        ],
        "next_cursor": None,
    }
    registry_summary, markets, collect_meta = (
        refresh_registry_and_collect_markets_from_events(
            client,
            config=slug_only_cfg(),
            max_pages=5,
            tag_discovery=False,
        )
    )
    assert client.get.call_count == 1
    assert registry_summary["registry_rows_upserted"] == 2
    assert len(markets) == 2
    assert collect_meta["markets_collected"] == 2
    assert markets[0].get("events")


def test_refresh_registry_with_seed_market_ids(monkeypatch, tmp_path):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "seed_registry.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = Wc2026ScopeConfig(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=(),
        market_ids=("seed-only",),
        registry_max_event_pages=5,
    )
    client = MagicMock()
    client.get.return_value = {"events": [], "next_cursor": None}
    summary = refresh_registry_from_events(client, config=cfg, max_pages=1)
    assert summary["registry_rows_upserted"] >= 1
    assert summary["by_source"].get("seed", 0) >= 1


def test_targeted_registry_ignores_progress_callback_failures(monkeypatch):
    cfg = Wc2026ScopeConfig(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=(),
        market_ids=("123",),
        registry_max_event_pages=5,
    )
    monkeypatch.setattr(
        scope_registry_mod, "fetch_gamma_event_by_slug", lambda *_a: None
    )
    monkeypatch.setattr(scope_registry_mod, "get_registry_market_ids", lambda: ["456"])
    monkeypatch.setattr(
        scope_registry_mod,
        "_fetch_markets_batch_resilient",
        lambda *_a, **_k: [],
    )
    monkeypatch.setattr(
        scope_registry_mod,
        "_finalize_registry_collect",
        lambda *_a, **_k: ({"registry_rows_upserted": 0}, [], {}),
    )

    summary, markets, meta = (
        scope_registry_mod.refresh_registry_and_collect_markets_targeted(
            MagicMock(),
            config=cfg,
            progress_callback=lambda phase, payload: (_ for _ in ()).throw(
                RuntimeError("callback failed")
            ),
        )
    )

    assert summary == {"registry_rows_upserted": 0}
    assert markets == []
    assert meta == {}


def test_targeted_registry_emits_progress_callbacks(monkeypatch):
    cfg = Wc2026ScopeConfig(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=(),
        market_ids=("123",),
        registry_max_event_pages=5,
    )
    monkeypatch.setattr(
        scope_registry_mod, "fetch_gamma_event_by_slug", lambda *_a: None
    )
    monkeypatch.setattr(scope_registry_mod, "get_registry_market_ids", lambda: ["456"])
    monkeypatch.setattr(
        scope_registry_mod,
        "_fetch_markets_batch_resilient",
        lambda *_a, **_k: [],
    )
    monkeypatch.setattr(
        scope_registry_mod,
        "_finalize_registry_collect",
        lambda *_a, **_k: ({"registry_rows_upserted": 0}, [], {}),
    )
    progress = []

    scope_registry_mod.refresh_registry_and_collect_markets_targeted(
        MagicMock(),
        config=cfg,
        progress_callback=lambda phase, payload: progress.append((phase, payload)),
    )

    assert [phase for phase, _payload in progress] == [
        "wc2026_event_by_slug",
        "wc2026_markets_by_id",
    ]


def test_targeted_registry_without_progress_callback(monkeypatch):
    cfg = Wc2026ScopeConfig(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=(),
        market_ids=("123",),
        registry_max_event_pages=5,
    )
    monkeypatch.setattr(
        scope_registry_mod, "fetch_gamma_event_by_slug", lambda *_a: None
    )
    monkeypatch.setattr(scope_registry_mod, "get_registry_market_ids", lambda: ["456"])
    monkeypatch.setattr(
        scope_registry_mod,
        "_fetch_markets_batch_resilient",
        lambda *_a, **_k: [],
    )
    monkeypatch.setattr(
        scope_registry_mod,
        "_finalize_registry_collect",
        lambda *_a, **_k: ({"registry_rows_upserted": 0}, [], {}),
    )

    summary, markets, meta = (
        scope_registry_mod.refresh_registry_and_collect_markets_targeted(
            MagicMock(),
            config=cfg,
        )
    )

    assert summary == {"registry_rows_upserted": 0}
    assert markets == []
    assert meta == {}


def test_markets_sync_targeted_discovery(monkeypatch, tmp_path):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules
    from oddsfox.ingestion.polymarket.markets.sync import sync_markets

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "targeted.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    event_payload = {
        "id": "ev1",
        "slug": "2026-fifa-world-cup-winner",
        "markets": [
            {
                "id": "253591",
                "question": "Q",
                "outcomes": "[]",
                "clobTokenIds": '["t1"]',
            },
        ],
    }

    def _fake_get(path, **kwargs):
        if path.endswith("/events/slug/2026-fifa-world-cup-winner"):
            return event_payload
        if path == "/markets":
            return []
        raise AssertionError(f"unexpected path: {path}")

    client = MagicMock()
    client.get.side_effect = _fake_get
    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.markets.sync.build_client",
        lambda: client,
    )
    progress = []

    result = sync_markets(
        discovery_mode="targeted",
        progress_callback=lambda phase, payload: progress.append(phase),
    )
    assert result["mode"] == "wc2026_event_first"
    assert result["discovery_mode"] == "targeted"
    assert result["total_fetched"] >= 1
    assert "wc2026_event_by_slug" in progress
    assert "discovery_complete" in progress

    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.markets.sync.prepare_batch_for_db",
        lambda df: ([], []),
    )
    progress.clear()
    result_empty_batch = sync_markets(
        discovery_mode="targeted",
        progress_callback=lambda phase, payload: progress.append(phase),
    )
    assert result_empty_batch["total_fetched"] == 0
    assert "discovery_complete" in progress

    result_no_cb = sync_markets(discovery_mode="targeted")
    assert result_no_cb["mode"] == "wc2026_event_first"

    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.markets.sync.refresh_registry_and_collect_markets_targeted",
        lambda *a, **k: ({"registry_rows_upserted": 0}, [], {"markets_collected": 0}),
    )
    empty_events = sync_markets(discovery_mode="targeted")
    assert empty_events["total_fetched"] == 0


def test_markets_sync_full_keyset_mode(monkeypatch, tmp_path):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules
    from oddsfox.ingestion.polymarket.markets.sync import sync_markets

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "full_keyset.duckdb"))
    monkeypatch.setenv("POLYMARKET_WC2026_TAG_DISCOVERY", "false")
    monkeypatch.delenv("POLYMARKET_WC2026_EVENT_TAGS", raising=False)
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    client = MagicMock()
    client.get.return_value = {
        "events": [
            {
                "id": "ev1",
                "slug": "2026-fifa-world-cup-winner-595",
                "markets": [
                    {
                        "id": "m1",
                        "question": "Q",
                        "outcomes": "[]",
                        "clobTokenIds": '["t1"]',
                    },
                ],
            },
        ],
        "next_cursor": None,
    }
    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.markets.sync.build_client",
        lambda: client,
    )

    cfg = load_wc2026_config()
    result = sync_markets(discovery_mode="full_keyset")
    assert result["discovery_mode"] == "full_keyset"
    keyset_calls = [
        c
        for c in client.get.call_args_list
        if c.args and str(c.args[0]).endswith("/events/keyset")
    ]
    assert len(keyset_calls) == len(cfg.event_tags)
    assert {c.kwargs["params"]["tag_slug"] for c in keyset_calls} == set(cfg.event_tags)
    assert result["total_fetched"] >= 1


def test_refresh_registry_from_events_keyset_closed_filter(monkeypatch, tmp_path):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules
    from oddsfox.ingestion.polymarket.wc2026_scope import (
        refresh_registry_from_events,
    )

    page1 = {
        "events": [
            {
                "id": "ev1",
                "slug": "2026-fifa-world-cup-winner-595",
                "markets": [{"id": "m1"}],
            },
        ],
        "next_cursor": None,
    }
    client = MagicMock()
    client.get.return_value = page1

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "keyset_closed.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = slug_only_cfg()
    refresh_registry_from_events(client, config=cfg, max_pages=10, keyset_closed=False)
    params = client.get.call_args.kwargs.get("params") or {}
    assert params.get("closed") is False


def test_refresh_registry_from_events_keyset_tag_and_volume_filters(
    monkeypatch, tmp_path
):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules
    from oddsfox.ingestion.polymarket.wc2026_scope import (
        refresh_registry_from_events,
    )

    page1 = {
        "events": [
            {
                "id": "ev1",
                "slug": "2026-fifa-world-cup-winner-595",
                "markets": [{"id": "m1"}],
            },
        ],
        "next_cursor": None,
    }
    client = MagicMock()
    client.get.return_value = page1

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "keyset_filters.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = slug_only_cfg()
    summary = refresh_registry_from_events(
        client,
        config=cfg,
        max_pages=10,
        keyset_closed=False,
        keyset_tag_slugs=["fifa-world-cup", "2026-fifa-world-cup"],
        keyset_volume_min=100000,
    )
    assert client.get.call_count == 2
    tag_slugs = [
        (c.kwargs.get("params") or {}).get("tag_slug")
        for c in client.get.call_args_list
    ]
    assert tag_slugs == ["fifa-world-cup", "2026-fifa-world-cup"]
    for call in client.get.call_args_list:
        params = call.kwargs.get("params") or {}
        assert params.get("closed") is False
        assert params.get("volume_min") == 100000
    assert summary["keyset_tag_slugs"] == ["fifa-world-cup", "2026-fifa-world-cup"]
    assert summary["keyset_volume_min"] == 100000


def test_refresh_registry_targeted_slug_and_markets(monkeypatch, tmp_path):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules
    from oddsfox.ingestion.polymarket.wc2026_scope import (
        Wc2026ScopeConfig,
        refresh_registry_and_collect_markets_targeted,
    )

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "targeted_registry.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = Wc2026ScopeConfig(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=(),
        market_ids=("1001",),
        registry_max_event_pages=None,
    )
    progress = []

    def _fake_get(path, **kwargs):
        if path.endswith("/events/slug/2026-fifa-world-cup-winner-595"):
            return {
                "id": "ev1",
                "slug": "2026-fifa-world-cup-winner-595",
                "markets": [{"id": "2001"}],
            }
        if path == "/markets":
            return [
                {
                    "id": "1001",
                    "events": [{"slug": "2026-fifa-world-cup-winner-595", "id": "ev1"}],
                }
            ]
        raise AssertionError(path)

    client = MagicMock()
    client.get.side_effect = _fake_get
    summary, markets, meta = refresh_registry_and_collect_markets_targeted(
        client,
        config=cfg,
        progress_callback=lambda phase, payload: progress.append(phase),
    )
    assert summary["discovery_mode"] == "targeted"
    assert summary["registry_refreshed"] is True
    assert meta["api_requests"] >= 2
    assert len(markets) >= 2
    assert "wc2026_event_by_slug" in progress
    assert "wc2026_markets_by_id" in progress


def test_full_keyset_stops_after_pages_without_progress(monkeypatch, tmp_path):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules
    from oddsfox.ingestion.polymarket.wc2026_scope import (
        collect_wc2026_markets_from_events,
    )

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "no_progress.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)

    cfg = slug_only_cfg(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=(),
        market_ids=(),
    )
    # Advancing cursor each page so the stall guard does not trip; this exercises
    # the distinct ``max_pages_without_progress`` (no in-scope match) stop path.
    client = MagicMock()
    client.get.side_effect = [
        {
            "events": [{"id": "x", "slug": "other-event", "markets": [{"id": "m-x"}]}],
            "next_cursor": f"more-{i}",
        }
        for i in range(30)
    ]

    markets, meta = collect_wc2026_markets_from_events(
        client,
        config=cfg,
        max_pages=100,
    )
    assert markets == []
    assert meta["truncated"] is True
    assert meta["events_pages"] == 25


def test_wc2026_discovery_ledger_and_scope_hash(monkeypatch, tmp_path):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules
    from oddsfox.ingestion.polymarket.wc2026_scope import (
        Wc2026ScopeConfig,
        scope_config_hash,
    )
    from oddsfox.storage.duckdb.metadata import (
        get_wc2026_discovery_fully_checked,
        get_wc2026_discovery_scope_config_hash,
        set_wc2026_discovery_fully_checked,
    )

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "ledger.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = Wc2026ScopeConfig(
        event_slugs=("slug-a",),
        event_slug_prefixes=("prefix",),
        market_ids=("m1",),
        registry_max_event_pages=None,
    )
    digest = scope_config_hash(cfg)
    assert digest
    set_wc2026_discovery_fully_checked(True, scope_config_hash=digest)
    assert get_wc2026_discovery_fully_checked() is True
    assert get_wc2026_discovery_scope_config_hash() == digest


def test_targeted_skips_missing_slug_and_markets_callback_errors(monkeypatch, tmp_path):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules
    from oddsfox.ingestion.polymarket.errors import GammaRequestError
    from oddsfox.ingestion.polymarket.wc2026_scope import (
        Wc2026ScopeConfig,
        refresh_registry_and_collect_markets_targeted,
    )

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "targeted_skip.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = Wc2026ScopeConfig(
        event_slugs=("missing-slug", "slug-a"),
        event_slug_prefixes=(),
        market_ids=("9001",),
        registry_max_event_pages=None,
    )

    def _fake_get(path, **kwargs):
        if path.endswith("/events/slug/missing-slug"):
            response = MagicMock()
            response.status_code = 404
            raise GammaRequestError("missing", response=response)
        if path.endswith("/events/slug/slug-a"):
            return {"id": "ev", "slug": "slug-a", "markets": []}
        if path == "/markets":
            return [{"id": "9001", "events": [{"slug": "slug-a", "id": "ev"}]}]
        raise AssertionError(path)

    client = MagicMock()
    client.get.side_effect = _fake_get
    calls = {"count": 0}

    def _progress(phase, payload):
        calls["count"] += 1
        if phase == "wc2026_markets_by_id":
            raise RuntimeError("markets progress failed")

    summary, _, _ = refresh_registry_and_collect_markets_targeted(
        client,
        config=cfg,
        progress_callback=_progress,
    )
    assert summary["discovery_mode"] == "targeted"
    assert calls["count"] >= 2


def test_targeted_progress_callbacks_ignore_failures(monkeypatch, tmp_path):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules
    from oddsfox.ingestion.polymarket.wc2026_scope import (
        Wc2026ScopeConfig,
        refresh_registry_and_collect_markets_targeted,
    )

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "cb_fail.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = Wc2026ScopeConfig(
        event_slugs=("slug-a",),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=None,
    )

    def _fake_get(path, **kwargs):
        if "/events/slug/" in path:
            return {"id": "ev", "slug": "slug-a", "markets": []}
        return []

    client = MagicMock()
    client.get.side_effect = _fake_get

    def _boom(*args, **kwargs):
        raise RuntimeError("progress failed")

    summary, _, _ = refresh_registry_and_collect_markets_targeted(
        client,
        config=cfg,
        progress_callback=_boom,
    )
    assert summary["discovery_mode"] == "targeted"


def test_full_keyset_marks_discovery_complete(monkeypatch, tmp_path):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules
    from oddsfox.storage.duckdb.metadata import (
        get_wc2026_discovery_fully_checked,
    )

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "complete.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = slug_only_cfg(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=(),
        market_ids=(),
    )
    client = MagicMock()
    client.get.return_value = {
        "events": [
            {
                "id": "ev1",
                "slug": "2026-fifa-world-cup-winner-595",
                "markets": [{"id": "m1"}],
            },
        ],
        "next_cursor": None,
    }
    refresh_registry_from_events(client, config=cfg, max_pages=5)
    assert get_wc2026_discovery_fully_checked() is True


def test_discovery_ledger_invalidates_on_scope_change(monkeypatch, tmp_path):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules
    from oddsfox.storage.duckdb.metadata import (
        get_wc2026_discovery_fully_checked,
        get_wc2026_discovery_scope_config_hash,
        set_wc2026_discovery_fully_checked,
    )

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "hash_change.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg_a = Wc2026ScopeConfig(
        event_slugs=("slug-a",),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=None,
    )
    set_wc2026_discovery_fully_checked(
        True, scope_config_hash=scope_mod.scope_config_hash(cfg_a)
    )
    cfg_b = Wc2026ScopeConfig(
        event_slugs=("slug-b",),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=None,
    )
    client = MagicMock()
    client.get.return_value = {
        "events": [{"id": "x", "slug": "other", "markets": []}],
        "next_cursor": "more",
    }
    refresh_registry_from_events(client, config=cfg_b, max_pages=1)
    assert get_wc2026_discovery_fully_checked() is False
    assert get_wc2026_discovery_scope_config_hash() == scope_mod.scope_config_hash(
        cfg_b
    )


def test_truncated_full_keyset_clears_fully_checked(monkeypatch, tmp_path):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules
    from oddsfox.storage.duckdb.metadata import (
        get_wc2026_discovery_fully_checked,
        set_wc2026_discovery_fully_checked,
    )

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "truncated.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = slug_only_cfg(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=(),
        market_ids=(),
    )
    set_wc2026_discovery_fully_checked(
        True, scope_config_hash=scope_mod.scope_config_hash(cfg)
    )
    client = MagicMock()
    client.get.side_effect = [
        {
            "events": [{"id": f"x{i}", "slug": "other", "markets": []}],
            "next_cursor": f"cursor-{i + 1}",
        }
        for i in range(30)
    ]
    refresh_registry_from_events(
        client,
        config=cfg,
        max_pages=100,
        max_pages_without_progress=2,
    )
    assert get_wc2026_discovery_fully_checked() is False


def test_get_registry_event_slugs(monkeypatch, tmp_path):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules
    from oddsfox.storage.duckdb.wc2026_registry import (
        RegistryRow,
        get_registry_event_slugs,
        upsert_registry_rows,
    )

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "slugs.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    upsert_registry_rows(
        [
            RegistryRow("m1", "slug-b", "ev", "seed"),
            RegistryRow("m2", "slug-a", "ev", "seed"),
            RegistryRow("m3", None, None, "seed"),
        ]
    )
    assert get_registry_event_slugs() == ["slug-a", "slug-b"]


def test_collect_markets_omits_closed_when_unset(monkeypatch):
    cfg = slug_only_cfg()
    scan_result = Wc2026EventsScanResult(
        registry_rows=(),
        raw_markets=(),
        pages_done=0,
        truncated=False,
        discovered_slugs=(),
    )
    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.wc2026_scope.registry._scan_wc2026_gamma_events",
        lambda *a, **k: scan_result,
    )
    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.wc2026_scope.registry._resolve_keyset_closed",
        lambda _v: None,
    )
    _markets, meta = collect_wc2026_markets_from_events(MagicMock(), config=cfg)
    assert "keyset_closed" not in meta


def test_collect_markets_meta_uses_keyset_slugs_when_no_crawl_tags(monkeypatch):
    cfg = slug_only_cfg()
    scan_result = Wc2026EventsScanResult(
        registry_rows=(),
        raw_markets=({"id": "m1"},),
        pages_done=1,
        truncated=False,
        discovered_slugs=(),
        crawl_tag_slugs=(),
        scope_tag_slugs=("fifa-world-cup",),
    )
    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.wc2026_scope.registry._scan_wc2026_gamma_events",
        lambda *a, **k: scan_result,
    )
    markets, meta = collect_wc2026_markets_from_events(
        MagicMock(),
        config=cfg,
        keyset_tag_slugs=["explicit-tag"],
        keyset_volume_min=5000.0,
    )
    assert markets[0]["id"] == "m1"
    assert meta["keyset_tag_slugs"] == ["explicit-tag"]
    assert meta["keyset_volume_min"] == 5000.0

    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.wc2026_scope.registry._resolve_keyset_volume_min",
        lambda _v: None,
    )
    _markets2, meta2 = collect_wc2026_markets_from_events(
        MagicMock(),
        config=cfg,
        keyset_tag_slugs=["explicit-tag"],
    )
    assert "keyset_volume_min" not in meta2


def test_registry_seed_rows_and_dedupe_priority() -> None:
    from oddsfox.storage.duckdb.wc2026_registry import RegistryRow

    cfg = Wc2026ScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=("seed-m",),
        registry_max_event_pages=1,
    )

    assert any(
        r.market_id == "seed-m" for r in scope_registry_mod._seed_registry_rows(cfg)
    )
    merged = scope_registry_mod._dedupe_registry_rows(
        [
            RegistryRow("x", None, None, "seed"),
            RegistryRow("x", "es", "e1", "events_api"),
        ]
    )
    assert merged[0].source == "events_api"
    merged_same = scope_registry_mod._dedupe_registry_rows(
        [
            RegistryRow("y", None, None, "events_api"),
            RegistryRow("y", "a", None, "events_api"),
        ]
    )
    assert len(merged_same) == 1
    assert scope_registry_mod._source_priority("unknown") == 0


def test_finalize_registry_collect_meta_branches(monkeypatch) -> None:

    monkeypatch.setattr(
        scope_registry_mod,
        "upsert_registry_rows",
        lambda rows: len(rows),
    )
    cfg = slug_only_cfg()
    scan = Wc2026EventsScanResult(
        registry_rows=(),
        raw_markets=(),
        pages_done=0,
        truncated=False,
        discovered_slugs=(),
        crawl_tag_slugs=("crawl-a",),
        scope_tag_slugs=("fifa-world-cup",),
        tag_sources=(("crawl-a", ("seed",)),),
    )
    reg, _markets, meta = scope_registry_mod._finalize_registry_collect(
        scan,
        cfg,
        discovery_mode=scope_registry_mod.DISCOVERY_MODE_TARGETED,
        t0=time.monotonic(),
        keyset_closed=True,
        keyset_tag_slugs=["fallback-tag"],
        keyset_volume_min=100.0,
    )
    assert reg["keyset_closed"] is True
    assert reg["crawl_tag_slugs"] == ["crawl-a"]
    assert meta["keyset_volume_min"] == 100.0

    reg2, _markets2, meta2 = scope_registry_mod._finalize_registry_collect(
        scan,
        cfg,
        discovery_mode=scope_registry_mod.DISCOVERY_MODE_TARGETED,
        t0=time.monotonic(),
        keyset_closed=None,
        keyset_volume_min=None,
    )
    assert "keyset_closed" not in reg2
    assert "keyset_closed" not in meta2
