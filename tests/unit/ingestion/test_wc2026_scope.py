"""Unit tests for WC 2026 Polymarket scope helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from oddsfox.ingestion.polymarket import wc2026_scope as scope_mod
from oddsfox.ingestion.polymarket.wc2026_scope import (
    MARKET_SCOPE_ALL,
    MARKET_SCOPE_WC2026,
    Wc2026EventsScanResult,
    Wc2026ScopeConfig,
    collect_wc2026_markets_from_events,
    event_in_scope,
    event_matches_wc2026_config,
    event_matches_wc2026_tags,
    is_wc2026_market_row,
    load_wc2026_config,
    market_scope_predicate_sql,
    market_scope_sql,
    refresh_registry_and_collect_markets_from_events,
    refresh_registry_from_events,
    validate_market_scope,
)
from oddsfox.ingestion.polymarket.wc2026_scope import (
    config as scope_config_mod,
)
from oddsfox.ingestion.polymarket.wc2026_scope import (
    gamma as scope_gamma_mod,
)
from oddsfox.ingestion.polymarket.wc2026_scope import (
    predicates as scope_predicates_mod,
)
from oddsfox.ingestion.polymarket.wc2026_scope import (
    scan as scope_scan_mod,
)
from oddsfox.storage.duckdb.wc2026_registry import RegistryRow


def _slug_only_cfg(**kwargs) -> Wc2026ScopeConfig:
    defaults = {
        "event_slugs": ("2026-fifa-world-cup-winner-595",),
        "event_slug_prefixes": ("2026-fifa-world-cup",),
        "market_ids": (),
        "registry_max_event_pages": None,
        "event_tags": (),
    }
    defaults.update(kwargs)
    return Wc2026ScopeConfig(**defaults)


def test_validate_market_scope_rejects_legacy():
    with pytest.raises(ValueError, match="wc2026_legacy"):
        validate_market_scope("wc2026_legacy")


@pytest.fixture(autouse=True)
def _wc2026_test_discovery_settings(monkeypatch):
    """Keep unit tests deterministic (no live Gamma tag discovery)."""
    monkeypatch.setattr(scope_predicates_mod, "POLYMARKET_WC2026_TAG_DISCOVERY", False)
    monkeypatch.setattr(scope_scan_mod, "POLYMARKET_WC2026_TAG_CLOSURE_ROUNDS", 0)
    monkeypatch.setattr(scope_scan_mod, "POLYMARKET_WC2026_TAG_CRAWL_MAX", 100)
    monkeypatch.setattr(
        scope_predicates_mod, "POLYMARKET_WC2026_KEYSET_RELATED_TAGS", False
    )
    monkeypatch.setattr(scope_predicates_mod, "POLYMARKET_WC2026_KEYSET_CLOSED", False)
    monkeypatch.setattr(
        scope_predicates_mod, "POLYMARKET_WC2026_KEYSET_VOLUME_MIN", 10000.0
    )


def test_load_wc2026_config_includes_default_slug(monkeypatch):
    monkeypatch.delenv("POLYMARKET_WC2026_EVENT_TAGS", raising=False)
    cfg = load_wc2026_config()
    assert "2026-fifa-world-cup-winner" in cfg.event_slugs
    assert cfg.event_slug_prefixes
    assert "2026-fifa-world-cup" in cfg.event_tags
    assert "fifa-world-cup" in cfg.event_tags
    assert "world-cup" in cfg.event_tags


def test_event_in_scope_rejects_related_pass_without_wc_tag():
    cfg = Wc2026ScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=None,
        event_tags=("fifa-world-cup",),
    )
    event = {
        "slug": "unrelated-esports-finals",
        "tags": [{"slug": "esports"}],
    }
    assert not event_in_scope(
        event,
        config=cfg,
        keyset_tag_slug="fifa-world-cup",
        keyset_related_tags=True,
        scope_tag_slugs=cfg.event_tags,
    )


def test_event_in_scope_related_pass_keeps_wc_tagged_event():
    cfg = Wc2026ScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=None,
        event_tags=("fifa-world-cup",),
    )
    event = {
        "slug": "world-cup-group-a-winner",
        "tags": [{"slug": "fifa-world-cup"}],
    }
    assert event_in_scope(
        event,
        config=cfg,
        keyset_tag_slug="fifa-world-cup",
        keyset_related_tags=True,
        scope_tag_slugs=cfg.event_tags,
    )


def test_event_in_scope_matches_tag_without_prefix_slug():
    cfg = Wc2026ScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=None,
        event_tags=("2026-fifa-world-cup", "fifa-world-cup"),
    )
    event = {
        "slug": "world-cup-group-a-winner",
        "tags": [{"slug": "fifa-world-cup"}, {"slug": "soccer"}],
    }
    assert event_in_scope(event, config=cfg)
    assert event_in_scope(event, config=cfg, keyset_tag_slug="fifa-world-cup")
    assert event_matches_wc2026_tags(event, config=cfg)
    assert not event_matches_wc2026_config("world-cup-group-a-winner", config=cfg)


def test_event_in_scope_rejects_crawl_only_discovered_tag():
    """Crawl-only tags must not widen strict scope admission."""
    cfg = Wc2026ScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=None,
        event_tags=("fifa-world-cup", "2026-fifa-world-cup", "world-cup"),
    )
    event = {
        "slug": "copa-america-final",
        "tags": [{"slug": "argentina"}],
    }
    assert not event_in_scope(
        event,
        config=cfg,
        keyset_tag_slug="argentina",
        scope_tag_slugs=cfg.event_tags,
    )


def test_scan_decouples_crawl_tags_from_scope_allowlist(monkeypatch):
    cfg = Wc2026ScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=5,
        event_tags=("fifa-world-cup",),
    )
    monkeypatch.setattr(
        scope_mod,
        "resolve_keyset_crawl_tags",
        lambda *args, **kwargs: (
            ["fifa-world-cup", "argentina"],
            {
                "fifa-world-cup": {"seed"},
                "argentina": {"event_closure"},
            },
        ),
    )

    def _get(endpoint, **kwargs):
        params = kwargs.get("params") or {}
        tag = params.get("tag_slug")
        if tag == "fifa-world-cup":
            return {"events": [], "next_cursor": None}
        if tag == "argentina":
            return {
                "events": [
                    {
                        "id": "ev-copa",
                        "slug": "copa-america-final",
                        "tags": [{"slug": "argentina"}],
                        "markets": [{"id": "m-copa"}],
                    }
                ],
                "next_cursor": None,
            }
        return {"events": [], "next_cursor": None}

    client = MagicMock()
    client.get.side_effect = _get
    scan = scope_mod._scan_wc2026_gamma_events(
        client,
        cfg,
        max_pages=10,
        tag_discovery=False,
    )
    assert scan.scope_tag_slugs == ("fifa-world-cup",)
    assert "argentina" not in scan.crawl_tag_slugs
    assert "fifa-world-cup" in scan.crawl_tag_slugs
    assert {m["id"] for m in scan.raw_markets} == set()


def test_scan_tag_closure_expands_crawl_tags(monkeypatch):
    cfg = Wc2026ScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=5,
        event_tags=("fifa-world-cup",),
    )
    monkeypatch.setattr(scope_scan_mod, "POLYMARKET_WC2026_TAG_CLOSURE_ROUNDS", 1)
    monkeypatch.setattr(
        scope_mod,
        "resolve_keyset_crawl_tags",
        lambda *args, **kwargs: (["fifa-world-cup"], {"fifa-world-cup": {"seed"}}),
    )
    calls: list[str | None] = []

    def _get(endpoint, **kwargs):
        params = kwargs.get("params") or {}
        tag = params.get("tag_slug")
        calls.append(tag)
        if tag == "fifa-world-cup":
            return {
                "events": [
                    {
                        "id": "ev1",
                        "slug": "world-cup-group-a-winner",
                        "tags": [
                            {"slug": "fifa-world-cup"},
                            {"slug": "argentina"},
                            {"slug": "world-cup-qualifiers"},
                        ],
                        "markets": [{"id": "m1"}],
                    }
                ],
                "next_cursor": None,
            }
        if tag == "argentina":
            return {"events": [], "next_cursor": None}
        if tag == "world-cup-qualifiers":
            return {"events": [], "next_cursor": None}
        return {"events": [], "next_cursor": None}

    client = MagicMock()
    client.get.side_effect = _get
    scan = scope_mod._scan_wc2026_gamma_events(
        client,
        cfg,
        max_pages=10,
        tag_discovery=False,
    )
    assert "fifa-world-cup" in scan.crawl_tag_slugs
    assert "world-cup-qualifiers" in scan.crawl_tag_slugs
    assert "argentina" not in scan.crawl_tag_slugs
    assert "world-cup-qualifiers" in dict(scan.tag_sources)
    assert dict(scan.tag_sources)["world-cup-qualifiers"] == ("event_closure",)
    assert "m1" in {m["id"] for m in scan.raw_markets}
    assert calls.count("fifa-world-cup") == 1
    assert calls.count("argentina") == 0
    assert calls.count("world-cup-qualifiers") == 1


def test_crawl_tag_allowed_skips_broad_and_keeps_wc_tags():
    scope = ("fifa-world-cup",)
    seed = ("fifa-world-cup",)
    denylist = ("sports", "portugal")
    assert not scope_mod._crawl_tag_allowed(
        "sports", scope_tags=scope, seed_tags=seed, denylist=denylist
    )
    assert not scope_mod._crawl_tag_allowed(
        "portugal", scope_tags=scope, seed_tags=seed, denylist=denylist
    )
    assert scope_mod._crawl_tag_allowed(
        "world-cup-qualifiers",
        scope_tags=scope,
        seed_tags=seed,
        denylist=denylist,
        keyword_gate=True,
    )
    assert scope_mod._crawl_tag_allowed(
        "fifa-world-cup", scope_tags=scope, seed_tags=seed, denylist=denylist
    )


def test_crawl_tag_allowed_scope_seed_always_crawl_even_when_denylisted():
    scope = ("sports",)
    seed = ("sports",)
    denylist = ("sports",)
    assert scope_mod._crawl_tag_allowed(
        "sports", scope_tags=scope, seed_tags=seed, denylist=denylist
    )


def test_crawl_tag_allowed_denylist_blocks_keyword_match():
    scope = ("fifa-world-cup",)
    seed = ("fifa-world-cup",)
    denylist = ("world-cup-qualifiers",)
    assert not scope_mod._crawl_tag_allowed(
        "world-cup-qualifiers",
        scope_tags=scope,
        seed_tags=seed,
        denylist=denylist,
        keyword_gate=True,
    )


def test_scan_collection_parity_with_closure_gate_on_vs_off(monkeypatch):
    cfg = Wc2026ScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=5,
        event_tags=("fifa-world-cup",),
    )
    monkeypatch.setattr(scope_scan_mod, "POLYMARKET_WC2026_TAG_CLOSURE_ROUNDS", 1)
    monkeypatch.setattr(
        scope_mod,
        "resolve_keyset_crawl_tags",
        lambda *args, **kwargs: (["fifa-world-cup"], {"fifa-world-cup": {"seed"}}),
    )

    def _get(endpoint, **kwargs):
        params = kwargs.get("params") or {}
        tag = params.get("tag_slug")
        if tag == "fifa-world-cup":
            return {
                "events": [
                    {
                        "id": "ev1",
                        "slug": "world-cup-group-a-winner",
                        "tags": [
                            {"slug": "fifa-world-cup"},
                            {"slug": "sports"},
                            {"slug": "portugal"},
                        ],
                        "markets": [{"id": "m1"}, {"id": "m2"}],
                    }
                ],
                "next_cursor": None,
            }
        return {"events": [], "next_cursor": None}

    client = MagicMock()
    client.get.side_effect = _get

    monkeypatch.setattr(
        scope_predicates_mod, "POLYMARKET_WC2026_TAG_CLOSURE_KEYWORD_GATE", True
    )
    gated = scope_mod._scan_wc2026_gamma_events(
        client, cfg, max_pages=10, tag_discovery=False
    )

    monkeypatch.setattr(
        scope_predicates_mod, "POLYMARKET_WC2026_TAG_CLOSURE_KEYWORD_GATE", False
    )
    ungated = scope_mod._scan_wc2026_gamma_events(
        client, cfg, max_pages=10, tag_discovery=False
    )

    assert {m["id"] for m in gated.raw_markets} == {"m1", "m2"}
    assert {m["id"] for m in ungated.raw_markets} == {"m1", "m2"}
    assert "sports" not in gated.crawl_tag_slugs
    assert "portugal" not in gated.crawl_tag_slugs


def test_is_wc2026_market_row_matches_event_tags():
    cfg = Wc2026ScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=None,
        event_tags=("fifa-world-cup",),
    )
    assert is_wc2026_market_row(
        market_id="x",
        event_slug="world-cup-group-a-winner",
        event_tags=("fifa-world-cup",),
        config=cfg,
    )


def test_is_wc2026_strict_by_allowlisted_event_slug():
    cfg = Wc2026ScopeConfig(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=("2026-fifa-world-cup",),
        market_ids=(),
        registry_max_event_pages=None,
    )
    assert is_wc2026_market_row(
        market_id="1",
        event_slug="2026-fifa-world-cup-winner-595",
        config=cfg,
    )


def test_is_wc2026_strict_excludes_unrelated_market():
    cfg = load_wc2026_config()
    assert not is_wc2026_market_row(
        market_id="x",
        question="Premier League 2026",
        description="No world cup here",
        config=cfg,
    )


def test_event_matches_wc2026_config_prefix():
    cfg = Wc2026ScopeConfig(
        event_slugs=(),
        event_slug_prefixes=("2026-fifa-world-cup",),
        market_ids=(),
        registry_max_event_pages=None,
    )
    assert event_matches_wc2026_config("2026-fifa-world-cup-winner-595", config=cfg)


def test_refresh_registry_from_events(monkeypatch, tmp_path):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "registry.duckdb"))
    reload_all_settings_modules()
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
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
    summary = refresh_registry_from_events(client, config=_slug_only_cfg(), max_pages=5)
    assert summary["registry_rows_upserted"] == 2
    assert "2026-fifa-world-cup-winner-595" in summary["discovered_event_slugs"]
    assert summary["by_source"] == {"events_api": 2}

    from oddsfox.storage.duckdb.wc2026_registry import get_registry_market_ids

    assert sorted(get_registry_market_ids()) == ["m100", "m101"]


def test_iter_wc2026_gamma_events_skips_non_allowlisted(monkeypatch, tmp_path):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "event_first.duckdb"))
    reload_all_settings_modules()
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
    importlib.reload(connection)

    client = MagicMock()
    client.get.side_effect = [
        {
            "events": [
                {
                    "id": "ev1",
                    "slug": "2026-fifa-world-cup-winner-595",
                    "markets": [{"id": "m1"}],
                },
            ],
            "next_cursor": "page2",
        },
        {
            "events": [
                {
                    "id": "ev2",
                    "slug": "other-league-2026",
                    "markets": [{"id": "m2"}],
                },
            ],
            "next_cursor": None,
        },
    ]
    markets, meta = collect_wc2026_markets_from_events(
        client,
        config=_slug_only_cfg(),
        max_pages=10,
        tag_discovery=False,
    )
    assert len(markets) == 1
    assert markets[0]["id"] == "m1"
    assert meta["events_pages"] == 2


def test_refresh_registry_and_collect_single_events_pass(monkeypatch, tmp_path):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "combined.duckdb"))
    reload_all_settings_modules()
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
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
            config=_slug_only_cfg(),
            max_pages=5,
            tag_discovery=False,
        )
    )
    assert client.get.call_count == 1
    assert registry_summary["registry_rows_upserted"] == 2
    assert len(markets) == 2
    assert collect_meta["markets_collected"] == 2
    assert markets[0].get("events")


def test_wc2026_scope_config_and_sql_helpers():
    assert scope_config_mod._parse_csv_list("") == ()
    assert scope_config_mod._parse_csv_list(" a , b ") == ("a", "b")
    with pytest.raises(ValueError, match="Invalid event slug"):
        scope_config_mod._validate_slug_token("bad slug!")
    cfg_empty = Wc2026ScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=None,
    )
    assert cfg_empty.default_event_slug
    cfg_slug = Wc2026ScopeConfig(
        event_slugs=("first-slug",),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=None,
    )
    assert cfg_slug.default_event_slug == "first-slug"
    cfg_ids = Wc2026ScopeConfig(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=(),
        market_ids=("mid-1",),
        registry_max_event_pages=None,
    )
    assert "mid-1" in market_scope_sql(MARKET_SCOPE_WC2026, config=cfg_ids)
    from oddsfox.ingestion.polymarket.scope_sql import _sql_quote_list

    assert _sql_quote_list(()) == "NULL"
    sql = market_scope_sql(MARKET_SCOPE_WC2026, config=cfg_ids)
    assert "wc2026_market_registry" in sql
    assert market_scope_sql(MARKET_SCOPE_ALL) == ""
    assert market_scope_predicate_sql(MARKET_SCOPE_ALL) == "TRUE"
    assert not event_matches_wc2026_config(None)
    assert is_wc2026_market_row(market_id="seed1", in_registry=True, config=cfg_ids)
    assert is_wc2026_market_row(
        market_id="x",
        market_scope=MARKET_SCOPE_ALL,
        config=cfg_ids,
    )


def test_load_wc2026_config_missing_seed_and_prefix_only_sql(tmp_path, monkeypatch):
    missing = tmp_path / "missing.yml"
    monkeypatch.delenv("POLYMARKET_WC2026_EVENT_SLUGS", raising=False)
    monkeypatch.delenv("POLYMARKET_WC2026_EVENT_SLUG_PREFIXES", raising=False)
    cfg = load_wc2026_config(seed_path=missing)
    assert cfg.event_slugs
    prefix_only = Wc2026ScopeConfig(
        event_slugs=(),
        event_slug_prefixes=("2026-fifa-world-cup",),
        market_ids=(),
        registry_max_event_pages=None,
    )
    sql = market_scope_sql(MARKET_SCOPE_WC2026, config=prefix_only)
    assert "LIKE '2026-fifa-world-cup%'" in sql


def test_load_wc2026_config_yaml_validation(tmp_path, monkeypatch):
    monkeypatch.delenv("POLYMARKET_WC2026_EVENT_SLUGS", raising=False)
    monkeypatch.delenv("POLYMARKET_WC2026_EVENT_SLUG_PREFIXES", raising=False)
    monkeypatch.delenv("POLYMARKET_WC2026_EVENT_TAGS", raising=False)
    good = tmp_path / "good.yml"
    good.write_text("event_slug_prefixes:\n  - pre\n", encoding="utf-8")
    cfg = load_wc2026_config(seed_path=good)
    assert cfg.event_slug_prefixes == ("pre",)
    bad = tmp_path / "bad.yml"
    bad.write_text("not-a-dict", encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid YAML root"):
        load_wc2026_config(seed_path=bad)
    bad2 = tmp_path / "bad2.yml"
    bad2.write_text("event_slugs: 1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="event_slugs must be"):
        load_wc2026_config(seed_path=bad2)


def test_scan_wc2026_gamma_events_edge_cases():
    cfg = Wc2026ScopeConfig(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=(),
        market_ids=("seed-m",),
        registry_max_event_pages=1,
    )
    client = MagicMock()
    progress = []

    client.get.return_value = {
        "events": [
            {
                "id": "ev1",
                "slug": "2026-fifa-world-cup-winner-595",
                "markets": [
                    "not-dict",
                    {"id": ""},
                    {"id": "dup"},
                    {"id": "dup"},
                    {"id": "m2"},
                    {
                        "id": "m3",
                        "events": [{"slug": "2026-fifa-world-cup-winner-595"}],
                    },
                ],
            },
        ],
        "next_cursor": "more-pages",
    }
    scan = scope_mod._scan_wc2026_gamma_events(
        client,
        cfg,
        max_pages=1,
        progress_callback=lambda phase, payload: progress.append((phase, payload)),
    )
    assert progress
    assert "m2" in {m["id"] for m in scan.raw_markets}
    assert scan.raw_markets[0].get("events")

    client2 = MagicMock()
    client2.get.return_value = [
        {
            "slug": "2026-fifa-world-cup-winner-595",
            "markets": [{"id": "list-path"}],
        }
    ]
    scan2 = scope_mod._scan_wc2026_gamma_events(client2, cfg, max_pages=5)
    assert scan2.raw_markets and scan2.raw_markets[0]["id"] == "list-path"
    assert any(r.market_id == "seed-m" for r in scope_scan_mod._seed_registry_rows(cfg))
    from oddsfox.storage.duckdb.wc2026_registry import RegistryRow

    merged = scope_scan_mod._dedupe_registry_rows(
        [
            RegistryRow("x", None, None, "seed"),
            RegistryRow("x", "es", "e1", "events_api"),
        ]
    )
    assert merged[0].source == "events_api"
    merged_same = scope_scan_mod._dedupe_registry_rows(
        [
            RegistryRow("y", None, None, "events_api"),
            RegistryRow("y", "a", None, "events_api"),
        ]
    )
    assert len(merged_same) == 1
    assert scope_scan_mod._source_priority("unknown") == 0


def test_refresh_registry_with_seed_market_ids(monkeypatch, tmp_path):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "seed_registry.duckdb"))
    reload_all_settings_modules()
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
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


def test_markets_sync_targeted_discovery(monkeypatch, tmp_path):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules
    from oddsfox.ingestion.polymarket.markets.sync import sync_markets

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "targeted.duckdb"))
    reload_all_settings_modules()
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
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
    import oddsfox.ingestion.polymarket.wc2026_scope as reloaded_scope

    monkeypatch.setattr(
        reloaded_scope.predicates, "POLYMARKET_WC2026_TAG_DISCOVERY", False
    )
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
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


def test_gamma_events_keyset_shared_pagination_params(monkeypatch, tmp_path):
    """WC2026 scan and event-slug fallback use the same /events/keyset pagination."""
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules
    from oddsfox.ingestion.polymarket.markets.backfill._events_fallback import (
        _fill_from_events_endpoint,
    )
    from oddsfox.ingestion.polymarket.wc2026_scope import (
        refresh_registry_from_events,
    )

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "shared_pagination.duckdb"))
    reload_all_settings_modules()
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
    importlib.reload(connection)
    connection.ensure_duck_db()

    page1 = {
        "events": [
            {
                "id": "ev1",
                "slug": "2026-fifa-world-cup-winner-595",
                "markets": [{"id": "m1"}],
            },
            {"id": "ev2", "slug": "other", "markets": [{"id": "m2"}]},
        ],
        "next_cursor": "c2",
    }
    page2 = {"events": [], "next_cursor": None}
    client = MagicMock()
    client.get.side_effect = [page1, page2, page1, page2]

    cfg = _slug_only_cfg()
    refresh_registry_from_events(
        client,
        config=cfg,
        max_pages=10,
        tag_discovery=False,
        keyset_related_tags=False,
    )
    wc_calls = [
        c.kwargs.get("params") or {}
        for c in client.get.call_args_list
        if c.args and str(c.args[0]).endswith("/events/keyset")
    ][:2]

    client.get.reset_mock()
    client.get.side_effect = [page1, page2]
    saved, _meta = _fill_from_events_endpoint(client, {"m99"}, max_pages=10)
    assert saved == 0
    fb_calls = [c.kwargs.get("params") or {} for c in client.get.call_args_list]

    assert len(wc_calls) >= 1 and len(fb_calls) >= 1
    assert wc_calls[0].get("limit") == fb_calls[0].get("limit") == 500
    assert wc_calls[0].get("closed") is False
    assert wc_calls[0].get("volume_min") == 10000
    assert wc_calls[1].get("next_cursor") == "c2"
    assert fb_calls[1].get("next_cursor") == "c2"
    assert "closed" not in fb_calls[0]


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
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = _slug_only_cfg()
    refresh_registry_from_events(client, config=cfg, max_pages=10, keyset_closed=False)
    params = client.get.call_args.kwargs.get("params") or {}
    assert params.get("closed") is False


def test_keyset_tag_pass_keeps_non_prefix_event_slug():
    cfg = Wc2026ScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=None,
        event_tags=("fifa-world-cup", "2026-fifa-world-cup"),
    )
    client = MagicMock()
    client.get.return_value = {
        "events": [
            {
                "id": "ev-group-a",
                "slug": "world-cup-group-a-winner",
                "tags": [{"slug": "fifa-world-cup"}],
                "markets": [{"id": "m-group-a"}],
            },
        ],
        "next_cursor": None,
    }
    markets, meta = collect_wc2026_markets_from_events(
        client,
        config=cfg,
        max_pages=5,
        keyset_tag_slugs=["fifa-world-cup"],
    )
    assert len(markets) == 1
    assert markets[0]["id"] == "m-group-a"
    params = client.get.call_args.kwargs.get("params") or {}
    assert params.get("tag_slug") == "fifa-world-cup"
    assert meta["keyset_tag_slugs"] == ["fifa-world-cup"]


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
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = _slug_only_cfg()
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


def test_fetch_gamma_event_by_slug_handles_missing(monkeypatch):
    from oddsfox.ingestion.polymarket.errors import GammaRequestError
    from oddsfox.ingestion.polymarket.gamma_events import (
        fetch_gamma_event_by_slug,
    )

    client = MagicMock()
    response = MagicMock()
    response.status_code = 404
    client.get.side_effect = GammaRequestError("missing", response=response)
    assert fetch_gamma_event_by_slug(client, "missing-slug") is None
    assert fetch_gamma_event_by_slug(client, "  ") is None

    client.get.reset_mock()
    client.get.side_effect = None
    client.get.return_value = {"slug": "x"}
    assert fetch_gamma_event_by_slug(client, "empty-id") is None

    client.get.return_value = {"id": "1", "slug": "ok-slug"}
    assert fetch_gamma_event_by_slug(client, "ok-slug")["id"] == "1"


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
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
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
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
    importlib.reload(connection)

    cfg = _slug_only_cfg(
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
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
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


def test_gamma_market_id_filter_and_resilient_fetch():
    from oddsfox.ingestion.polymarket.errors import GammaRequestError

    assert scope_gamma_mod._is_gamma_market_id("253591") is True
    assert scope_gamma_mod._is_gamma_market_id("m1") is False
    assert scope_gamma_mod._gamma_market_ids(["253591", "m1", "m2"]) == ["253591"]

    client = MagicMock()
    client.get.side_effect = [
        GammaRequestError("batch", response=MagicMock(status_code=422)),
        GammaRequestError("bad", response=MagicMock(status_code=422)),
        [{"id": "1"}],
    ]
    rows = scope_gamma_mod._fetch_markets_batch_resilient(client, ["bad", "1"])
    assert len(rows) == 1
    assert rows[0]["id"] == "1"
    assert scope_gamma_mod._fetch_markets_batch_resilient(client, []) == []

    client.get.side_effect = GammaRequestError(
        "server", response=MagicMock(status_code=500)
    )
    with pytest.raises(GammaRequestError):
        scope_gamma_mod._fetch_markets_batch_resilient(client, ["1"])


def test_wc2026_internal_collect_helpers():
    cfg = Wc2026ScopeConfig(
        event_slugs=("slug-a",),
        event_slug_prefixes=("prefix",),
        market_ids=("seed-m",),
        registry_max_event_pages=None,
    )
    assert scope_scan_mod._event_slug_from_market({}) == (None, None)
    assert scope_scan_mod._event_slug_from_market({"events": "bad"}) == (None, None)
    assert scope_scan_mod._event_slug_from_market({"events": [["x"]]}) == (None, None)

    empty_slug = scope_scan_mod._collect_from_events(
        [{"slug": " ", "markets": [{"id": "m1"}]}], cfg
    )
    assert empty_slug.registry_rows == ()

    non_match = scope_scan_mod._collect_from_events(
        [{"slug": "other-event", "markets": [{"id": "m1"}]}], cfg
    )
    assert non_match.registry_rows == ()

    dup = scope_scan_mod._collect_from_events(
        [
            {
                "slug": "slug-a",
                "id": "ev",
                "markets": [{"id": "m1"}, {"id": "m1"}],
            }
        ],
        cfg,
    )
    assert len(dup.raw_markets) == 1
    assert len(dup.registry_rows) == 2

    markets_collect = scope_scan_mod._collect_from_market_payloads(
        [
            "bad",
            {"id": ""},
            {
                "id": "seed-m",
                "events": [{"slug": "slug-a", "id": "ev"}],
            },
            {"id": "seed-m"},
            {
                "id": "prefix-only",
                "events": [{"slug": "prefix-new-event", "id": "ev2"}],
            },
            {"id": "skip-me", "events": [{"slug": "unrelated", "id": "x"}]},
            {"id": "no-slug-seed", "events": []},
        ],
        cfg,
        allowlisted_market_ids={"seed-m", "no-slug-seed"},
    )
    assert {m["id"] for m in markets_collect.raw_markets} == {
        "seed-m",
        "prefix-only",
        "no-slug-seed",
    }

    left = scope_scan_mod._empty_scan_result()
    right = Wc2026EventsScanResult(
        registry_rows=(RegistryRow("m1", "slug-a", "ev", "events_api"),),
        raw_markets=({"id": "m1"}, {"id": ""}),
        pages_done=1,
        truncated=True,
        discovered_slugs=("slug-a",),
        api_requests=1,
    )
    merged = scope_scan_mod._merge_scan_results(left, right)
    assert merged.truncated is True
    assert merged.api_requests == 1


def test_iter_wc2026_gamma_events_stops_on_empty_page():
    cfg = Wc2026ScopeConfig(
        event_slugs=("2026-fifa-world-cup-winner-595",),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=5,
    )
    client = MagicMock()
    client.get.return_value = {"events": [], "next_cursor": None}
    yielded = list(scope_scan_mod._iter_wc2026_gamma_events(client, cfg, max_pages=5))
    assert yielded == []


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
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
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


def test_iter_wc2026_gamma_events_yields_allowlisted_only():
    cfg = _slug_only_cfg(event_slugs=("2026-fifa-world-cup-winner-595",))
    client = MagicMock()
    client.get.return_value = {
        "events": [
            {"id": "1", "slug": "2026-fifa-world-cup-winner-595", "markets": []},
            {"id": "2", "slug": "other", "markets": []},
        ],
        "next_cursor": None,
    }
    yielded = list(scope_scan_mod._iter_wc2026_gamma_events(client, cfg, max_pages=5))
    events = [
        item for item in yielded if item[0] is not scope_scan_mod._EVENTS_PAGE_MARKER
    ]
    assert len(events) == 1
    assert events[0][1] == "2026-fifa-world-cup-winner-595"


def test_fetch_gamma_event_by_slug_reraises_non_404():
    from oddsfox.ingestion.polymarket.errors import GammaRequestError
    from oddsfox.ingestion.polymarket.gamma_events import (
        fetch_gamma_event_by_slug,
    )

    client = MagicMock()
    response = MagicMock()
    response.status_code = 500
    client.get.side_effect = GammaRequestError("server error", response=response)
    with pytest.raises(GammaRequestError):
        fetch_gamma_event_by_slug(client, "some-slug")


def test_fetch_gamma_event_by_slug_reraises_transport_errors(monkeypatch):
    import requests

    from oddsfox.ingestion.polymarket.gamma_events import (
        fetch_gamma_event_by_slug,
    )

    client = MagicMock()
    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.gamma_events.gamma_get",
        lambda *a, **k: (_ for _ in ()).throw(requests.ConnectionError("down")),
    )
    with pytest.raises(requests.ConnectionError):
        fetch_gamma_event_by_slug(client, "some-slug")


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
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
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
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = _slug_only_cfg(
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
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
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
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
    importlib.reload(connection)
    connection.ensure_duck_db()

    cfg = _slug_only_cfg(
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
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
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


def test_resolve_keyset_crawl_tags_discovery_failure(monkeypatch):
    cfg = _slug_only_cfg(event_tags=("fifa-world-cup",))
    client = MagicMock()

    def _boom(*_a: object, **_k: object) -> object:
        raise RuntimeError("discovery down")

    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.wc2026_tags.discover_wc2026_tag_slugs",
        _boom,
    )
    slugs, sources = scope_predicates_mod.resolve_keyset_crawl_tags(
        None,
        config=cfg,
        client=client,
        tag_discovery=True,
    )
    assert slugs == ["fifa-world-cup"]
    assert sources["fifa-world-cup"] == {"seed"}


def test_resolve_keyset_crawl_tags_discovery_no_log_when_unchanged(monkeypatch):
    from types import SimpleNamespace

    cfg = _slug_only_cfg(event_tags=("fifa-world-cup",))
    client = MagicMock()
    discovered = SimpleNamespace(
        tag_slugs=["fifa-world-cup"],
        sources={"fifa-world-cup": {"discovered"}},
    )
    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.wc2026_tags.discover_wc2026_tag_slugs",
        lambda *a, **k: discovered,
    )
    slugs, _sources = scope_predicates_mod.resolve_keyset_crawl_tags(
        None,
        config=cfg,
        client=client,
        tag_discovery=True,
    )
    assert slugs == ["fifa-world-cup"]


def test_resolve_keyset_crawl_tags_discovery_expands(monkeypatch):
    from types import SimpleNamespace

    cfg = _slug_only_cfg(event_tags=("fifa-world-cup",))
    client = MagicMock()
    discovered = SimpleNamespace(
        tag_slugs=["fifa-world-cup", "extra-tag"],
        sources={"extra-tag": {"discovered"}},
    )
    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.wc2026_tags.discover_wc2026_tag_slugs",
        lambda *a, **k: discovered,
    )
    slugs, sources = scope_predicates_mod.resolve_keyset_crawl_tags(
        None,
        config=cfg,
        client=client,
        tag_discovery=True,
    )
    assert slugs == ["extra-tag", "fifa-world-cup"]
    assert sources["extra-tag"] == {"discovered"}
    assert sources["fifa-world-cup"] == {"seed"}


def test_event_tag_slugs_skips_blank_slug() -> None:
    assert scope_predicates_mod._event_tag_slugs(
        {"tags": ["not-a-dict", {"slug": ""}, {"slug": "  "}, {"slug": "WC"}]}
    ) == frozenset({"wc"})


def test_parse_tag_discovery_keywords_default() -> None:
    assert scope_predicates_mod._parse_tag_discovery_keywords(None)
    assert scope_predicates_mod._parse_tag_discovery_keywords("  ")


def test_scan_tag_crawl_max_truncates(monkeypatch):
    cfg = Wc2026ScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=5,
        event_tags=("fifa-world-cup", "argentina"),
    )
    monkeypatch.setattr(scope_scan_mod, "POLYMARKET_WC2026_TAG_CRAWL_MAX", 1)
    monkeypatch.setattr(
        scope_mod,
        "resolve_keyset_crawl_tags",
        lambda *a, **k: (
            ["fifa-world-cup", "argentina"],
            {"fifa-world-cup": {"seed"}, "argentina": {"seed"}},
        ),
    )

    def _get(_endpoint, **kwargs):
        return {"events": [], "next_cursor": None}

    client = MagicMock()
    client.get.side_effect = _get
    scan = scope_mod._scan_wc2026_gamma_events(
        client, cfg, max_pages=10, tag_discovery=False
    )
    assert scan.truncated is True
    assert len(scan.crawl_tag_slugs) == 1


def test_collect_markets_omits_closed_when_unset(monkeypatch):
    cfg = _slug_only_cfg()
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
    cfg = _slug_only_cfg()
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


def test_finalize_registry_collect_meta_branches(monkeypatch) -> None:
    import time

    monkeypatch.setattr(
        "oddsfox.storage.duckdb.wc2026_registry.upsert_registry_rows",
        lambda rows: len(rows),
    )
    cfg = _slug_only_cfg()
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
    reg, _markets, meta = scope_scan_mod._finalize_registry_collect(
        scan,
        cfg,
        discovery_mode=scope_scan_mod.DISCOVERY_MODE_TARGETED,
        t0=time.monotonic(),
        keyset_closed=True,
        keyset_tag_slugs=["fallback-tag"],
        keyset_volume_min=100.0,
    )
    assert reg["keyset_closed"] is True
    assert reg["crawl_tag_slugs"] == ["crawl-a"]
    assert meta["keyset_volume_min"] == 100.0

    reg2, _markets2, meta2 = scope_scan_mod._finalize_registry_collect(
        scan,
        cfg,
        discovery_mode=scope_scan_mod.DISCOVERY_MODE_TARGETED,
        t0=time.monotonic(),
        keyset_closed=None,
        keyset_volume_min=None,
    )
    assert "keyset_closed" not in reg2
    assert "keyset_closed" not in meta2


def test_predicate_helpers_cover_remaining_branches() -> None:
    assert scope_predicates_mod.event_matches_wc2026_tags(None) is False
    assert scope_predicates_mod.event_in_scope(None) is False
    assert (
        scope_predicates_mod._crawl_tag_allowed(None, scope_tags=(), seed_tags=())
        is True
    )
    assert (
        scope_predicates_mod._crawl_tag_allowed(
            "  ", scope_tags=("fifa-world-cup",), seed_tags=()
        )
        is False
    )
    cfg = _slug_only_cfg(event_tags=("fifa-world-cup",))
    assert scope_predicates_mod.is_wc2026_market_row(
        market_id="x",
        event_slug="2026-fifa-world-cup-extra",
        market_scope=MARKET_SCOPE_ALL,
    )
    assert scope_predicates_mod.is_wc2026_market_row(
        market_id="x",
        event_slug="2026-fifa-world-cup-extra",
        config=cfg,
    )
    assert scope_predicates_mod.is_wc2026_market_row(
        market_id="x",
        event_tags=["fifa-world-cup"],
        config=cfg,
    )
    assert not scope_predicates_mod.is_wc2026_market_row(
        market_id="zzz",
        event_tags=["unrelated"],
        config=cfg,
    )
    denied = scope_predicates_mod._filter_crawl_tag_slugs(
        ["blocked-tag"],
        scope_tags=("fifa-world-cup",),
        seed_tags=(),
    )
    assert denied == []


def test_resolve_tag_crawl_max_disabled(monkeypatch) -> None:
    monkeypatch.setattr(scope_scan_mod, "POLYMARKET_WC2026_TAG_CRAWL_MAX", 0)
    assert scope_scan_mod._resolve_tag_crawl_max() is None


def test_scan_max_pages_exhausted_sets_truncated(monkeypatch):
    cfg = _slug_only_cfg(event_tags=("fifa-world-cup",))
    monkeypatch.setattr(
        scope_mod,
        "resolve_keyset_crawl_tags",
        lambda *a, **k: (["fifa-world-cup"], {"fifa-world-cup": {"seed"}}),
    )
    client = MagicMock()
    client.get.return_value = {"events": [], "next_cursor": None}
    scan = scope_mod._scan_wc2026_gamma_events(
        client, cfg, max_pages=0, tag_discovery=False
    )
    assert scan.truncated is True
