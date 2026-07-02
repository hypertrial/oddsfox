"""Unit tests for WC2026 scope config helpers."""

from __future__ import annotations

import pytest

from oddsfox.ingestion.polymarket.scope_sql import _sql_quote_list
from oddsfox.ingestion.polymarket.wc2026_scope import (
    MARKET_SCOPE_ALL,
    MARKET_SCOPE_WC2026,
    Wc2026ScopeConfig,
    event_matches_wc2026_config,
    is_wc2026_market_row,
    load_wc2026_config,
    market_scope_predicate_sql,
    market_scope_sql,
    validate_market_scope,
)
from oddsfox.ingestion.polymarket.wc2026_scope import (
    config as scope_config_mod,
)


def test_validate_market_scope_rejects_unknown_scope():
    with pytest.raises(ValueError, match="bad-scope"):
        validate_market_scope("bad-scope")


def test_load_wc2026_config_includes_default_slug(monkeypatch):
    monkeypatch.delenv("POLYMARKET_WC2026_EVENT_TAGS", raising=False)
    cfg = load_wc2026_config()
    assert "2026-fifa-world-cup-winner" in cfg.event_slugs
    assert cfg.event_slug_prefixes
    assert "2026-fifa-world-cup" in cfg.event_tags
    assert "fifa-world-cup" in cfg.event_tags
    assert "world-cup" in cfg.event_tags


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
