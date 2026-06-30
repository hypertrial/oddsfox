"""WC 2026 allowlist parity: Python seed, dbt vars, and default env slug must stay aligned.

Seed ``market_ids`` affect Python registry upsert only; dbt staging uses registry plus
``wc2026_event_tags``, ``wc2026_event_slugs``, and ``wc2026_event_slug_prefixes`` vars
(not seed market_ids).
"""

from __future__ import annotations

from pathlib import Path

import yaml

from oddsfox.config.settings_polymarket import (
    POLYMARKET_WC2026_DEFAULT_EVENT_SLUG,
)
from oddsfox.ingestion.polymarket.wc2026_scope import load_wc2026_config

REPO_ROOT = Path(__file__).resolve().parents[3]
DBT_PROJECT = REPO_ROOT / "dbt" / "dbt_project.yml"


def _normalized_set(values: tuple[str, ...] | list[str]) -> frozenset[str]:
    return frozenset(v.strip().lower() for v in values if str(v).strip())


def test_wc2026_seed_matches_dbt_vars(monkeypatch) -> None:
    monkeypatch.delenv("POLYMARKET_WC2026_EVENT_SLUGS", raising=False)
    monkeypatch.delenv("POLYMARKET_WC2026_EVENT_SLUG_PREFIXES", raising=False)
    monkeypatch.delenv("POLYMARKET_WC2026_EVENT_TAGS", raising=False)

    cfg = load_wc2026_config()
    raw = yaml.safe_load(DBT_PROJECT.read_text(encoding="utf-8")) or {}
    vars_block = raw.get("vars") or {}

    dbt_slugs = _normalized_set(vars_block.get("wc2026_event_slugs") or [])
    dbt_prefixes = _normalized_set(vars_block.get("wc2026_event_slug_prefixes") or [])
    dbt_tags = _normalized_set(vars_block.get("wc2026_event_tags") or [])

    assert _normalized_set(cfg.event_slugs) == dbt_slugs
    assert _normalized_set(cfg.event_slug_prefixes) == dbt_prefixes
    assert _normalized_set(cfg.event_tags) == dbt_tags


def test_default_event_slug_in_seed_allowlist(monkeypatch) -> None:
    monkeypatch.delenv("POLYMARKET_WC2026_EVENT_SLUGS", raising=False)
    monkeypatch.delenv("POLYMARKET_WC2026_EVENT_SLUG_PREFIXES", raising=False)

    cfg = load_wc2026_config()
    default_slug = POLYMARKET_WC2026_DEFAULT_EVENT_SLUG.strip().lower()
    seed_slugs = _normalized_set(cfg.event_slugs)

    assert default_slug in seed_slugs


def test_wc2026_keyset_discovery_defaults(monkeypatch) -> None:
    monkeypatch.delenv("POLYMARKET_WC2026_KEYSET_CLOSED", raising=False)
    monkeypatch.delenv("POLYMARKET_WC2026_KEYSET_VOLUME_MIN", raising=False)
    from oddsfox.config._reload_settings import reload_all_settings_modules

    reload_all_settings_modules()
    from oddsfox.config.settings_polymarket import (
        POLYMARKET_WC2026_KEYSET_CLOSED as closed,
    )
    from oddsfox.config.settings_polymarket import (
        POLYMARKET_WC2026_KEYSET_VOLUME_MIN as volume_min,
    )

    assert closed is False
    assert volume_min == 10000.0


def test_wc2026_keyset_discovery_omit_filters_via_empty_env(monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_WC2026_KEYSET_CLOSED", "any")
    monkeypatch.setenv("POLYMARKET_WC2026_KEYSET_VOLUME_MIN", "")
    from oddsfox.config._reload_settings import reload_all_settings_modules

    reload_all_settings_modules()
    from oddsfox.config.settings_polymarket import (
        POLYMARKET_WC2026_KEYSET_CLOSED as closed,
    )
    from oddsfox.config.settings_polymarket import (
        POLYMARKET_WC2026_KEYSET_VOLUME_MIN as volume_min,
    )

    assert closed is None
    assert volume_min is None


def test_load_wc2026_config_includes_world_cup_tag(monkeypatch) -> None:
    monkeypatch.delenv("POLYMARKET_WC2026_EVENT_TAGS", raising=False)
    cfg = load_wc2026_config()
    assert "world-cup" in cfg.event_tags
