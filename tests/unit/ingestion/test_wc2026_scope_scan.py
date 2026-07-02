"""Unit tests for WC2026 scope scan helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

from tests.unit.ingestion.wc2026_scope_test_support import slug_only_cfg

from oddsfox.config import settings as config_settings
from oddsfox.ingestion.polymarket import wc2026_scope as scope_mod
from oddsfox.ingestion.polymarket.wc2026_scope import (
    Wc2026EventsScanResult,
    Wc2026ScopeConfig,
)
from oddsfox.ingestion.polymarket.wc2026_scope import (
    scan as scope_scan_mod,
)
from oddsfox.storage.duckdb.wc2026_registry import RegistryRow


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
    monkeypatch.setattr(
        config_settings, "POLYMARKET_WC2026_TAG_CLOSURE_ROUNDS", 1, raising=False
    )
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


def test_scan_skips_duplicate_crawl_keys(monkeypatch):
    cfg = Wc2026ScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=5,
        event_tags=("fifa-world-cup", "world-cup"),
    )
    monkeypatch.setattr(
        scope_mod,
        "resolve_keyset_crawl_tags",
        lambda *args, **kwargs: (
            ["fifa-world-cup", "world-cup"],
            {"fifa-world-cup": {"seed"}, "world-cup": {"seed"}},
        ),
    )
    monkeypatch.setattr(scope_scan_mod, "_tag_crawl_key", lambda _tag: "same-key")

    client = MagicMock()
    client.get.return_value = {"events": [], "next_cursor": None}
    scan = scope_mod._scan_wc2026_gamma_events(
        client,
        cfg,
        max_pages=10,
        tag_discovery=False,
    )

    assert client.get.call_count == 1
    assert scan.crawl_tag_slugs == ("same-key",)


def test_queue_harvested_crawl_tags_filters_duplicates_and_scope():
    next_queue: list[str | None] = ["already-queued"]
    tag_sources: dict[str, set[str]] = {}

    scope_scan_mod._queue_harvested_crawl_tags(
        [
            None,
            "fifa-world-cup",
            "already-queued",
            "world-cup-qualifiers",
            "argentina",
            "world-cup-qualifiers",
        ],
        crawled_set={"fifa-world-cup"},
        next_queue=next_queue,
        scope_tag_slugs=("fifa-world-cup",),
        seed_tag_slugs=("fifa-world-cup",),
        tag_sources_map=tag_sources,
    )

    assert next_queue == ["already-queued", "world-cup-qualifiers"]
    assert tag_sources == {"world-cup-qualifiers": {"event_closure"}}


def test_scan_crawl_state_helpers():
    crawled_set: set[str] = set()
    crawled_keys: list[str] = []

    assert (
        scope_scan_mod._mark_crawled(
            "fifa-world-cup",
            crawled_set=crawled_set,
            crawled_keys=crawled_keys,
        )
        is True
    )
    assert (
        scope_scan_mod._mark_crawled(
            "fifa-world-cup",
            crawled_set=crawled_set,
            crawled_keys=crawled_keys,
        )
        is False
    )
    assert crawled_keys == ["fifa-world-cup"]
    assert scope_scan_mod._crawl_cap_reached(1, 1) is True
    assert scope_scan_mod._crawl_cap_reached(1, 2) is False
    assert scope_scan_mod._crawl_cap_reached(100, None) is False
    assert scope_scan_mod._remaining_page_budget(None, 3) is None
    assert scope_scan_mod._remaining_page_budget(5, 3) == 2


def test_scan_collection_parity_with_closure_gate_on_vs_off(monkeypatch):
    cfg = Wc2026ScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=5,
        event_tags=("fifa-world-cup",),
    )
    monkeypatch.setattr(
        config_settings, "POLYMARKET_WC2026_TAG_CLOSURE_ROUNDS", 1, raising=False
    )
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
        config_settings,
        "POLYMARKET_WC2026_TAG_CLOSURE_KEYWORD_GATE",
        True,
        raising=False,
    )
    gated = scope_mod._scan_wc2026_gamma_events(
        client, cfg, max_pages=10, tag_discovery=False
    )

    monkeypatch.setattr(
        config_settings,
        "POLYMARKET_WC2026_TAG_CLOSURE_KEYWORD_GATE",
        False,
        raising=False,
    )
    ungated = scope_mod._scan_wc2026_gamma_events(
        client, cfg, max_pages=10, tag_discovery=False
    )

    assert {m["id"] for m in gated.raw_markets} == {"m1", "m2"}
    assert {m["id"] for m in ungated.raw_markets} == {"m1", "m2"}
    assert "sports" not in gated.crawl_tag_slugs
    assert "portugal" not in gated.crawl_tag_slugs


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


def test_iter_wc2026_gamma_events_yields_allowlisted_only():
    cfg = slug_only_cfg(event_slugs=("2026-fifa-world-cup-winner-595",))
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


def test_scan_tag_crawl_max_truncates(monkeypatch):
    cfg = Wc2026ScopeConfig(
        event_slugs=(),
        event_slug_prefixes=(),
        market_ids=(),
        registry_max_event_pages=5,
        event_tags=("fifa-world-cup", "argentina"),
    )
    monkeypatch.setattr(
        config_settings, "POLYMARKET_WC2026_TAG_CRAWL_MAX", 1, raising=False
    )
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


def test_resolve_tag_crawl_max_disabled(monkeypatch) -> None:
    monkeypatch.setattr(
        config_settings, "POLYMARKET_WC2026_TAG_CRAWL_MAX", 0, raising=False
    )
    assert scope_scan_mod._resolve_tag_crawl_max() is None


def test_scan_max_pages_exhausted_sets_truncated(monkeypatch):
    cfg = slug_only_cfg(event_tags=("fifa-world-cup",))
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
