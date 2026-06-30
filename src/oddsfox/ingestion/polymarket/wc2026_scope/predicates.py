"""WC2026 event/market scope predicates and tag crawl rules."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Sequence

from oddsfox.config.settings import (
    POLYMARKET_WC2026_KEYSET_CLOSED,
    POLYMARKET_WC2026_KEYSET_RELATED_TAGS,
    POLYMARKET_WC2026_KEYSET_VOLUME_MIN,
    POLYMARKET_WC2026_TAG_CLOSURE_KEYWORD_GATE,
    POLYMARKET_WC2026_TAG_CRAWL_DENYLIST,
    POLYMARKET_WC2026_TAG_DISCOVERY,
    POLYMARKET_WC2026_TAG_DISCOVERY_KEYWORDS,
)
from oddsfox.ingestion.polymarket.scope_sql import (
    MARKET_SCOPE_ALL,
    MARKET_SCOPE_WC2026,
    validate_market_scope,
)
from oddsfox.storage.duckdb.wc2026_registry import RegistryRow

from .config import Wc2026ScopeConfig, _validate_slug_token, load_wc2026_config

logger = logging.getLogger(__name__)


def _event_tag_slugs(event: dict[str, Any]) -> frozenset[str]:
    tags = event.get("tags")
    if not isinstance(tags, list):
        return frozenset()
    slugs: set[str] = set()
    for tag in tags:
        if isinstance(tag, dict):
            slug = (tag.get("slug") or "").strip().lower()
            if slug:
                slugs.add(slug)
    return frozenset(slugs)


def _resolve_keyset_closed(keyset_closed: bool | None) -> bool | None:
    if keyset_closed is not None:
        return keyset_closed
    return POLYMARKET_WC2026_KEYSET_CLOSED


def _resolve_keyset_volume_min(keyset_volume_min: float | None) -> float | None:
    if keyset_volume_min is not None:
        return keyset_volume_min
    return POLYMARKET_WC2026_KEYSET_VOLUME_MIN


def _resolve_keyset_related_tags(keyset_related_tags: bool | None) -> bool:
    if keyset_related_tags is not None:
        return keyset_related_tags
    return POLYMARKET_WC2026_KEYSET_RELATED_TAGS


def _parse_tag_discovery_keywords(raw: str | None) -> tuple[str, ...]:
    if not raw or not str(raw).strip():
        from oddsfox.ingestion.polymarket.wc2026_tags import (
            DEFAULT_WC2026_TAG_DISCOVERY_KEYWORDS,
        )

        return DEFAULT_WC2026_TAG_DISCOVERY_KEYWORDS
    return tuple(k.strip().lower() for k in str(raw).split(",") if k.strip())


def _crawl_tag_allowed(
    slug: str | None,
    *,
    scope_tags: Sequence[str],
    seed_tags: Sequence[str],
    denylist: Sequence[str] | None = None,
    keyword_gate: bool | None = None,
    keywords: Sequence[str] | None = None,
) -> bool:
    """True when a tag slug may be crawled via /events/keyset (not scope admission)."""
    if slug is None:
        return True
    normalized = str(slug).strip().lower()
    if not normalized:
        return False

    scope_set = frozenset(str(t).strip().lower() for t in scope_tags if str(t).strip())
    seed_set = frozenset(str(t).strip().lower() for t in seed_tags if str(t).strip())
    if normalized in scope_set or normalized in seed_set:
        return True

    effective_denylist = (
        denylist if denylist is not None else POLYMARKET_WC2026_TAG_CRAWL_DENYLIST
    )
    if normalized in frozenset(
        str(d).strip().lower() for d in effective_denylist if str(d).strip()
    ):
        return False

    effective_gate = (
        POLYMARKET_WC2026_TAG_CLOSURE_KEYWORD_GATE
        if keyword_gate is None
        else keyword_gate
    )
    if not effective_gate:
        return True

    from oddsfox.ingestion.polymarket.wc2026_tags import tag_matches_keywords

    effective_keywords = (
        keywords
        if keywords is not None
        else _parse_tag_discovery_keywords(POLYMARKET_WC2026_TAG_DISCOVERY_KEYWORDS)
    )
    return tag_matches_keywords({"slug": normalized}, effective_keywords)


def _filter_crawl_tag_slugs(
    tag_slugs: Sequence[str],
    *,
    scope_tags: Sequence[str],
    seed_tags: Sequence[str],
) -> list[str]:
    filtered: list[str] = []
    for slug in tag_slugs:
        if _crawl_tag_allowed(slug, scope_tags=scope_tags, seed_tags=seed_tags):
            filtered.append(slug)
        else:
            logger.info(
                "Skipping WC2026 crawl tag %s (denylist or keyword gate)",
                slug,
            )
    return filtered


def event_matches_wc2026_tags(
    event: dict[str, Any] | None,
    *,
    config: Wc2026ScopeConfig | None = None,
    scope_tag_slugs: Sequence[str] | None = None,
) -> bool:
    if not event or not isinstance(event, dict):
        return False
    cfg = config or load_wc2026_config()
    allowed_slugs = (
        tuple(scope_tag_slugs) if scope_tag_slugs is not None else cfg.event_tags
    )
    if not allowed_slugs:
        return False
    allowed = frozenset(allowed_slugs)
    return bool(_event_tag_slugs(event) & allowed)


def event_in_scope(
    event: dict[str, Any] | None,
    *,
    config: Wc2026ScopeConfig | None = None,
    keyset_tag_slug: str | None = None,
    keyset_related_tags: bool = False,
    scope_tag_slugs: Sequence[str] | None = None,
) -> bool:
    """True when a Gamma event belongs to the configured WC2026 scope."""
    if not event or not isinstance(event, dict):
        return False
    cfg = config or load_wc2026_config()
    if keyset_tag_slug is not None and not keyset_related_tags:
        normalized_tag = _validate_slug_token(keyset_tag_slug)
        allowlist = (
            frozenset(scope_tag_slugs)
            if scope_tag_slugs is not None
            else frozenset(cfg.event_tags)
        )
        if normalized_tag in allowlist:
            return True
        return event_matches_wc2026_tags(
            event, config=cfg, scope_tag_slugs=scope_tag_slugs
        )
    event_slug = (event.get("slug") or "").strip().lower()
    if event_slug and event_matches_wc2026_config(event_slug, config=cfg):
        return True
    return event_matches_wc2026_tags(event, config=cfg, scope_tag_slugs=scope_tag_slugs)


def resolve_keyset_crawl_tags(
    keyset_tag_slugs: Sequence[str] | None,
    *,
    config: Wc2026ScopeConfig | None = None,
    client: Any = None,
    tag_discovery: bool | None = None,
) -> tuple[list[str], dict[str, set[str]]]:
    """Return crawl tag slugs for /events/keyset and their discovery sources."""
    if keyset_tag_slugs is not None:
        slugs = list(keyset_tag_slugs)
        return slugs, {slug: {"explicit"} for slug in slugs}
    cfg = config or load_wc2026_config()
    base = list(cfg.default_keyset_tag_slugs)
    sources: dict[str, set[str]] = {slug: {"seed"} for slug in base}
    discovery_enabled = (
        POLYMARKET_WC2026_TAG_DISCOVERY if tag_discovery is None else tag_discovery
    )
    if not discovery_enabled or client is None:
        return base, sources
    from oddsfox.ingestion.polymarket.wc2026_tags import (
        discover_wc2026_tag_slugs,
    )

    try:
        discovered = discover_wc2026_tag_slugs(
            client,
            seed_slugs=base,
            keywords=_parse_tag_discovery_keywords(
                POLYMARKET_WC2026_TAG_DISCOVERY_KEYWORDS
            ),
        )
    except Exception:
        logger.warning(
            "WC2026 tag discovery failed; using configured event_tags only",
            exc_info=True,
        )
        return base, sources
    for slug, srcs in discovered.sources.items():
        sources.setdefault(slug, set()).update(srcs)
    merged = sorted({*base, *discovered.tag_slugs})
    if merged != base:
        logger.info(
            "WC2026 tag discovery expanded crawl tags from %s to %s",
            base,
            merged,
        )
    return merged, sources


def resolve_keyset_tag_slugs(
    keyset_tag_slugs: Sequence[str] | None,
    *,
    config: Wc2026ScopeConfig | None = None,
    client: Any = None,
    tag_discovery: bool | None = None,
) -> list[str]:
    """Return tag slugs for /events/keyset passes (explicit list or config default)."""
    crawl_tags, _sources = resolve_keyset_crawl_tags(
        keyset_tag_slugs,
        config=config,
        client=client,
        tag_discovery=tag_discovery,
    )
    return crawl_tags


def is_wc2026_market_row(
    *,
    market_id: str,
    question: str = "",
    category: str = "",
    description: str = "",
    slug: str = "",
    event_slug: str = "",
    event_tags: Sequence[str] = (),
    market_scope: str = MARKET_SCOPE_WC2026,
    config: Wc2026ScopeConfig | None = None,
    in_registry: bool = False,
) -> bool:
    """Pure-Python scope check for unit tests (approximates SQL rules)."""
    scope = validate_market_scope(market_scope)
    if scope == MARKET_SCOPE_ALL:
        return True

    cfg = config or load_wc2026_config()
    if in_registry or market_id in cfg.market_ids:
        return True
    es = (event_slug or "").lower()
    if es in cfg.event_slugs:
        return True
    if any(es.startswith(p) for p in cfg.event_slug_prefixes):
        return True
    if cfg.event_tags and event_tags:
        allowed = frozenset(cfg.event_tags)
        if allowed & {t.strip().lower() for t in event_tags if str(t).strip()}:
            return True
    return False


def event_matches_wc2026_config(
    event_slug: str | None,
    *,
    config: Wc2026ScopeConfig | None = None,
) -> bool:
    if not event_slug:
        return False
    cfg = config or load_wc2026_config()
    slug = event_slug.strip().lower()
    if slug in cfg.event_slugs:
        return True
    return any(slug.startswith(p) for p in cfg.event_slug_prefixes)


@dataclass(frozen=True)
class Wc2026EventsScanResult:
    """Gamma WC2026 collection: registry rows and raw market payloads."""

    registry_rows: tuple[RegistryRow, ...]
    raw_markets: tuple[dict[str, Any], ...]
    pages_done: int
    truncated: bool
    discovered_slugs: tuple[str, ...]
    api_requests: int = 0
    harvested_tag_slugs: frozenset[str] = frozenset()
    crawl_tag_slugs: tuple[str, ...] = ()
    scope_tag_slugs: tuple[str, ...] = ()
    tag_sources: tuple[tuple[str, tuple[str, ...]], ...] = ()
