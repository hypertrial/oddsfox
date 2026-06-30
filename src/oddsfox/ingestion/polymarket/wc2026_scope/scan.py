"""Gamma event scanning and registry collection for WC2026."""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, Iterable, Iterator, Literal, Sequence

from oddsfox.config.settings import (
    POLYMARKET_WC2026_KEYSET_CLOSED,
    POLYMARKET_WC2026_KEYSET_VOLUME_MIN,
    POLYMARKET_WC2026_TAG_CLOSURE_ROUNDS,
    POLYMARKET_WC2026_TAG_CRAWL_MAX,
)
from oddsfox.ingestion.polymarket.gamma_events import (
    EventsPageMeta,
    iter_gamma_events_keyset,
)
from oddsfox.storage.duckdb.metadata import (
    get_wc2026_discovery_fully_checked,
    set_wc2026_discovery_fully_checked,
)
from oddsfox.storage.duckdb.wc2026_registry import (
    RegistryRow,
    upsert_registry_rows,
)

from .config import Wc2026ScopeConfig, scope_config_hash
from .predicates import (
    Wc2026EventsScanResult,
    _crawl_tag_allowed,
    _event_tag_slugs,
    _filter_crawl_tag_slugs,
    _resolve_keyset_closed,
    _resolve_keyset_related_tags,
    _resolve_keyset_volume_min,
    event_in_scope,
    event_matches_wc2026_config,
)

logger = logging.getLogger(__name__)

DISCOVERY_MODE_TARGETED = "targeted"
DISCOVERY_MODE_FULL_KEYSET = "full_keyset"
DiscoveryMode = Literal["targeted", "full_keyset"]
DEFAULT_MAX_PAGES_WITHOUT_PROGRESS = 25
_EVENTS_PAGE_MARKER: object = object()


def _event_slug_from_market(market: dict[str, Any]) -> tuple[str | None, str | None]:
    events = market.get("events")
    if not isinstance(events, list) or not events:
        return None, None
    first = events[0]
    if not isinstance(first, dict):
        return None, None
    slug = (first.get("slug") or "").strip().lower() or None
    event_id = str(first.get("id") or "") or None
    return slug, event_id


def _collect_from_events(
    events: Iterable[dict[str, Any]],
    cfg: Wc2026ScopeConfig,
    *,
    source: str = "events_api",
    require_allowlist_match: bool = True,
    keyset_tag_slug: str | None = None,
    keyset_related_tags: bool = False,
    scope_tag_slugs: Sequence[str] | None = None,
) -> Wc2026EventsScanResult:
    rows: list[RegistryRow] = []
    markets: list[dict[str, Any]] = []
    seen_market_ids: set[str] = set()
    discovered_slugs: set[str] = set()

    for event in events:
        event_slug = (event.get("slug") or "").strip().lower()
        if not event_slug:
            continue
        if require_allowlist_match and not event_in_scope(
            event,
            config=cfg,
            keyset_tag_slug=keyset_tag_slug,
            keyset_related_tags=keyset_related_tags,
            scope_tag_slugs=scope_tag_slugs,
        ):
            continue
        discovered_slugs.add(event_slug)
        event_id = str(event.get("id") or "") or None
        for market in event.get("markets") or []:
            if not isinstance(market, dict):
                continue
            market_id = str(market.get("id") or "").strip()
            if not market_id:
                continue
            rows.append(
                RegistryRow(
                    market_id=market_id,
                    event_slug=event_slug,
                    event_id=event_id,
                    source=source,
                )
            )
            if market_id in seen_market_ids:
                continue
            seen_market_ids.add(market_id)
            if not market.get("events"):
                market = {
                    **market,
                    "events": [{"slug": event_slug, "id": event.get("id")}],
                }
            markets.append(market)

    return Wc2026EventsScanResult(
        registry_rows=tuple(rows),
        raw_markets=tuple(markets),
        pages_done=0,
        truncated=False,
        discovered_slugs=tuple(sorted(discovered_slugs)),
    )


def _collect_from_market_payloads(
    market_payloads: Iterable[dict[str, Any]],
    cfg: Wc2026ScopeConfig,
    *,
    allowlisted_market_ids: set[str],
) -> Wc2026EventsScanResult:
    rows: list[RegistryRow] = []
    markets: list[dict[str, Any]] = []
    seen_market_ids: set[str] = set()
    discovered_slugs: set[str] = set()

    for market in market_payloads:
        if not isinstance(market, dict):
            continue
        market_id = str(market.get("id") or "").strip()
        if not market_id:
            continue
        event_slug, event_id = _event_slug_from_market(market)
        in_allowlist = market_id in allowlisted_market_ids
        slug_match = event_slug and event_matches_wc2026_config(event_slug, config=cfg)
        if not in_allowlist and not slug_match:
            continue
        if event_slug:
            discovered_slugs.add(event_slug)
        rows.append(
            RegistryRow(
                market_id=market_id,
                event_slug=event_slug,
                event_id=event_id,
                source="markets_api",
            )
        )
        if market_id in seen_market_ids:
            continue
        seen_market_ids.add(market_id)
        markets.append(market)

    return Wc2026EventsScanResult(
        registry_rows=tuple(rows),
        raw_markets=tuple(markets),
        pages_done=0,
        truncated=False,
        discovered_slugs=tuple(sorted(discovered_slugs)),
    )


def _merge_scan_results(
    left: Wc2026EventsScanResult,
    right: Wc2026EventsScanResult,
) -> Wc2026EventsScanResult:
    rows = list(left.registry_rows) + list(right.registry_rows)
    markets_by_id: dict[str, dict[str, Any]] = {
        str(m.get("id")): m for m in left.raw_markets if m.get("id")
    }
    for market in right.raw_markets:
        market_id = str(market.get("id") or "")
        if market_id:
            markets_by_id[market_id] = market
    slugs = set(left.discovered_slugs) | set(right.discovered_slugs)
    harvested = left.harvested_tag_slugs | right.harvested_tag_slugs
    return Wc2026EventsScanResult(
        registry_rows=tuple(rows),
        raw_markets=tuple(markets_by_id.values()),
        pages_done=max(left.pages_done, right.pages_done),
        truncated=left.truncated or right.truncated,
        discovered_slugs=tuple(sorted(slugs)),
        api_requests=left.api_requests + right.api_requests,
        harvested_tag_slugs=harvested,
        crawl_tag_slugs=left.crawl_tag_slugs or right.crawl_tag_slugs,
        scope_tag_slugs=left.scope_tag_slugs or right.scope_tag_slugs,
        tag_sources=left.tag_sources or right.tag_sources,
    )


def _record_discovery_ledger(
    cfg: Wc2026ScopeConfig,
    *,
    discovery_mode: DiscoveryMode,
    truncated: bool,
    events_pages: int,
) -> None:
    config_hash = scope_config_hash(cfg)
    from oddsfox.storage.duckdb.metadata import (
        get_wc2026_discovery_scope_config_hash,
    )

    stored_hash = get_wc2026_discovery_scope_config_hash()
    if stored_hash and stored_hash != config_hash:
        set_wc2026_discovery_fully_checked(False, scope_config_hash=config_hash)
    if discovery_mode == DISCOVERY_MODE_FULL_KEYSET and not truncated:
        set_wc2026_discovery_fully_checked(True, scope_config_hash=config_hash)
    elif get_wc2026_discovery_fully_checked() and truncated:
        set_wc2026_discovery_fully_checked(False, scope_config_hash=config_hash)


def _empty_scan_result() -> Wc2026EventsScanResult:
    return Wc2026EventsScanResult(
        registry_rows=(),
        raw_markets=(),
        pages_done=0,
        truncated=False,
        discovered_slugs=(),
        api_requests=0,
    )


def _resolve_tag_closure_rounds() -> int:
    rounds = POLYMARKET_WC2026_TAG_CLOSURE_ROUNDS
    return max(0, rounds)


def _resolve_tag_crawl_max() -> int | None:
    cap = POLYMARKET_WC2026_TAG_CRAWL_MAX
    if cap <= 0:
        return None
    return cap


def _seed_registry_rows(cfg: Wc2026ScopeConfig) -> list[RegistryRow]:
    rows: list[RegistryRow] = []
    for market_id in cfg.market_ids:
        rows.append(
            RegistryRow(
                market_id=market_id,
                event_slug=None,
                event_id=None,
                source="seed",
            )
        )
    return rows


def _dedupe_registry_rows(rows: Iterable[RegistryRow]) -> list[RegistryRow]:
    seen: dict[str, RegistryRow] = {}
    for row in rows:
        prev = seen.get(row.market_id)
        if prev is None or _source_priority(row.source) > _source_priority(prev.source):
            seen[row.market_id] = row
    return list(seen.values())


def _source_priority(source: str) -> int:
    return {"seed": 1, "events_api": 2, "markets_api": 2}.get(source, 0)


def _count_by_source(rows: Sequence[RegistryRow]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        counts[row.source] = counts.get(row.source, 0) + 1
    return counts


def _finalize_registry_collect(
    scan: Wc2026EventsScanResult,
    cfg: Wc2026ScopeConfig,
    *,
    discovery_mode: DiscoveryMode,
    t0: float,
    api_requests: int = 0,
    keyset_closed: bool | None = POLYMARKET_WC2026_KEYSET_CLOSED,
    keyset_tag_slugs: Sequence[str] | None = None,
    keyset_volume_min: float | None = POLYMARKET_WC2026_KEYSET_VOLUME_MIN,
) -> tuple[Dict[str, Any], list[dict[str, Any]], Dict[str, Any]]:
    _record_discovery_ledger(
        cfg,
        discovery_mode=discovery_mode,
        truncated=scan.truncated,
        events_pages=scan.pages_done,
    )
    seed_rows = _seed_registry_rows(cfg)
    merged = _dedupe_registry_rows(list(scan.registry_rows) + seed_rows)
    saved = upsert_registry_rows(merged)
    total_api = scan.api_requests + api_requests
    registry_summary = {
        "task": "refresh_wc2026_registry",
        "discovery_mode": discovery_mode,
        "events_pages": scan.pages_done,
        "truncated": scan.truncated,
        "registry_rows_upserted": saved,
        "discovered_event_slugs": list(scan.discovered_slugs),
        "by_source": _count_by_source(merged),
        "duration_seconds": round(time.monotonic() - t0, 3),
        "api_requests": total_api,
        "registry_refreshed": True,
    }
    if keyset_closed is not None:
        registry_summary["keyset_closed"] = keyset_closed
    effective_crawl_tags = (
        list(scan.crawl_tag_slugs)
        if scan.crawl_tag_slugs
        else (list(keyset_tag_slugs) if keyset_tag_slugs else [])
    )
    if effective_crawl_tags:
        registry_summary["keyset_tag_slugs"] = effective_crawl_tags
        registry_summary["crawl_tag_slugs"] = effective_crawl_tags
    if scan.scope_tag_slugs:
        registry_summary["scope_tag_slugs"] = list(scan.scope_tag_slugs)
    if scan.tag_sources:
        registry_summary["tag_sources"] = {
            slug: list(srcs) for slug, srcs in scan.tag_sources
        }
    if keyset_volume_min is not None:
        registry_summary["keyset_volume_min"] = keyset_volume_min
    markets = list(scan.raw_markets)
    collect_meta = {
        "discovery_mode": discovery_mode,
        "events_pages": scan.pages_done,
        "truncated": scan.truncated,
        "markets_collected": len(markets),
        "api_requests": total_api,
        "registry_refreshed": True,
    }
    if keyset_closed is not None:
        collect_meta["keyset_closed"] = keyset_closed
    if effective_crawl_tags:
        collect_meta["keyset_tag_slugs"] = effective_crawl_tags
        collect_meta["crawl_tag_slugs"] = effective_crawl_tags
    if scan.scope_tag_slugs:
        collect_meta["scope_tag_slugs"] = list(scan.scope_tag_slugs)
    if scan.tag_sources:
        collect_meta["tag_sources"] = {
            slug: list(srcs) for slug, srcs in scan.tag_sources
        }
    if keyset_volume_min is not None:
        collect_meta["keyset_volume_min"] = keyset_volume_min
    return registry_summary, markets, collect_meta


def _iter_wc2026_gamma_events(
    client: Any,
    cfg: Wc2026ScopeConfig,
    *,
    max_pages: int | None,
    keyset_closed: bool | None = None,
    keyset_tag_slug: str | None = None,
    keyset_related_tags: bool | None = None,
    keyset_volume_min: float | None = None,
    scope_tag_slugs: Sequence[str] | None = None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
    progress_task: str = "wc2026_registry_events",
) -> Iterator[tuple[dict[str, Any], str, EventsPageMeta]]:
    """Yield (event_dict, normalized_event_slug, page_meta) for allowlisted WC events."""
    effective_closed = _resolve_keyset_closed(keyset_closed)
    effective_related = _resolve_keyset_related_tags(keyset_related_tags)
    effective_volume = _resolve_keyset_volume_min(keyset_volume_min)
    page_cap = max_pages if max_pages is not None else cfg.registry_max_event_pages
    for events, page_meta in iter_gamma_events_keyset(
        client,
        max_pages=page_cap,
        keyset_closed=effective_closed,
        keyset_tag_slug=keyset_tag_slug,
        keyset_related_tags=effective_related,
        keyset_volume_min=effective_volume,
        progress_callback=progress_callback,
        progress_task=progress_task,
    ):
        if not events:
            break
        for event in events:
            event_slug = (event.get("slug") or "").strip().lower()
            if not event_slug or not event_in_scope(
                event,
                config=cfg,
                keyset_tag_slug=keyset_tag_slug,
                keyset_related_tags=effective_related,
                scope_tag_slugs=scope_tag_slugs,
            ):
                continue
            yield event, event_slug, page_meta
        yield _EVENTS_PAGE_MARKER, "", page_meta


def _scan_wc2026_gamma_events_keyset_pass(
    client: Any,
    cfg: Wc2026ScopeConfig,
    *,
    max_pages: int | None,
    max_pages_without_progress: int | None,
    keyset_closed: bool | None,
    keyset_tag_slug: str | None,
    keyset_related_tags: bool | None,
    keyset_volume_min: float | None,
    scope_tag_slugs: Sequence[str] | None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None,
    progress_task: str,
) -> Wc2026EventsScanResult:
    """Single /events/keyset pass with optional tag and volume filters."""
    effective_related = _resolve_keyset_related_tags(keyset_related_tags)
    effective_closed = _resolve_keyset_closed(keyset_closed)
    effective_volume = _resolve_keyset_volume_min(keyset_volume_min)
    page_cap = max_pages if max_pages is not None else cfg.registry_max_event_pages
    merged = _empty_scan_result()
    pages_done = 0
    truncated = False
    pages_without_progress = 0
    harvested_tag_slugs: set[str] = set()

    for events, page_meta in iter_gamma_events_keyset(
        client,
        max_pages=page_cap,
        keyset_closed=effective_closed,
        keyset_tag_slug=keyset_tag_slug,
        keyset_related_tags=effective_related,
        keyset_volume_min=effective_volume,
        progress_callback=progress_callback,
        progress_task=progress_task,
    ):
        pages_done = page_meta.pages_done
        truncated = page_meta.truncated
        if not events:
            break

        page_events = [
            event
            for event in events
            if (event.get("slug") or "").strip().lower()
            and event_in_scope(
                event,
                config=cfg,
                keyset_tag_slug=keyset_tag_slug,
                keyset_related_tags=effective_related,
                scope_tag_slugs=scope_tag_slugs,
            )
        ]
        if page_events:
            for event in page_events:
                harvested_tag_slugs.update(_event_tag_slugs(event))
            page_scan = _collect_from_events(
                page_events,
                cfg,
                keyset_tag_slug=keyset_tag_slug,
                keyset_related_tags=effective_related,
                scope_tag_slugs=scope_tag_slugs,
            )
            merged = _merge_scan_results(merged, page_scan)
            pages_without_progress = 0
        else:
            pages_without_progress += 1

        if (
            max_pages_without_progress is not None
            and pages_without_progress >= max_pages_without_progress
        ):
            truncated = True
            logger.info(
                "Stopping WC2026 /events scan after %s pages without allowlist matches",
                pages_without_progress,
            )
            break

        if not events or page_meta.truncated:
            break

    return Wc2026EventsScanResult(
        registry_rows=merged.registry_rows,
        raw_markets=merged.raw_markets,
        pages_done=pages_done,
        truncated=truncated,
        discovered_slugs=merged.discovered_slugs,
        api_requests=pages_done,
        harvested_tag_slugs=frozenset(harvested_tag_slugs),
    )


def _tag_crawl_key(tag_slug: str | None) -> str:
    return tag_slug if tag_slug is not None else "__all__"


def _scan_wc2026_gamma_events(
    client: Any,
    cfg: Wc2026ScopeConfig,
    *,
    max_pages: int | None,
    max_pages_without_progress: int | None = DEFAULT_MAX_PAGES_WITHOUT_PROGRESS,
    keyset_closed: bool | None = None,
    keyset_tag_slugs: Sequence[str] | None = None,
    keyset_related_tags: bool | None = None,
    keyset_volume_min: float | None = None,
    tag_discovery: bool | None = None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
    progress_task: str = "wc2026_registry_events",
) -> Wc2026EventsScanResult:
    """One or more /events/keyset passes: registry rows and raw markets for allowlisted WC events."""
    effective_related = _resolve_keyset_related_tags(keyset_related_tags)
    scope_tag_slugs = tuple(cfg.event_tags)
    scope_for_passes = scope_tag_slugs if scope_tag_slugs else None
    seed_tag_slugs = tuple(cfg.default_keyset_tag_slugs)
    from oddsfox.ingestion.polymarket.wc2026_scope import (
        resolve_keyset_crawl_tags as _resolve_keyset_crawl_tags,
    )

    initial_crawl_tags, tag_sources_map = _resolve_keyset_crawl_tags(
        keyset_tag_slugs,
        config=cfg,
        client=client,
        tag_discovery=tag_discovery,
    )
    initial_crawl_tags = _filter_crawl_tag_slugs(
        initial_crawl_tags,
        scope_tags=scope_tag_slugs,
        seed_tags=seed_tag_slugs,
    )
    logger.info(
        "WC2026 tag crawl scope anchored to %s; initial crawl tags %s",
        list(scope_tag_slugs),
        initial_crawl_tags,
    )

    max_closure_rounds = _resolve_tag_closure_rounds()
    max_crawl_tags = _resolve_tag_crawl_max()
    crawled_keys: list[str] = []
    crawled_set: set[str] = set()
    if initial_crawl_tags:
        queue: list[str | None] = list(dict.fromkeys(initial_crawl_tags))
    else:
        queue = [None]
    merged = _empty_scan_result()
    total_pages = 0
    truncated = False

    def _mark_crawled(crawl_key: str) -> bool:
        if crawl_key in crawled_set:  # pragma: no cover
            return False
        crawled_set.add(crawl_key)
        crawled_keys.append(crawl_key)
        return True

    for closure_round in range(max_closure_rounds + 1):
        if not queue:
            break
        next_queue: list[str | None] = []
        for tag_slug in queue:
            crawl_key = _tag_crawl_key(tag_slug)
            if crawl_key in crawled_set:  # pragma: no cover
                continue
            if (  # pragma: no cover
                max_crawl_tags is not None and len(crawled_set) >= max_crawl_tags
            ):
                truncated = True
                logger.info(
                    "WC2026 tag crawl cap reached (%s tags); stopping expansion",
                    max_crawl_tags,
                )
                break

            remaining_pages: int | None
            if max_pages is not None:
                remaining_pages = max_pages - total_pages
                if remaining_pages <= 0:
                    truncated = True
                    break
            else:
                remaining_pages = None

            _mark_crawled(crawl_key)
            pass_scan = _scan_wc2026_gamma_events_keyset_pass(
                client,
                cfg,
                max_pages=remaining_pages,
                max_pages_without_progress=max_pages_without_progress,
                keyset_closed=keyset_closed,
                keyset_tag_slug=tag_slug,
                keyset_related_tags=effective_related,
                keyset_volume_min=keyset_volume_min,
                scope_tag_slugs=scope_for_passes,
                progress_callback=progress_callback,
                progress_task=progress_task,
            )
            merged = _merge_scan_results(merged, pass_scan)
            total_pages += pass_scan.pages_done

            for harvested_slug in pass_scan.harvested_tag_slugs:
                if (
                    harvested_slug in crawled_set
                    or harvested_slug in next_queue
                    or harvested_slug is None
                ):
                    continue
                if not _crawl_tag_allowed(
                    harvested_slug,
                    scope_tags=scope_tag_slugs,
                    seed_tags=seed_tag_slugs,
                ):
                    continue
                next_queue.append(harvested_slug)
                tag_sources_map.setdefault(harvested_slug, set()).add("event_closure")

            if pass_scan.truncated:
                truncated = True
                break

        if truncated:
            break
        queue = list(dict.fromkeys(next_queue))

    final_crawl = tuple(slug for slug in crawled_keys if slug != "__all__")
    final_sources = tuple(
        (slug, tuple(sorted(tag_sources_map.get(slug, ("unknown",)))))
        for slug in final_crawl
    )
    logger.info(
        "WC2026 tag crawl complete: %s tags (scope anchored to %s)",
        len(final_crawl),
        list(scope_tag_slugs),
    )
    if final_sources:
        logger.info("WC2026 tag crawl sources: %s", dict(final_sources))

    return Wc2026EventsScanResult(
        registry_rows=merged.registry_rows,
        raw_markets=merged.raw_markets,
        pages_done=total_pages,
        truncated=truncated,
        discovered_slugs=merged.discovered_slugs,
        api_requests=total_pages,
        crawl_tag_slugs=final_crawl,
        scope_tag_slugs=scope_tag_slugs,
        tag_sources=final_sources,
    )
