"""WC2026 registry refresh entrypoints."""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, Sequence

from oddsfox.ingestion.polymarket.gamma_events import fetch_gamma_event_by_slug
from oddsfox.storage.duckdb.wc2026_registry import (
    get_registry_market_ids,
)

from .config import Wc2026ScopeConfig, load_wc2026_config
from .gamma import (
    _chunk_market_ids,
    _fetch_markets_batch_resilient,
    _gamma_market_ids,
)
from .predicates import _resolve_keyset_closed, _resolve_keyset_volume_min
from .scan import (
    DEFAULT_MAX_PAGES_WITHOUT_PROGRESS,
    DISCOVERY_MODE_FULL_KEYSET,
    DISCOVERY_MODE_TARGETED,
    Wc2026EventsScanResult,
    _collect_from_events,
    _collect_from_market_payloads,
    _empty_scan_result,
    _finalize_registry_collect,
    _merge_scan_results,
    _scan_wc2026_gamma_events,
)

logger = logging.getLogger(__name__)

_TARGETED_MARKETS_BATCH_SIZE = 50


def refresh_registry_and_collect_markets_targeted(
    client: Any,
    *,
    config: Wc2026ScopeConfig | None = None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> tuple[Dict[str, Any], list[dict[str, Any]], Dict[str, Any]]:
    """Targeted discovery: allowlisted slugs plus /markets by seed and registry IDs."""
    cfg = config or load_wc2026_config()
    t0 = time.monotonic()
    merged = _empty_scan_result()
    api_requests = 0

    for slug in cfg.event_slugs:
        event = fetch_gamma_event_by_slug(client, slug)
        api_requests += 1
        if progress_callback:
            try:
                progress_callback(
                    "wc2026_event_by_slug",
                    {"slug": slug, "found": event is not None},
                )
            except Exception:
                logger.debug("Ignoring slug progress callback failure", exc_info=True)
        if event:
            merged = _merge_scan_results(merged, _collect_from_events([event], cfg))

    allowlisted_ids = set(cfg.market_ids) | set(get_registry_market_ids())
    market_ids = _gamma_market_ids(allowlisted_ids)
    for batch_idx, chunk in enumerate(
        _chunk_market_ids(market_ids, _TARGETED_MARKETS_BATCH_SIZE), start=1
    ):
        payloads = _fetch_markets_batch_resilient(client, chunk, include_events=True)
        api_requests += 1
        if progress_callback:
            try:
                progress_callback(
                    "wc2026_markets_by_id",
                    {
                        "batch": batch_idx,
                        "chunk_size": len(chunk),
                        "markets_fetched": len(payloads or []),
                    },
                )
            except Exception:
                logger.debug(
                    "Ignoring markets-by-id progress callback failure", exc_info=True
                )
        merged = _merge_scan_results(
            merged,
            _collect_from_market_payloads(
                payloads or [], cfg, allowlisted_market_ids=allowlisted_ids
            ),
        )

    merged = Wc2026EventsScanResult(
        registry_rows=merged.registry_rows,
        raw_markets=merged.raw_markets,
        pages_done=0,
        truncated=False,
        discovered_slugs=merged.discovered_slugs,
        api_requests=api_requests,
    )
    return _finalize_registry_collect(
        merged,
        cfg,
        discovery_mode=DISCOVERY_MODE_TARGETED,
        t0=t0,
    )


def refresh_registry_from_events(
    client: Any,
    *,
    config: Wc2026ScopeConfig | None = None,
    max_pages: int | None = None,
    max_pages_without_progress: int | None = DEFAULT_MAX_PAGES_WITHOUT_PROGRESS,
    keyset_closed: bool | None = None,
    keyset_tag_slugs: Sequence[str] | None = None,
    keyset_related_tags: bool | None = None,
    keyset_volume_min: float | None = None,
    tag_discovery: bool | None = None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> Dict[str, Any]:
    """Scan Gamma /events/keyset and upsert WC 2026 markets into the ops registry."""
    cfg = config or load_wc2026_config()
    t0 = time.monotonic()
    effective_closed = _resolve_keyset_closed(keyset_closed)
    effective_volume = _resolve_keyset_volume_min(keyset_volume_min)
    scan = _scan_wc2026_gamma_events(
        client,
        cfg,
        max_pages=max_pages,
        max_pages_without_progress=max_pages_without_progress,
        keyset_closed=effective_closed,
        keyset_tag_slugs=keyset_tag_slugs,
        keyset_related_tags=keyset_related_tags,
        keyset_volume_min=effective_volume,
        tag_discovery=tag_discovery,
        progress_callback=progress_callback,
    )
    registry_summary, _, _ = _finalize_registry_collect(
        scan,
        cfg,
        discovery_mode=DISCOVERY_MODE_FULL_KEYSET,
        t0=t0,
        keyset_closed=effective_closed,
        keyset_tag_slugs=keyset_tag_slugs,
        keyset_volume_min=keyset_volume_min,
    )
    return registry_summary


def collect_wc2026_markets_from_events(
    client: Any,
    *,
    config: Wc2026ScopeConfig | None = None,
    max_pages: int | None = None,
    keyset_closed: bool | None = None,
    keyset_tag_slugs: Sequence[str] | None = None,
    keyset_related_tags: bool | None = None,
    keyset_volume_min: float | None = None,
    tag_discovery: bool | None = None,
) -> tuple[list[dict[str, Any]], Dict[str, Any]]:
    """Return raw Gamma market dicts for WC events (for event-first inventory sync)."""
    cfg = config or load_wc2026_config()
    effective_closed = _resolve_keyset_closed(keyset_closed)
    effective_volume = _resolve_keyset_volume_min(keyset_volume_min)
    scan = _scan_wc2026_gamma_events(
        client,
        cfg,
        max_pages=max_pages,
        keyset_closed=effective_closed,
        keyset_tag_slugs=keyset_tag_slugs,
        keyset_related_tags=keyset_related_tags,
        keyset_volume_min=effective_volume,
        tag_discovery=tag_discovery,
        progress_task="wc2026_market_events",
    )
    markets = list(scan.raw_markets)
    meta = {
        "events_pages": scan.pages_done,
        "truncated": scan.truncated,
        "markets_collected": len(markets),
    }
    if effective_closed is not None:
        meta["keyset_closed"] = effective_closed
    if scan.crawl_tag_slugs:
        meta["keyset_tag_slugs"] = list(scan.crawl_tag_slugs)
        meta["crawl_tag_slugs"] = list(scan.crawl_tag_slugs)
    elif keyset_tag_slugs:
        meta["keyset_tag_slugs"] = list(keyset_tag_slugs)
    if scan.scope_tag_slugs:
        meta["scope_tag_slugs"] = list(scan.scope_tag_slugs)
    if scan.tag_sources:
        meta["tag_sources"] = {slug: list(srcs) for slug, srcs in scan.tag_sources}
    if effective_volume is not None:
        meta["keyset_volume_min"] = effective_volume
    return markets, meta


def refresh_registry_and_collect_markets_from_events(
    client: Any,
    *,
    config: Wc2026ScopeConfig | None = None,
    max_pages: int | None = None,
    max_pages_without_progress: int | None = DEFAULT_MAX_PAGES_WITHOUT_PROGRESS,
    keyset_closed: bool | None = None,
    keyset_tag_slugs: Sequence[str] | None = None,
    keyset_related_tags: bool | None = None,
    keyset_volume_min: float | None = None,
    tag_discovery: bool | None = None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> tuple[Dict[str, Any], list[dict[str, Any]], Dict[str, Any]]:
    """Single /events pass: upsert registry and return raw markets for event-first sync."""
    cfg = config or load_wc2026_config()
    t0 = time.monotonic()
    effective_closed = _resolve_keyset_closed(keyset_closed)
    effective_volume = _resolve_keyset_volume_min(keyset_volume_min)
    scan = _scan_wc2026_gamma_events(
        client,
        cfg,
        max_pages=max_pages,
        max_pages_without_progress=max_pages_without_progress,
        keyset_closed=effective_closed,
        keyset_tag_slugs=keyset_tag_slugs,
        keyset_related_tags=keyset_related_tags,
        keyset_volume_min=effective_volume,
        tag_discovery=tag_discovery,
        progress_callback=progress_callback,
        progress_task="wc2026_market_events",
    )
    return _finalize_registry_collect(
        scan,
        cfg,
        discovery_mode=DISCOVERY_MODE_FULL_KEYSET,
        t0=t0,
        keyset_closed=effective_closed,
        keyset_tag_slugs=keyset_tag_slugs,
        keyset_volume_min=effective_volume,
    )
