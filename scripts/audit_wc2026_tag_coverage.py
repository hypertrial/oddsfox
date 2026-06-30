#!/usr/bin/env python3
"""
Reconcile WC2026 Gamma tag/search discovery against polymarket_ops.wc2026_market_registry.

Fetches tag slugs via runtime discovery, crawls GET /events/keyset per tag (with
related_tags), and unions GET /public-search events. Reports events whose markets
are absent from the registry.

Usage:
  python3 scripts/audit_wc2026_tag_coverage.py
  python3 scripts/audit_wc2026_tag_coverage.py --fail-on-gaps
  python3 scripts/audit_wc2026_tag_coverage.py --no-tag-discovery --max-pages 2
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
for _rp in (REPO_ROOT, REPO_ROOT / "src"):
    if str(_rp) not in sys.path:
        sys.path.insert(0, str(_rp))

from oddsfox.config.settings import (  # noqa: E402
    POLYMARKET_WC2026_KEYSET_CLOSED,
    POLYMARKET_WC2026_KEYSET_RELATED_TAGS,
    POLYMARKET_WC2026_KEYSET_VOLUME_MIN,
    POLYMARKET_WC2026_TAG_DISCOVERY,
)
from oddsfox.ingestion.polymarket.errors import gamma_get  # noqa: E402
from oddsfox.ingestion.polymarket.gamma_events import (  # noqa: E402
    iter_gamma_events_keyset,
)
from oddsfox.ingestion.polymarket.markets.fetch import (  # noqa: E402
    build_client,
)
from oddsfox.ingestion.polymarket.wc2026_scope import (  # noqa: E402
    event_in_scope,
    load_wc2026_config,
    resolve_keyset_tag_slugs,
)
from oddsfox.ingestion.polymarket.wc2026_tags import (  # noqa: E402
    discover_wc2026_tag_slugs,
)
from oddsfox.storage.duckdb.connection import (  # noqa: E402
    ensure_duck_db,
    get_connection,
)
from oddsfox.storage.duckdb.schemas.constants import (  # noqa: E402
    polymarket_ops_tbl,
)
from oddsfox.storage.duckdb.wc2026_registry import (  # noqa: E402
    get_registry_market_ids,
    registry_market_count,
)

logger = logging.getLogger(__name__)

_PUBLIC_SEARCH_QUERIES = ("FIFA World Cup", "World Cup")


def _market_ids_from_event(event: dict[str, Any]) -> set[str]:
    ids: set[str] = set()
    for market in event.get("markets") or []:
        if isinstance(market, dict):
            mid = str(market.get("id") or "").strip()
            if mid:
                ids.add(mid)
    return ids


def _event_is_in_registry(event: dict[str, Any], registry_ids: set[str]) -> bool:
    market_ids = _market_ids_from_event(event)
    if not market_ids:
        return False
    return bool(market_ids & registry_ids)


def _fetch_public_search_events(
    client: Any,
    query: str,
    *,
    limit_per_type: int = 50,
) -> list[dict[str, Any]]:
    payload = gamma_get(
        client,
        "/public-search",
        params={
            "q": query,
            "limit_per_type": limit_per_type,
            "search_tags": "true",
            "search_profiles": "false",
            "events_status": "active",
        },
    )
    if not isinstance(payload, dict):
        return []
    events = payload.get("events") or []
    return [e for e in events if isinstance(e, dict)]


def _collect_keyset_events(
    client: Any,
    tag_slugs: list[str],
    *,
    keyset_closed: bool | None,
    keyset_volume_min: float | None,
    keyset_related_tags: bool,
    max_pages: int | None,
    cfg,
    scope_tag_slugs: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    for tag_slug in tag_slugs:
        for events, _meta in iter_gamma_events_keyset(
            client,
            max_pages=max_pages,
            keyset_closed=keyset_closed,
            keyset_tag_slug=tag_slug,
            keyset_related_tags=keyset_related_tags,
            keyset_volume_min=keyset_volume_min,
        ):
            if not events:
                break
            for event in events:
                if not isinstance(event, dict):
                    continue
                if not event_in_scope(
                    event,
                    config=cfg,
                    keyset_tag_slug=tag_slug,
                    keyset_related_tags=keyset_related_tags,
                    scope_tag_slugs=scope_tag_slugs,
                ):
                    continue
                event_id = str(event.get("id") or "").strip()
                if event_id:
                    by_id[event_id] = event
    return by_id


def _registry_market_id_set() -> set[str]:
    return set(get_registry_market_ids())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fail-on-gaps",
        action="store_true",
        help="Exit 1 when tag/search events have no registry market overlap.",
    )
    parser.add_argument(
        "--no-tag-discovery",
        action="store_true",
        help="Use configured event_tags only (skip runtime /tags and /sports discovery).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optional cap on keyset pages per tag (smoke tests).",
    )
    parser.add_argument(
        "--search-limit",
        type=int,
        default=50,
        help="public-search limit_per_type per query (default 50).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    ensure_duck_db()
    cfg = load_wc2026_config()
    client = build_client()
    tag_discovery = POLYMARKET_WC2026_TAG_DISCOVERY and not args.no_tag_discovery
    tag_slugs = resolve_keyset_tag_slugs(
        None,
        config=cfg,
        client=client if tag_discovery else None,
        tag_discovery=tag_discovery,
    )
    scope_tag_slugs = tuple(tag_slugs)

    if tag_discovery:
        discovered = discover_wc2026_tag_slugs(client, seed_slugs=list(cfg.event_tags))
        logger.info("Discovered tag slugs: %s", discovered.tag_slugs)
        logger.info("Tag discovery sources: %s", discovered.sources)

    keyset_events = _collect_keyset_events(
        client,
        tag_slugs,
        keyset_closed=POLYMARKET_WC2026_KEYSET_CLOSED,
        keyset_volume_min=POLYMARKET_WC2026_KEYSET_VOLUME_MIN,
        keyset_related_tags=POLYMARKET_WC2026_KEYSET_RELATED_TAGS,
        max_pages=args.max_pages,
        cfg=cfg,
        scope_tag_slugs=scope_tag_slugs,
    )

    search_events: dict[str, dict[str, Any]] = {}
    for query in _PUBLIC_SEARCH_QUERIES:
        for event in _fetch_public_search_events(
            client, query, limit_per_type=args.search_limit
        ):
            event_id = str(event.get("id") or "").strip()
            if event_id:
                search_events[event_id] = event

    registry_ids = _registry_market_id_set()
    all_candidates = {**keyset_events, **search_events}

    gaps: list[tuple[str, str, str]] = []
    for event_id, event in sorted(all_candidates.items()):
        if _event_is_in_registry(event, registry_ids):
            continue
        gaps.append(
            (
                event_id,
                str(event.get("slug") or ""),
                str(event.get("title") or "")[:120],
            )
        )

    in_keyset_only = set(keyset_events) - set(search_events)
    in_search_only = set(search_events) - set(keyset_events)

    print(f"Registry market rows: {registry_market_count()}")
    print(f"Keyset tag slugs: {tag_slugs}")
    print(f"Keyset in-scope events: {len(keyset_events)}")
    print(f"Public-search events (union): {len(search_events)}")
    print(f"Union candidate events: {len(all_candidates)}")
    print(f"Keyset-only event ids: {len(in_keyset_only)}")
    print(f"Search-only event ids: {len(in_search_only)}")
    print(f"Candidate events missing from registry: {len(gaps)}")

    for event_id, slug, title in gaps[:50]:
        print(f"  GAP {event_id} slug={slug!r} title={title!r}")
    if len(gaps) > 50:
        print(f"  ... and {len(gaps) - 50} more")

    with get_connection() as conn:
        tab = polymarket_ops_tbl("wc2026_market_registry")
        event_ids_in_registry = {
            str(r[0])
            for r in conn.execute(
                f"""
                SELECT DISTINCT event_id
                FROM {tab}
                WHERE event_id IS NOT NULL AND TRIM(event_id) != ''
                """
            ).fetchall()
        }
    event_id_gaps = sorted(set(all_candidates) - event_ids_in_registry)
    print(f"Candidate events with no registry event_id: {len(event_id_gaps)}")

    if args.fail_on_gaps and gaps:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
