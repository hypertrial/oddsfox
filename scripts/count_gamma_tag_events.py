#!/usr/bin/env python3
"""
Count Polymarket Gamma events for WC2026 scope tags via GET /events/keyset.

Defaults match routine ingestion (``MarketsSyncConfig``):
  - all scope tags from ``wc2026_events.yml`` / ``POLYMARKET_WC2026_EVENT_TAGS``
  - default ``closed=false`` and ``volume_min=10000`` (omit via env ``any`` / empty volume)
  - ``related_tags=true`` when ``POLYMARKET_WC2026_KEYSET_RELATED_TAGS`` is on

Logs progress every N events (default 1000) per tag and prints per-tag totals.

Usage:
  python3 scripts/count_gamma_tag_events.py
  python3 scripts/count_gamma_tag_events.py --tag 2026-fifa-world-cup
  python3 scripts/count_gamma_tag_events.py --tag fifa-world-cup --tag 2026-fifa-world-cup
  python3 scripts/count_gamma_tag_events.py --keyset-closed any
  python3 scripts/count_gamma_tag_events.py --keyset-volume-min 0
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
for _rp in (REPO_ROOT, REPO_ROOT / "src"):
    if str(_rp) not in sys.path:
        sys.path.insert(0, str(_rp))

from oddsfox.config.settings import (  # noqa: E402
    POLYMARKET_WC2026_KEYSET_CLOSED,
    POLYMARKET_WC2026_KEYSET_VOLUME_MIN,
)
from oddsfox.ingestion.polymarket.gamma_events import (  # noqa: E402
    iter_gamma_events_keyset,
)
from oddsfox.ingestion.polymarket.markets.fetch import (  # noqa: E402
    build_client,
)
from oddsfox.ingestion.polymarket.wc2026_scope import (  # noqa: E402
    load_wc2026_config,
    resolve_keyset_tag_slugs,
)

logger = logging.getLogger(__name__)


def _parse_keyset_closed(value: str) -> bool | None:
    normalized = value.strip().lower()
    if normalized in {"false", "0", "open"}:
        return False
    if normalized in {"true", "1", "closed"}:
        return True
    if normalized in {"any", "all", "none", "null"}:
        return None
    raise argparse.ArgumentTypeError(
        "keyset-closed must be one of: false (open only), true (closed only), any"
    )


def count_tag_events(
    tag_slug: str,
    *,
    keyset_closed: bool | None = POLYMARKET_WC2026_KEYSET_CLOSED,
    keyset_volume_min: float | None = POLYMARKET_WC2026_KEYSET_VOLUME_MIN,
    log_every: int = 1000,
    max_pages: int | None = None,
) -> int:
    client = build_client()
    total = 0
    pages = 0
    truncated = False
    next_log_at = log_every

    logger.info(
        "start tag=%s keyset_closed=%s keyset_volume_min=%s",
        tag_slug,
        keyset_closed,
        keyset_volume_min,
    )

    for events, meta in iter_gamma_events_keyset(
        client,
        max_pages=max_pages,
        keyset_tag_slug=tag_slug,
        keyset_closed=keyset_closed,
        keyset_volume_min=keyset_volume_min,
    ):
        pages = meta.pages_done
        truncated = meta.truncated
        if not events:
            break
        total += len(events)
        while total >= next_log_at:
            logger.info(
                "tag=%s events=%s pages=%s truncated=%s keyset_closed=%s keyset_volume_min=%s",
                tag_slug,
                total,
                pages,
                truncated,
                keyset_closed,
                keyset_volume_min,
            )
            next_log_at += log_every

    logger.info(
        "tag=%s total_events=%s total_pages=%s truncated=%s keyset_closed=%s keyset_volume_min=%s",
        tag_slug,
        total,
        pages,
        truncated,
        keyset_closed,
        keyset_volume_min,
    )
    return total


def count_scope_tags(
    tag_slugs: list[str],
    *,
    keyset_closed: bool | None = POLYMARKET_WC2026_KEYSET_CLOSED,
    keyset_volume_min: float | None = POLYMARKET_WC2026_KEYSET_VOLUME_MIN,
    log_every: int = 1000,
    max_pages: int | None = None,
) -> dict[str, int]:
    totals: dict[str, int] = {}
    for tag_slug in tag_slugs:
        totals[tag_slug] = count_tag_events(
            tag_slug,
            keyset_closed=keyset_closed,
            keyset_volume_min=keyset_volume_min,
            log_every=log_every,
            max_pages=max_pages,
        )
    logger.info(
        "summary tags=%s grand_total=%s keyset_closed=%s keyset_volume_min=%s",
        tag_slugs,
        sum(totals.values()),
        keyset_closed,
        keyset_volume_min,
    )
    return totals


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Count Gamma /events/keyset rows for WC2026 scope tag_slug values."
    )
    parser.add_argument(
        "--tag",
        action="append",
        dest="tags",
        help="Gamma tag_slug (repeatable). Default: all scope event_tags.",
    )
    parser.add_argument(
        "--keyset-closed",
        type=_parse_keyset_closed,
        default=POLYMARKET_WC2026_KEYSET_CLOSED,
        help=("closed filter: false=open only (default), true=closed only, any=both"),
    )
    parser.add_argument(
        "--keyset-volume-min",
        type=float,
        default=POLYMARKET_WC2026_KEYSET_VOLUME_MIN,
        help=(
            "Gamma volume_min query filter "
            f"(default: {POLYMARKET_WC2026_KEYSET_VOLUME_MIN:g})"
        ),
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=1000,
        help="Log after this many events per tag (default: 1000)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optional cap on API pages per tag (for smoke tests)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.log_every <= 0:
        parser.error("--log-every must be positive")
    if args.keyset_volume_min is not None and args.keyset_volume_min < 0:
        parser.error("--keyset-volume-min must be >= 0")

    cfg = load_wc2026_config()
    tag_slugs = resolve_keyset_tag_slugs(args.tags, config=cfg)
    if not tag_slugs:
        parser.error("no tag_slug values resolved; pass --tag or configure event_tags")

    count_scope_tags(
        tag_slugs,
        keyset_closed=args.keyset_closed,
        keyset_volume_min=args.keyset_volume_min,
        log_every=args.log_every,
        max_pages=args.max_pages,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
