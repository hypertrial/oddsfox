"""Unfiltered Gamma /events fallback for missing event_slug backfill."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Tuple

from oddsfox.ingestion.polymarket.gamma_events import iter_gamma_events_keyset
from oddsfox.resources.http import APIClient
from oddsfox.storage.duckdb.markets import save_event_slugs_batch

from ._gamma import (
    DEFAULT_EVENT_SLUG_FALLBACK_MAX_NO_PROGRESS_PAGES,
    ProgressCallback,
)

logger = logging.getLogger(__name__)


def _fill_from_events_endpoint(
    client: APIClient,
    remaining_ids: set[str],
    *,
    max_pages: int | None = None,
    max_pages_without_progress: int
    | None = DEFAULT_EVENT_SLUG_FALLBACK_MAX_NO_PROGRESS_PAGES,
    progress_callback: ProgressCallback = None,
    progress_every_pages: int = 25,
    progress_phase: str = "event_slug_events_fallback",
) -> Tuple[int, Dict[str, Any]]:
    """
    Fallback: fetch /events pages and map their markets to missing market_ids.

    Returns (saved_count, metadata dict with pages, truncation flag, remaining count).
    """
    meta: Dict[str, Any] = {
        "events_fallback_pages": 0,
        "events_fallback_truncated": False,
        "events_fallback_remaining_ids": 0,
    }
    if not remaining_ids:
        return 0, meta

    saved = 0
    pages_done = 0
    pages_without_progress = 0
    truncated = False
    loop_started = time.monotonic()

    def _progress_extra(_page: int, _has_cursor: bool) -> dict[str, Any]:
        return {
            "remaining_ids": len(remaining_ids),
            "saved_this_fallback": saved,
            "pages_without_progress": pages_without_progress,
            "elapsed_seconds": round(time.monotonic() - loop_started, 3),
        }

    for events, page_meta in iter_gamma_events_keyset(
        client,
        max_pages=max_pages,
        progress_callback=progress_callback,
        progress_task=progress_phase,
        progress_every_pages=progress_every_pages,
        progress_extra=_progress_extra,
    ):
        pages_done = page_meta.pages_done
        truncated = page_meta.truncated

        if not events:
            break

        batch = []
        for event in events:
            event_slug = event.get("slug")
            if not event_slug:
                continue
            for market in event.get("markets") or []:
                market_id = str(market.get("id"))
                if market_id in remaining_ids:
                    batch.append((event_slug, market_id))
                    remaining_ids.remove(market_id)

        if batch:
            save_event_slugs_batch(batch)
            saved += len(batch)
            logger.debug("Saved %s event_slugs via /events fallback batch", len(batch))
            pages_without_progress = 0
        else:
            pages_without_progress += 1

        if not remaining_ids:
            break
        if (
            max_pages_without_progress is not None
            and pages_without_progress >= max_pages_without_progress
        ):
            truncated = True
            logger.info(
                "Stopping /events fallback after %s pages without event_slug matches; %s market IDs remain unresolved",
                pages_without_progress,
                len(remaining_ids),
            )
            break

    meta["events_fallback_pages"] = pages_done
    meta["events_fallback_truncated"] = truncated
    meta["events_fallback_remaining_ids"] = len(remaining_ids)
    return saved, meta
