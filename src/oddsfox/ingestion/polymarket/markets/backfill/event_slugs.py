"""Polymarket metadata backfill: backfill_event_slugs."""

import logging
import time
from typing import Any, Dict

from oddsfox.storage.duckdb.connection import ensure_duck_db
from oddsfox.storage.duckdb.markets import (
    get_markets_without_event_slugs,
    save_event_slugs_batch,
)
from oddsfox.storage.duckdb.metadata import (
    get_backfill_fully_checked,
    get_backfill_progress,
    set_backfill_fully_checked,
    set_backfill_progress,
)

from ._events_fallback import _fill_from_events_endpoint
from ._extract import _extract_event_slug_record
from ._gamma import (
    DEFAULT_EVENT_SLUG_FALLBACK_MAX_NO_PROGRESS_PAGES,
    DEFAULT_EVENT_SLUG_FALLBACK_MAX_PAGES,
    ProgressCallback,
    _duration_since,
    _gamma_client,
    _process_market_chunks,
)

logger = logging.getLogger(__name__)


def backfill_event_slugs(
    batch_size: int = 50,
    max_markets: int = None,
    force: bool = False,
    *,
    progress_callback: ProgressCallback = None,
    progress_every_n_batches: int = 10,
    gamma_requests_per_second: float | None = None,
    event_slug_fallback_max_pages: int | None = DEFAULT_EVENT_SLUG_FALLBACK_MAX_PAGES,
    event_slug_fallback_max_pages_without_progress: int
    | None = DEFAULT_EVENT_SLUG_FALLBACK_MAX_NO_PROGRESS_PAGES,
    event_slug_fallback_progress_every_pages: int = 25,
) -> Dict[str, Any]:
    """
    Backfill event_slugs for markets that don't have them yet.
    Uses ID filtering to fetch specific markets efficiently.

    Args:
        batch_size: Number of markets to fetch per API call (recommended: 20-50)
        max_markets: Maximum number of markets to process (None = all)
        force: Run even if ledger says fully checked
    """
    t0 = time.monotonic()
    if not force and get_backfill_fully_checked("event_slugs"):
        logger.debug(
            "Event slug backfill previously marked complete. Use --force to rerun."
        )
        return {
            "task": "backfill_event_slugs",
            "skipped": True,
            "reason": "fully_checked",
            "duration_seconds": _duration_since(t0),
            "api_requests": 0,
            "events_fallback_pages": 0,
            "events_fallback_truncated": False,
            "events_fallback_remaining_ids": 0,
        }

    logger.info("Starting event_slug backfill for existing markets")

    ensure_duck_db()

    # Get markets without event_slugs (stable ORDER BY id in storage helper).
    # Do not slice by saved ledger progress: eligible rows can shrink between runs
    # (e.g. after truncation), and an index into an old list can skip unresolved IDs
    # or falsely mark the task fully_checked.
    all_market_ids = get_markets_without_event_slugs(limit=max_markets)
    total_markets = len(all_market_ids)

    ledger_progress_at_start = 0
    if max_markets is None:
        saved = get_backfill_progress("event_slugs")
        if saved < 0:
            saved = 0
        # force=True always reprocesses the current unresolved set; do not surface stale
        # ledger offsets in metadata (work selection never uses them).
        ledger_progress_at_start = 0 if force else saved

    market_ids = all_market_ids
    remaining_ids = set(str(mid) for mid in market_ids)

    if total_markets == 0:
        logger.debug("All markets already have event_slugs. Nothing to backfill.")
        if max_markets is None:
            set_backfill_progress("event_slugs", 0)
            set_backfill_fully_checked("event_slugs", True)
        return {
            "task": "backfill_event_slugs",
            "skipped": False,
            "eligible": 0,
            "processed": 0,
            "saved": 0,
            "progress_start": ledger_progress_at_start,
            "fully_checked_set": max_markets is None,
            "duration_seconds": _duration_since(t0),
            "api_requests": 0,
            "events_fallback_pages": 0,
            "events_fallback_truncated": False,
            "events_fallback_remaining_ids": 0,
        }

    logger.info(
        f"Found {total_markets} markets without event_slugs. Starting backfill..."
    )
    if progress_callback:
        progress_callback(
            "backfill_event_slugs",
            {
                "stage": "start",
                "eligible": total_markets,
                "progress_start": ledger_progress_at_start,
                "progress_note": "ledger_only_not_used_for_resume",
                "batch_size": batch_size,
            },
        )

    client = _gamma_client(gamma_requests_per_second)

    processed = 0
    saved = 0
    api_requests = 0
    completed_all = False
    events_fb_meta: Dict[str, Any] = {
        "events_fallback_pages": 0,
        "events_fallback_truncated": False,
        "events_fallback_remaining_ids": 0,
    }

    try:
        processed, saved, api_requests = _process_market_chunks(
            client=client,
            market_ids=market_ids,
            batch_size=batch_size,
            desc="Backfilling event_slugs",
            include_events=True,
            extract_record=_extract_event_slug_record,
            save_batch=save_event_slugs_batch,
            on_record_saved=remaining_ids.discard,
            processed_start=0,
            progress_phase="backfill_event_slugs_markets",
            progress_callback=progress_callback,
            progress_every_n_batches=progress_every_n_batches,
        )
    finally:
        if remaining_ids:
            logger.info(
                "Falling back to /events endpoint for %s markets without event_slugs",
                len(remaining_ids),
            )
            if progress_callback:
                progress_callback(
                    "backfill_event_slugs",
                    {
                        "stage": "events_fallback_start",
                        "remaining_before_fallback": len(remaining_ids),
                        "max_pages": event_slug_fallback_max_pages,
                        "max_pages_without_progress": event_slug_fallback_max_pages_without_progress,
                    },
                )
            extra_saved, events_fb_meta = _fill_from_events_endpoint(
                client,
                remaining_ids,
                max_pages=event_slug_fallback_max_pages,
                max_pages_without_progress=event_slug_fallback_max_pages_without_progress,
                progress_callback=progress_callback,
                progress_every_pages=event_slug_fallback_progress_every_pages,
            )
            saved += extra_saved
            api_requests += int(events_fb_meta.get("events_fallback_pages", 0))

        markets_phase_done = processed >= total_markets
        fallback_done = len(remaining_ids) == 0
        completed_all = markets_phase_done and fallback_done
        truncated = bool(events_fb_meta.get("events_fallback_truncated"))
        remaining_fb = int(events_fb_meta.get("events_fallback_remaining_ids", 0))

        logger.info(
            "Backfill complete. Processed %s markets, saved event_slugs for %s markets.",
            processed,
            saved,
        )
        if max_markets is None:
            if completed_all and not truncated and remaining_fb == 0:
                still_missing = get_markets_without_event_slugs(limit=1)
                if not still_missing:
                    set_backfill_progress("event_slugs", 0)
                    set_backfill_fully_checked("event_slugs", True)
                else:
                    logger.warning(
                        "event_slug backfill reported no remaining in-memory IDs but "
                        "DuckDB still has markets without event_slug; not marking fully_checked."
                    )
                    set_backfill_progress("event_slugs", processed)
                    set_backfill_fully_checked("event_slugs", False)
            else:
                set_backfill_progress("event_slugs", processed)
                set_backfill_fully_checked("event_slugs", False)
        else:
            set_backfill_fully_checked("event_slugs", False)

    fc = get_backfill_fully_checked("event_slugs")
    if progress_callback:
        progress_callback(
            "backfill_event_slugs",
            {
                "stage": "complete",
                "eligible": total_markets,
                "processed": processed,
                "saved": saved,
                "api_requests": api_requests,
                "duration_seconds": _duration_since(t0),
                **events_fb_meta,
            },
        )

    return {
        "task": "backfill_event_slugs",
        "skipped": False,
        "eligible": total_markets,
        "processed": processed,
        "saved": saved,
        "progress_start": ledger_progress_at_start,
        "fully_checked_set": bool(fc),
        "duration_seconds": _duration_since(t0),
        "api_requests": api_requests,
        "events_fallback_pages": events_fb_meta.get("events_fallback_pages", 0),
        "events_fallback_truncated": events_fb_meta.get(
            "events_fallback_truncated", False
        ),
        "events_fallback_remaining_ids": events_fb_meta.get(
            "events_fallback_remaining_ids", 0
        ),
    }
