"""Polymarket metadata backfill: backfill_slugs."""

import logging
import time
from typing import Any, Dict

from oddsfox.storage.duckdb.connection import ensure_duck_db
from oddsfox.storage.duckdb.markets import (
    get_markets_without_slugs,
    save_slugs_batch,
)
from oddsfox.storage.duckdb.metadata import (
    get_backfill_fully_checked,
    set_backfill_fully_checked,
)

from ._extract import _extract_slug_record
from ._gamma import (
    ProgressCallback,
    _duration_since,
    _gamma_client,
    _process_market_chunks,
)

logger = logging.getLogger(__name__)


def backfill_slugs(
    batch_size: int = 50,
    max_markets: int = None,
    force: bool = False,
    *,
    progress_callback: ProgressCallback = None,
    progress_every_n_batches: int = 10,
    gamma_requests_per_second: float | None = None,
) -> Dict[str, Any]:
    """
    Backfill slugs for markets that don't have them yet.
    Uses ID filtering to fetch specific markets efficiently.

    Args:
        batch_size: Number of markets to fetch per API call (recommended: 20-50)
        max_markets: Maximum number of markets to process (None = all)
        force: Run even if ledger says fully checked
    """
    t0 = time.monotonic()
    if not force and get_backfill_fully_checked("slugs"):
        logger.debug("Slug backfill previously marked complete. Use --force to rerun.")
        return {
            "task": "backfill_slugs",
            "skipped": True,
            "reason": "fully_checked",
            "duration_seconds": _duration_since(t0),
            "api_requests": 0,
        }

    logger.info("Starting slug backfill for existing markets")

    ensure_duck_db()

    # Get markets without slugs
    market_ids = get_markets_without_slugs(limit=max_markets)
    total_markets = len(market_ids)

    if total_markets == 0:
        logger.debug("All markets already have slugs. Nothing to backfill.")
        if max_markets is None:
            set_backfill_fully_checked("slugs", True)
        return {
            "task": "backfill_slugs",
            "skipped": False,
            "eligible": 0,
            "processed": 0,
            "saved": 0,
            "fully_checked_set": max_markets is None,
            "duration_seconds": _duration_since(t0),
            "api_requests": 0,
        }

    logger.info(f"Found {total_markets} markets without slugs. Starting backfill...")
    if progress_callback:
        progress_callback(
            "backfill_slugs",
            {"stage": "start", "eligible": total_markets, "batch_size": batch_size},
        )

    client = _gamma_client(gamma_requests_per_second)

    processed = 0
    saved = 0
    api_requests = 0
    completed_all = False

    try:
        processed, saved, api_requests = _process_market_chunks(
            client=client,
            market_ids=market_ids,
            batch_size=batch_size,
            desc="Backfilling slugs",
            include_events=True,
            extract_record=_extract_slug_record,
            save_batch=save_slugs_batch,
            progress_phase="backfill_slugs",
            progress_callback=progress_callback,
            progress_every_n_batches=progress_every_n_batches,
        )
    finally:
        completed_all = processed >= total_markets
        logger.info(
            f"Backfill complete. Processed {processed} markets, saved slugs for {saved} markets."
        )
        mark_complete = max_markets is None and completed_all
        set_backfill_fully_checked("slugs", mark_complete)

    if progress_callback:
        progress_callback(
            "backfill_slugs",
            {
                "stage": "complete",
                "eligible": total_markets,
                "processed": processed,
                "saved": saved,
                "api_requests": api_requests,
                "duration_seconds": _duration_since(t0),
            },
        )

    return {
        "task": "backfill_slugs",
        "skipped": False,
        "eligible": total_markets,
        "processed": processed,
        "saved": saved,
        "fully_checked_set": mark_complete,
        "duration_seconds": _duration_since(t0),
        "api_requests": api_requests,
    }
