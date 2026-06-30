"""Polymarket metadata backfill: backfill_end_dates."""

import logging
import time
from typing import Any, Dict

from oddsfox.storage.duckdb.connection import ensure_duck_db
from oddsfox.storage.duckdb.markets import (
    get_markets_without_end_date,
    save_end_dates_batch,
)
from oddsfox.storage.duckdb.metadata import (
    get_backfill_fully_checked,
    set_backfill_fully_checked,
)

from ._extract import _extract_end_date_record
from ._gamma import (
    ProgressCallback,
    _duration_since,
    _gamma_client,
    _process_market_chunks,
)

logger = logging.getLogger(__name__)


def backfill_end_dates(
    batch_size: int = 50,
    max_markets: int = None,
    force: bool = False,
    *,
    progress_callback: ProgressCallback = None,
    progress_every_n_batches: int = 10,
    gamma_requests_per_second: float | None = None,
) -> Dict[str, Any]:
    """
    Backfill end_date for markets missing it using the Gamma API.
    """
    t0 = time.monotonic()
    if not force and get_backfill_fully_checked("end_dates"):
        logger.debug(
            "end_date backfill previously marked complete. Use --force to rerun."
        )
        return {
            "task": "backfill_end_dates",
            "skipped": True,
            "reason": "fully_checked",
            "duration_seconds": _duration_since(t0),
            "api_requests": 0,
        }

    logger.info("Starting end_date backfill for existing markets")
    ensure_duck_db()

    market_ids = get_markets_without_end_date(limit=max_markets)
    total_markets = len(market_ids)

    if total_markets == 0:
        logger.debug("All markets already have end_date. Nothing to backfill.")
        if max_markets is None:
            set_backfill_fully_checked("end_dates", True)
        return {
            "task": "backfill_end_dates",
            "skipped": False,
            "eligible": 0,
            "processed": 0,
            "saved": 0,
            "fully_checked_set": max_markets is None,
            "duration_seconds": _duration_since(t0),
            "api_requests": 0,
        }

    logger.info(f"Found {total_markets} markets without end_date. Starting backfill...")
    if progress_callback:
        progress_callback(
            "backfill_end_dates",
            {"stage": "start", "eligible": total_markets, "batch_size": batch_size},
        )

    client = _gamma_client(gamma_requests_per_second)
    processed = 0
    saved = 0
    completed_all = False

    processed, saved, api_requests = _process_market_chunks(
        client=client,
        market_ids=market_ids,
        batch_size=batch_size,
        desc="Backfilling end_date",
        include_events=False,
        extract_record=_extract_end_date_record,
        save_batch=save_end_dates_batch,
        count_errors_as_processed=True,
        progress_phase="backfill_end_dates",
        progress_callback=progress_callback,
        progress_every_n_batches=progress_every_n_batches,
    )

    completed_all = processed >= total_markets
    logger.info(
        f"Backfill complete. Processed {processed} markets, saved end_date for {saved} markets."
    )
    if max_markets is None:
        set_backfill_fully_checked("end_dates", completed_all)
    else:
        set_backfill_fully_checked("end_dates", False)

    if progress_callback:
        progress_callback(
            "backfill_end_dates",
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
        "task": "backfill_end_dates",
        "skipped": False,
        "eligible": total_markets,
        "processed": processed,
        "saved": saved,
        "fully_checked_set": completed_all if max_markets is None else False,
        "duration_seconds": _duration_since(t0),
        "api_requests": api_requests,
    }
