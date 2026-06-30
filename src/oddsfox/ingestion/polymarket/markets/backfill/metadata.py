"""Polymarket metadata backfill: backfill_market_metadata."""

import logging
import sys
import time
from typing import Any, Dict

from tqdm import tqdm

from oddsfox.ingestion.polymarket.errors import GammaRequestError
from oddsfox.storage.duckdb.connection import ensure_duck_db
from oddsfox.storage.duckdb.markets import (
    get_markets_missing_any_metadata,
    mark_market_metadata_unresolved,
    save_end_dates_batch,
    save_event_slugs_batch,
    save_slugs_batch,
    save_tokens_batch,
)
from oddsfox.storage.duckdb.metadata import (
    get_backfill_fully_checked,
    set_backfill_fully_checked,
)

from ._events_fallback import _fill_from_events_endpoint
from ._extract import (
    _extract_end_date_record,
    _extract_event_slug_record,
    _extract_tokens_record,
)
from ._gamma import (
    DEFAULT_EVENT_SLUG_FALLBACK_MAX_NO_PROGRESS_PAGES,
    DEFAULT_EVENT_SLUG_FALLBACK_MAX_PAGES,
    ProgressCallback,
    _chunk_market_ids,
    _duration_since,
    _fetch_markets_batch,
    _gamma_client,
)

logger = logging.getLogger(__name__)


def backfill_market_metadata(
    batch_size: int = 50,
    max_markets: int = None,
    force: bool = False,
    *,
    include_tokens: bool = True,
    include_slugs: bool = True,
    include_event_slugs: bool = True,
    include_end_dates: bool = True,
    progress_callback: ProgressCallback = None,
    progress_every_n_batches: int = 10,
    gamma_requests_per_second: float | None = None,
    market_scope: str = "all",
    event_slug_fallback_max_pages: int | None = DEFAULT_EVENT_SLUG_FALLBACK_MAX_PAGES,
    event_slug_fallback_max_pages_without_progress: int
    | None = DEFAULT_EVENT_SLUG_FALLBACK_MAX_NO_PROGRESS_PAGES,
    event_slug_fallback_progress_every_pages: int = 25,
    event_slug_unresolved_retry_hours: int = 168,
) -> Dict[str, Any]:
    """Backfill requested market metadata with a single Gamma pass per market."""
    t0 = time.monotonic()
    if not any([include_tokens, include_slugs, include_event_slugs, include_end_dates]):
        return {
            "task": "backfill_market_metadata",
            "skipped": True,
            "reason": "no_fields_enabled",
            "duration_seconds": _duration_since(t0),
            "api_requests": 0,
        }

    ledger_keys = []
    if include_tokens:
        ledger_keys.append("tokens")
    if include_slugs:
        ledger_keys.append("slugs")
    if include_event_slugs:
        ledger_keys.append("event_slugs")
    if include_end_dates:
        ledger_keys.append("end_dates")
    if not force and all(get_backfill_fully_checked(k) for k in ledger_keys):
        return {
            "task": "backfill_market_metadata",
            "skipped": True,
            "reason": "fully_checked",
            "duration_seconds": _duration_since(t0),
            "api_requests": 0,
        }

    ensure_duck_db()
    market_ids = get_markets_missing_any_metadata(
        include_tokens=include_tokens,
        include_slugs=include_slugs,
        include_event_slugs=include_event_slugs,
        include_end_dates=include_end_dates,
        limit=max_markets,
        market_scope=market_scope,
    )
    total_markets = len(market_ids)
    if total_markets == 0:
        for key in ledger_keys:
            if max_markets is None:
                set_backfill_fully_checked(key, True)
        return {
            "task": "backfill_market_metadata",
            "skipped": False,
            "eligible": 0,
            "processed": 0,
            "saved": {
                "tokens": 0,
                "slugs": 0,
                "event_slugs": 0,
                "end_dates": 0,
            },
            "fully_checked_set": max_markets is None,
            "duration_seconds": _duration_since(t0),
            "api_requests": 0,
        }

    if progress_callback:
        progress_callback(
            "backfill_market_metadata",
            {
                "stage": "start",
                "eligible": total_markets,
                "batch_size": batch_size,
                "market_scope": market_scope,
            },
        )

    client = _gamma_client(gamma_requests_per_second)
    processed = 0
    api_requests = 0
    saved = {"tokens": 0, "slugs": 0, "event_slugs": 0, "end_dates": 0}
    unresolved_event_slugs: list[tuple[str, str, str]] = []
    remaining_event_slug_ids: set[str] = set()
    events_fb_meta: dict[str, Any] = {
        "events_fallback_pages": 0,
        "events_fallback_truncated": False,
        "events_fallback_remaining_ids": 0,
    }
    chunks = _chunk_market_ids(market_ids, batch_size)
    total_batches = len(chunks)
    loop_started = time.monotonic()
    use_tqdm = sys.stderr.isatty()

    with tqdm(
        total=total_markets, desc="Backfilling market metadata", disable=not use_tqdm
    ) as pbar:
        for batch_idx, chunk in enumerate(chunks, start=1):
            token_rows: list[tuple[str, str]] = []
            slug_rows: list[tuple[str, str]] = []
            event_slug_rows: list[tuple[str, str]] = []
            end_date_rows: list[tuple[str, str]] = []
            try:
                api_requests += 1
                markets = _fetch_markets_batch(client, chunk, include_events=True)
                returned_map = {str(m.get("id")): m for m in markets or []}
                for market_id in chunk:
                    market = returned_map.get(str(market_id))
                    if not market:
                        processed += 1
                        pbar.update(1)
                        continue
                    if include_tokens:
                        record = _extract_tokens_record(market_id, market)
                        if record is not None:
                            token_rows.append(record)
                    if include_slugs:
                        slug = market.get("slug")
                        if slug:
                            slug_rows.append((slug, market_id))
                    if include_event_slugs:
                        record = _extract_event_slug_record(market_id, market)
                        if record is not None:
                            event_slug_rows.append(record)
                        else:
                            remaining_event_slug_ids.add(str(market_id))
                    if include_end_dates:
                        record = _extract_end_date_record(market_id, market)
                        if record is not None:
                            end_date_rows.append(record)
                    processed += 1
                    pbar.update(1)
                if token_rows:
                    save_tokens_batch(token_rows)
                    saved["tokens"] += len(token_rows)
                if slug_rows:
                    save_slugs_batch(slug_rows)
                    saved["slugs"] += len(slug_rows)
                if event_slug_rows:
                    save_event_slugs_batch(event_slug_rows)
                    saved["event_slugs"] += len(event_slug_rows)
                if end_date_rows:
                    save_end_dates_batch(end_date_rows)
                    saved["end_dates"] += len(end_date_rows)
            except (GammaRequestError, OSError) as exc:
                logger.error("Error during combined metadata backfill batch: %s", exc)
                pbar.update(len(chunk))
            except Exception as exc:
                logger.error("Error during combined metadata backfill batch: %s", exc)
                pbar.update(len(chunk))

            if progress_callback and (
                batch_idx % progress_every_n_batches == 0 or batch_idx == total_batches
            ):
                progress_callback(
                    "backfill_market_metadata",
                    {
                        "batch_index": batch_idx,
                        "batches_total": total_batches,
                        "processed": processed,
                        "saved": dict(saved),
                        "api_requests": api_requests,
                        "elapsed_seconds": round(time.monotonic() - loop_started, 3),
                    },
                )

    if include_event_slugs and remaining_event_slug_ids:
        if progress_callback:
            progress_callback(
                "backfill_market_metadata",
                {
                    "stage": "events_fallback_start",
                    "remaining_before_fallback": len(remaining_event_slug_ids),
                    "max_pages": event_slug_fallback_max_pages,
                    "max_pages_without_progress": event_slug_fallback_max_pages_without_progress,
                },
            )
        extra_saved, events_fb_meta = _fill_from_events_endpoint(
            client,
            remaining_event_slug_ids,
            max_pages=event_slug_fallback_max_pages,
            max_pages_without_progress=event_slug_fallback_max_pages_without_progress,
            progress_callback=progress_callback,
            progress_every_pages=event_slug_fallback_progress_every_pages,
            progress_phase="backfill_market_metadata_events_fallback",
        )
        saved["event_slugs"] += extra_saved
        api_requests += int(events_fb_meta.get("events_fallback_pages", 0) or 0)
        unresolved_event_slugs = [
            (market_id, "event_slug", "missing from Gamma market and events payload")
            for market_id in remaining_event_slug_ids
        ]

    if unresolved_event_slugs:
        mark_market_metadata_unresolved(
            unresolved_event_slugs,
            retry_after_hours=event_slug_unresolved_retry_hours,
        )

    completed_all = processed >= total_markets
    if max_markets is None:
        for key in ledger_keys:
            set_backfill_fully_checked(key, completed_all)
    else:
        for key in ledger_keys:
            set_backfill_fully_checked(key, False)

    if progress_callback:
        progress_callback(
            "backfill_market_metadata",
            {
                "stage": "complete",
                "eligible": total_markets,
                "processed": processed,
                "saved": dict(saved),
                "unresolved_event_slugs": len(unresolved_event_slugs),
                "api_requests": api_requests,
                "duration_seconds": _duration_since(t0),
                **events_fb_meta,
            },
        )

    return {
        "task": "backfill_market_metadata",
        "skipped": False,
        "eligible": total_markets,
        "processed": processed,
        "saved": dict(saved),
        "unresolved_event_slugs": len(unresolved_event_slugs),
        **events_fb_meta,
        "fully_checked_set": max_markets is None and completed_all,
        "duration_seconds": _duration_since(t0),
        "api_requests": api_requests,
    }
