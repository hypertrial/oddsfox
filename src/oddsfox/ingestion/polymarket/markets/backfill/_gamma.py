"""Gamma /markets chunk helpers for Polymarket backfill."""

from __future__ import annotations

import logging
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from tqdm import tqdm

from oddsfox.config.settings import GAMMA_API_URL
from oddsfox.ingestion.polymarket.errors import GammaRequestError, gamma_get
from oddsfox.resources.http import APIClient

logger = logging.getLogger(__name__)
DEFAULT_EVENT_SLUG_FALLBACK_MAX_PAGES = 20_000
DEFAULT_EVENT_SLUG_FALLBACK_MAX_NO_PROGRESS_PAGES = 25

ProgressCallback = Optional[Callable[[str, Dict[str, Any]], None]]


def _gamma_client(requests_per_second: Optional[float] = None) -> APIClient:
    return APIClient(
        base_url=GAMMA_API_URL,
        requests_per_second=requests_per_second,
    )


def _duration_since(start: float) -> float:
    return round(time.monotonic() - start, 3)


def _chunk_market_ids(market_ids: Sequence[str], chunk_size: int) -> List[List[str]]:
    return [
        market_ids[i : i + chunk_size] for i in range(0, len(market_ids), chunk_size)
    ]


def _fetch_markets_batch(
    client: APIClient, chunk: Sequence[str], include_events: bool = False
) -> list:
    params = {"id": list(chunk)}
    if include_events:
        params["includeEvents"] = "true"
    return gamma_get(client, "/markets", params=params)


def _process_market_chunks(
    *,
    client: APIClient,
    market_ids: Sequence[str],
    batch_size: int,
    desc: str,
    include_events: bool,
    extract_record: Callable[[str, dict], Optional[Tuple]],
    save_batch: Callable[[List[Tuple]], None],
    on_record_saved: Optional[Callable[[str], None]] = None,
    processed_start: int = 0,
    count_errors_as_processed: bool = False,
    progress_phase: str = "markets_chunks",
    progress_callback: ProgressCallback = None,
    progress_every_n_batches: int = 10,
) -> Tuple[int, int, int]:
    """Fetch /markets in chunks and persist extracted records.

    Returns (processed, saved, api_requests).
    """
    processed = processed_start
    saved = 0
    api_requests = 0
    chunks = _chunk_market_ids(market_ids, batch_size)
    total_batches = len(chunks)
    loop_started = time.monotonic()

    logger.debug(f"Processing {total_batches} batches of {batch_size} markets each...")
    use_tqdm = sys.stderr.isatty()
    with tqdm(total=len(market_ids), desc=desc, disable=not use_tqdm) as pbar:
        for batch_idx, chunk in enumerate(chunks, start=1):
            try:
                api_requests += 1
                markets = _fetch_markets_batch(
                    client, chunk, include_events=include_events
                )
                if not markets:
                    processed += len(chunk)
                    pbar.update(len(chunk))
                else:
                    returned_map = {str(m.get("id")): m for m in markets}
                    save_records: List[Tuple] = []

                    for market_id in chunk:
                        market = returned_map.get(str(market_id))
                        if market:
                            record = extract_record(market_id, market)
                            if record is not None:
                                save_records.append(record)
                                saved += 1
                                if on_record_saved:
                                    on_record_saved(str(market_id))

                        processed += 1
                        pbar.update(1)

                    if save_records:
                        save_batch(save_records)

            except (GammaRequestError, OSError) as exc:
                logger.error(f"Error during backfill batch: {exc}")
                if count_errors_as_processed:
                    processed += len(chunk)
                pbar.update(len(chunk))
            except Exception as exc:
                logger.error(f"Error during backfill batch: {exc}")
                if count_errors_as_processed:
                    processed += len(chunk)
                pbar.update(len(chunk))

            if progress_callback and (
                batch_idx % progress_every_n_batches == 0 or batch_idx == total_batches
            ):
                progress_callback(
                    progress_phase,
                    {
                        "batch_index": batch_idx,
                        "batches_total": total_batches,
                        "processed": processed,
                        "saved": saved,
                        "api_requests": api_requests,
                        "elapsed_seconds": round(time.monotonic() - loop_started, 3),
                    },
                )

    return processed, saved, api_requests
