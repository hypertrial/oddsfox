"""Polymarket market metadata backfill entrypoints."""

from __future__ import annotations

import sys
import time

from tqdm import tqdm

from oddsfox.ingestion.polymarket.gamma_events import (
    EVENTS_FETCH_LIMIT,
    EVENTS_KEYSET_REQUEST_LIMIT,
    GAMMA_EVENTS_KEYSET_EFFECTIVE_PAGE_SIZE,
)
from oddsfox.resources.http import APIClient
from oddsfox.storage.duckdb.connection import ensure_duck_db
from oddsfox.storage.duckdb.markets import (
    get_markets_missing_any_metadata,
    get_markets_without_end_date,
    get_markets_without_event_slugs,
    get_markets_without_slugs,
    get_markets_without_tokens,
    mark_market_metadata_unresolved,
    save_end_dates_batch,
    save_event_slugs_batch,
    save_slugs_batch,
    save_tokens_batch,
)
from oddsfox.storage.duckdb.metadata import (
    get_backfill_fully_checked,
    get_backfill_progress,
    set_backfill_fully_checked,
    set_backfill_progress,
)

from ._events_fallback import _fill_from_events_endpoint
from ._extract import (
    _extract_end_date_record,
    _extract_event_slug_record,
    _extract_slug_record,
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
    _process_market_chunks,
)
from .end_dates import backfill_end_dates
from .event_slugs import backfill_event_slugs
from .metadata import backfill_market_metadata
from .slugs import backfill_slugs
from .tokens import backfill_tokens

__all__ = [
    "APIClient",
    "DEFAULT_EVENT_SLUG_FALLBACK_MAX_NO_PROGRESS_PAGES",
    "DEFAULT_EVENT_SLUG_FALLBACK_MAX_PAGES",
    "EVENTS_FETCH_LIMIT",
    "EVENTS_KEYSET_REQUEST_LIMIT",
    "GAMMA_EVENTS_KEYSET_EFFECTIVE_PAGE_SIZE",
    "ProgressCallback",
    "backfill_end_dates",
    "backfill_event_slugs",
    "backfill_market_metadata",
    "backfill_slugs",
    "backfill_tokens",
    "ensure_duck_db",
    "get_backfill_fully_checked",
    "get_backfill_progress",
    "get_markets_missing_any_metadata",
    "get_markets_without_end_date",
    "get_markets_without_event_slugs",
    "get_markets_without_slugs",
    "get_markets_without_tokens",
    "mark_market_metadata_unresolved",
    "save_end_dates_batch",
    "save_event_slugs_batch",
    "save_slugs_batch",
    "save_tokens_batch",
    "set_backfill_fully_checked",
    "set_backfill_progress",
    "sys",
    "time",
    "tqdm",
    "_chunk_market_ids",
    "_duration_since",
    "_extract_end_date_record",
    "_extract_event_slug_record",
    "_extract_slug_record",
    "_extract_tokens_record",
    "_fetch_markets_batch",
    "_fill_from_events_endpoint",
    "_gamma_client",
    "_process_market_chunks",
]
