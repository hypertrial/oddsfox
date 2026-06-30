"""Record extractors for Polymarket /markets backfill payloads."""

from __future__ import annotations

import json
from typing import Optional, Tuple


def _extract_tokens_record(market_id: str, market: dict) -> Optional[Tuple[str, str]]:
    clob_token_ids = market.get("clobTokenIds")
    if isinstance(clob_token_ids, str):
        try:
            clob_token_ids = json.loads(clob_token_ids)
        except json.JSONDecodeError:
            return None
    if not isinstance(clob_token_ids, list) or not clob_token_ids:
        return None
    return market_id, json.dumps(clob_token_ids)


def _extract_slug_record(market_id: str, market: dict) -> Optional[Tuple[str, str]]:
    slug = market.get("slug")
    if not slug:
        return None
    return market_id, slug


def _extract_event_slug_record(
    market_id: str, market: dict
) -> Optional[Tuple[str, str]]:
    events = market.get("events")
    if not isinstance(events, list) or not events:
        return None
    first_event = events[0]
    if not isinstance(first_event, dict):
        return None
    event_slug = first_event.get("slug")
    if not event_slug:
        return None
    return event_slug, market_id


def _extract_end_date_record(market_id: str, market: dict) -> Optional[Tuple[str, str]]:
    end_date = market.get("endDate") or market.get("endDateIso")
    if not end_date:
        return None
    return end_date, market_id
