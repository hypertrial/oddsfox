"""Shared Gamma /events/keyset pagination (no business filters).

Full keyset crawls pass ``limit=EVENTS_KEYSET_REQUEST_LIMIT`` (500) on each
``GET /events/keyset`` call. As of 2026-05, Gamma returns at most about
``GAMMA_EVENTS_KEYSET_EFFECTIVE_PAGE_SIZE`` events per response regardless of
that request parameter—plan runtime and ``max_event_pages`` caps using API pages,
not requested batch size.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Iterator

import requests

from oddsfox.ingestion.polymarket.errors import GammaRequestError, gamma_get

logger = logging.getLogger(__name__)

# Requested ``limit`` query param for /events/keyset (Gamma may return fewer).
EVENTS_KEYSET_REQUEST_LIMIT = 500
# Observed Gamma cap per keyset response (for docs and runtime estimates only).
GAMMA_EVENTS_KEYSET_EFFECTIVE_PAGE_SIZE = 100


@dataclass(frozen=True)
class EventsPageMeta:
    pages_done: int
    truncated: bool


def _event_id(event: dict[str, Any]) -> str | None:
    raw = event.get("id")
    if raw is None:
        return None
    return str(raw)


def _remember_event_ids(events: list[dict[str, Any]], seen_event_ids: set[str]) -> None:
    for event in events:
        event_id = _event_id(event)
        if event_id is not None:
            seen_event_ids.add(event_id)


def _unseen_events(
    events: list[dict[str, Any]], seen_event_ids: set[str]
) -> list[dict[str, Any]]:
    unseen: list[dict[str, Any]] = []
    for event in events:
        event_id = _event_id(event)
        if event_id is None or event_id not in seen_event_ids:
            unseen.append(event)
    return unseen


def iter_gamma_events_keyset(
    client: Any,
    *,
    max_pages: int | None,
    fetch_limit: int = EVENTS_KEYSET_REQUEST_LIMIT,
    keyset_closed: bool | None = None,
    keyset_tag_slug: str | None = None,
    keyset_related_tags: bool = False,
    keyset_volume_min: float | None = None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
    progress_task: str = "gamma_events_keyset",
    progress_every_pages: int = 1,
    progress_extra: Callable[[int, bool], dict[str, Any]] | None = None,
) -> Iterator[tuple[list[dict[str, Any]], EventsPageMeta]]:
    """Yield (events_page, page_meta) for each Gamma /events/keyset response."""
    cursor: str | None = None
    pages_done = 0
    seen_event_ids: set[str] = set()

    while True:
        request_cursor = cursor
        params: dict[str, Any] = {"limit": fetch_limit}
        if keyset_closed is not None:
            params["closed"] = keyset_closed
        if keyset_tag_slug is not None:
            params["tag_slug"] = keyset_tag_slug
        if keyset_related_tags:
            params["related_tags"] = "true"
        if keyset_volume_min is not None:
            params["volume_min"] = keyset_volume_min
        if cursor:
            params["next_cursor"] = cursor
        payload = gamma_get(client, "/events/keyset", params=params)
        if isinstance(payload, dict):
            events = payload.get("events") or []
            cursor = payload.get("next_cursor")
        else:
            events = payload or []
            cursor = None
        pages_done += 1

        # Guard against an upstream keyset bug where Gamma ignores the supplied
        # cursor and echoes back the same non-advancing ``next_cursor`` forever.
        # Without this, pagination would loop on the same page indefinitely.
        if (
            request_cursor is not None
            and cursor is not None
            and cursor == request_cursor
        ):
            # Gamma often echoes the same cursor on the terminal page. Duplicate
            # rows signal natural EOF; genuinely new rows on a stuck cursor are stalls.
            new_events = _unseen_events(events, seen_event_ids)
            _remember_event_ids(events, seen_event_ids)
            truncated = bool(new_events)
            if truncated:
                logger.warning(
                    "Gamma /events/keyset returned a non-advancing cursor with "
                    "new data after %s page(s) (tag_slug=%s); stopping pagination to "
                    "avoid an infinite loop",
                    pages_done,
                    keyset_tag_slug,
                )
                yield new_events, EventsPageMeta(pages_done=pages_done, truncated=True)
            else:
                logger.info(
                    "Gamma /events/keyset pagination complete (non-advancing "
                    "cursor after %s page(s), tag_slug=%s)",
                    pages_done,
                    keyset_tag_slug,
                )
                yield [], EventsPageMeta(pages_done=pages_done, truncated=False)
            break

        hit_page_cap = max_pages is not None and pages_done >= max_pages
        page_meta = EventsPageMeta(
            pages_done=pages_done,
            truncated=hit_page_cap and bool(cursor),
        )

        if progress_callback and (
            progress_every_pages <= 1
            or pages_done == 1
            or pages_done % progress_every_pages == 0
            or hit_page_cap
        ):
            payload_dict: dict[str, Any] = {
                "events_page": pages_done,
                "truncated": page_meta.truncated,
                "events_cursor_present": bool(cursor),
            }
            if keyset_closed is not None:
                payload_dict["keyset_closed"] = keyset_closed
            if keyset_tag_slug is not None:
                payload_dict["keyset_tag_slug"] = keyset_tag_slug
            if keyset_related_tags:
                payload_dict["keyset_related_tags"] = True
            if keyset_volume_min is not None:
                payload_dict["keyset_volume_min"] = keyset_volume_min
            if progress_extra is not None:
                payload_dict.update(progress_extra(pages_done, bool(cursor)))
            progress_callback(progress_task, payload_dict)

        yield events, page_meta
        _remember_event_ids(events, seen_event_ids)

        if not events:
            break
        if not cursor:
            break
        if hit_page_cap:
            break


def fetch_gamma_event_by_slug(client: Any, slug: str) -> dict[str, Any] | None:
    """Fetch one Gamma event by slug; return None when missing (404)."""
    normalized = slug.strip().lower()
    if not normalized:
        return None
    try:
        payload = gamma_get(client, f"/events/slug/{normalized}")
    except GammaRequestError as exc:
        response = getattr(exc, "response", None)
        if response is not None and response.status_code == 404:
            logger.warning("Gamma event slug not found: %s", normalized)
            return None
        raise
    except requests.RequestException:
        raise
    if isinstance(payload, dict) and payload.get("id"):
        return payload
    logger.warning("Gamma event slug returned empty payload: %s", normalized)
    return None
