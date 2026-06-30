"""Gamma tag discovery for WC 2026 Polymarket scope (no registry/HTTP crawl logic)."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

import requests

from oddsfox.ingestion.polymarket.errors import GammaRequestError, gamma_get

logger = logging.getLogger(__name__)

DEFAULT_WC2026_TAG_DISCOVERY_KEYWORDS: tuple[str, ...] = (
    "world cup",
    "world-cup",
    "fifa",
    "world-cup-2026",
    "fifa-world-cup",
    "wc-2026",
    "world-cup-qualifier",
    "world-cup-qualifiers",
)

_SLUG_TOKEN_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$", re.IGNORECASE)


def _normalize_slug_token(value: str) -> str | None:
    s = str(value or "").strip().lower()
    if not s or not _SLUG_TOKEN_RE.fullmatch(s):
        return None
    return s


def _tag_text_blob(tag: dict[str, Any]) -> str:
    parts = [
        str(tag.get("label") or ""),
        str(tag.get("slug") or ""),
        str(tag.get("name") or ""),
    ]
    return " ".join(parts).lower()


def tag_matches_keywords(tag: dict[str, Any], keywords: Sequence[str]) -> bool:
    blob = _tag_text_blob(tag)
    if not blob:
        return False
    for keyword in keywords:
        kw = str(keyword or "").strip().lower()
        if kw and kw in blob:
            return True
    return False


def _coerce_tag_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [t for t in payload if isinstance(t, dict)]
    return []


def fetch_gamma_tag_by_slug(client: Any, slug: str) -> dict[str, Any] | None:
    """Fetch one Gamma tag by slug; return None when missing (404)."""
    normalized = _normalize_slug_token(slug)
    if not normalized:
        return None
    try:
        payload = gamma_get(client, f"/tags/slug/{normalized}")
    except GammaRequestError as exc:
        response = getattr(exc, "response", None)
        if response is not None and response.status_code == 404:
            logger.warning("Gamma tag slug not found: %s", normalized)
            return None
        raise
    except requests.RequestException:
        raise
    if isinstance(payload, dict) and payload.get("id"):
        return payload
    logger.warning("Gamma tag slug returned empty payload: %s", normalized)
    return None


def fetch_gamma_sports(client: Any) -> list[dict[str, Any]]:
    payload = gamma_get(client, "/sports")
    return _coerce_tag_list(payload)


def fetch_gamma_tags(client: Any, *, limit: int = 1000) -> list[dict[str, Any]]:
    payload = gamma_get(client, "/tags", params={"limit": limit})
    return _coerce_tag_list(payload)


def _sport_tag_ids(sport: dict[str, Any]) -> list[str]:
    raw = sport.get("tags")
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(t).strip() for t in raw if str(t).strip()]
    return [part.strip() for part in str(raw).split(",") if part.strip()]


def _tags_by_id(tags: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    for tag in tags:
        tag_id = str(tag.get("id") or "").strip()
        if tag_id:
            by_id[tag_id] = tag
    return by_id


@dataclass(frozen=True)
class Wc2026TagDiscoveryResult:
    tag_slugs: tuple[str, ...]
    tag_ids: tuple[str, ...]
    sources: dict[str, tuple[str, ...]]


def discover_wc2026_tag_slugs(
    client: Any,
    *,
    seed_slugs: Sequence[str],
    keywords: Sequence[str] = DEFAULT_WC2026_TAG_DISCOVERY_KEYWORDS,
) -> Wc2026TagDiscoveryResult:
    """Union seed slugs with WC-like tags from /tags/slug, /tags, and /sports."""
    slug_sources: dict[str, set[str]] = {}
    id_by_slug: dict[str, str] = {}

    def _add_slug(slug: str | None, source: str) -> None:
        normalized = _normalize_slug_token(slug or "")
        if not normalized:
            return
        slug_sources.setdefault(normalized, set()).add(source)

    for seed in seed_slugs:
        normalized = _normalize_slug_token(seed)
        if not normalized:
            continue
        _add_slug(normalized, "seed")
        try:
            tag = fetch_gamma_tag_by_slug(client, normalized)
        except (GammaRequestError, requests.RequestException):
            logger.warning(
                "Failed to resolve seed tag slug %s via /tags/slug",
                normalized,
                exc_info=True,
            )
            continue
        if tag:
            tag_id = str(tag.get("id") or "").strip()
            if tag_id:
                id_by_slug[normalized] = tag_id
            _add_slug(str(tag.get("slug") or normalized), "tags_slug")

    supplemental_tags: list[dict[str, Any]] = []
    try:
        supplemental_tags.extend(fetch_gamma_tags(client))
    except (GammaRequestError, requests.RequestException):
        logger.warning("Failed to fetch Gamma /tags for discovery", exc_info=True)

    sports_tag_ids: set[str] = set()
    try:
        for sport in fetch_gamma_sports(client):
            sports_tag_ids.update(_sport_tag_ids(sport))
    except (GammaRequestError, requests.RequestException):
        logger.warning("Failed to fetch Gamma /sports for discovery", exc_info=True)

    tags_by_id = _tags_by_id(supplemental_tags)
    for tag_id in sports_tag_ids:
        if tag_id in tags_by_id:
            continue
        # Sports may reference tag ids absent from the /tags page; keep id only.

    for tag in supplemental_tags:
        if not tag_matches_keywords(tag, keywords):
            continue
        slug = _normalize_slug_token(str(tag.get("slug") or ""))
        if not slug:
            continue
        _add_slug(slug, "tags_list")
        tag_id = str(tag.get("id") or "").strip()
        if tag_id:
            id_by_slug[slug] = tag_id

    for tag_id in sorted(sports_tag_ids):
        tag = tags_by_id.get(tag_id)
        if tag is None:
            continue
        if not tag_matches_keywords(tag, keywords):
            continue
        slug = _normalize_slug_token(str(tag.get("slug") or ""))
        if not slug:
            continue
        _add_slug(slug, "sports")
        id_by_slug[slug] = tag_id

    ordered_slugs = tuple(sorted(slug_sources))
    ordered_ids = tuple(
        id_by_slug[slug] for slug in ordered_slugs if slug in id_by_slug
    )
    sources = {slug: tuple(sorted(srcs)) for slug, srcs in slug_sources.items()}
    return Wc2026TagDiscoveryResult(
        tag_slugs=ordered_slugs,
        tag_ids=ordered_ids,
        sources=sources,
    )


__all__ = [
    "DEFAULT_WC2026_TAG_DISCOVERY_KEYWORDS",
    "Wc2026TagDiscoveryResult",
    "discover_wc2026_tag_slugs",
    "fetch_gamma_sports",
    "fetch_gamma_tag_by_slug",
    "fetch_gamma_tags",
    "tag_matches_keywords",
]
