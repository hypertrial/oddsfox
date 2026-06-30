"""WC2026 scope configuration and seed loading."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import yaml

from oddsfox.config.settings import (
    POLYMARKET_WC2026_DEFAULT_EVENT_SLUG,
    POLYMARKET_WC2026_REGISTRY_MAX_EVENT_PAGES,
)

logger = logging.getLogger(__name__)


def default_wc2026_seed_path() -> Path:
    return Path(__file__).resolve().parent.parent / "seeds" / "wc2026_events.yml"


def _parse_csv_list(raw: str | None) -> tuple[str, ...]:
    if not raw or not str(raw).strip():
        return ()
    return tuple(s.strip() for s in str(raw).split(",") if s.strip())


def _validate_slug_token(slug: str) -> str:
    s = slug.strip()
    if not s or not re.fullmatch(r"[a-z0-9][a-z0-9-]*", s, flags=re.IGNORECASE):
        raise ValueError(f"Invalid event slug token: {slug!r}")
    return s.lower()


@dataclass(frozen=True)
class Wc2026ScopeConfig:
    event_slugs: tuple[str, ...]
    event_slug_prefixes: tuple[str, ...]
    market_ids: tuple[str, ...]
    registry_max_event_pages: int | None
    event_tags: tuple[str, ...] = ()

    @property
    def default_event_slug(self) -> str:
        if self.event_slugs:
            return self.event_slugs[0]
        return POLYMARKET_WC2026_DEFAULT_EVENT_SLUG

    @property
    def default_keyset_tag_slugs(self) -> tuple[str, ...]:
        return self.event_tags


def load_wc2026_config(
    *,
    seed_path: Path | None = None,
    event_slugs_override: Sequence[str] | None = None,
    event_slug_prefixes_override: Sequence[str] | None = None,
    event_tags_override: Sequence[str] | None = None,
    market_ids_override: Sequence[str] | None = None,
) -> Wc2026ScopeConfig:
    path = seed_path or default_wc2026_seed_path()
    seed_slugs: list[str] = []
    seed_prefixes: list[str] = []
    seed_tags: list[str] = []
    seed_market_ids: list[str] = []
    if path.is_file():
        with path.open(encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        if not isinstance(raw, dict):
            raise ValueError(f"Invalid YAML root in {path}")
        for key, dest in (
            ("event_slugs", seed_slugs),
            ("event_slug_prefixes", seed_prefixes),
            ("event_tags", seed_tags),
            ("market_ids", seed_market_ids),
        ):
            vals = raw.get(key)
            if vals is None:
                continue
            if not isinstance(vals, list) or not all(isinstance(v, str) for v in vals):
                raise ValueError(f"{key} must be a list of strings in {path}")
            dest.extend(v.strip() for v in vals if v.strip())

    env_slugs = _parse_csv_list(os.getenv("POLYMARKET_WC2026_EVENT_SLUGS"))
    env_prefixes = _parse_csv_list(os.getenv("POLYMARKET_WC2026_EVENT_SLUG_PREFIXES"))
    env_tags = _parse_csv_list(os.getenv("POLYMARKET_WC2026_EVENT_TAGS"))

    slugs = list(event_slugs_override or ()) or env_slugs or tuple(seed_slugs)
    prefixes = (
        list(event_slug_prefixes_override or ()) or env_prefixes or tuple(seed_prefixes)
    )
    tags = (
        list(event_tags_override or ())
        if event_tags_override is not None
        else (env_tags or tuple(seed_tags))
    )
    market_ids = list(market_ids_override or ()) or tuple(seed_market_ids)

    if not slugs and POLYMARKET_WC2026_DEFAULT_EVENT_SLUG:
        slugs = [POLYMARKET_WC2026_DEFAULT_EVENT_SLUG]

    normalized_slugs = tuple(_validate_slug_token(s) for s in slugs)
    normalized_prefixes = tuple(_validate_slug_token(p) for p in prefixes)
    normalized_tags = tuple(_validate_slug_token(t) for t in tags)
    return Wc2026ScopeConfig(
        event_slugs=normalized_slugs,
        event_slug_prefixes=normalized_prefixes,
        market_ids=tuple(str(m).strip() for m in market_ids if str(m).strip()),
        registry_max_event_pages=POLYMARKET_WC2026_REGISTRY_MAX_EVENT_PAGES,
        event_tags=normalized_tags,
    )


def scope_config_hash(cfg: Wc2026ScopeConfig) -> str:
    payload = json.dumps(
        {
            "event_slugs": list(cfg.event_slugs),
            "event_slug_prefixes": list(cfg.event_slug_prefixes),
            "event_tags": list(cfg.event_tags),
            "market_ids": list(cfg.market_ids),
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
