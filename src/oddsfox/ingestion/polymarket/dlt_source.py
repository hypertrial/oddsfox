"""dlt resources for WC2026 Polymarket raw market landing."""

from __future__ import annotations

from typing import Any, Iterable

import dlt

from oddsfox.config.settings import POLYMARKET_WC2026_KEYSET_VOLUME_MIN
from oddsfox.ingestion.polymarket.markets.fetch import build_client
from oddsfox.ingestion.polymarket.markets.persistence import prepare_batch_for_db
from oddsfox.ingestion.polymarket.markets.transform import process_markets_dataframe
from oddsfox.ingestion.polymarket.wc2026_scope import (
    DISCOVERY_MODE_FULL_KEYSET,
    DISCOVERY_MODE_TARGETED,
    DiscoveryMode,
    load_wc2026_config,
    refresh_registry_and_collect_markets_from_events,
    refresh_registry_and_collect_markets_targeted,
    resolve_keyset_tag_slugs,
)
from oddsfox.storage.duckdb.dlt_batch import DLT_STRICT_SCHEMA_CONTRACT


def collect_raw_markets(
    *,
    discovery_mode: DiscoveryMode = DISCOVERY_MODE_FULL_KEYSET,
    max_event_pages: int | None = None,
    max_pages_without_progress: int | None = None,
    keyset_closed: bool | None = False,
    keyset_tag_slugs: list[str] | None = None,
    keyset_volume_min: float | None = POLYMARKET_WC2026_KEYSET_VOLUME_MIN,
) -> list[dict[str, Any]]:
    client = build_client()
    cfg = load_wc2026_config()
    if discovery_mode == DISCOVERY_MODE_TARGETED:
        _, raw_markets, _ = refresh_registry_and_collect_markets_targeted(
            client,
            config=cfg,
        )
        return list(raw_markets)

    effective_keyset_tag_slugs = resolve_keyset_tag_slugs(
        keyset_tag_slugs, config=cfg, client=client
    )
    _, raw_markets, _ = refresh_registry_and_collect_markets_from_events(
        client,
        config=cfg,
        max_pages=max_event_pages,
        max_pages_without_progress=max_pages_without_progress,
        keyset_closed=keyset_closed,
        keyset_tag_slugs=effective_keyset_tag_slugs or None,
        keyset_volume_min=keyset_volume_min,
    )
    return list(raw_markets)


def normalize_market_payloads_for_dlt(
    markets: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    df = process_markets_dataframe(list(markets))
    market_rows, _ = prepare_batch_for_db(df)
    cols = (
        "id",
        "question",
        "category",
        "description",
        "outcomes",
        "volume",
        "active",
        "closed",
        "created_at",
        "scraped_at",
        "end_date",
        "slug",
        "event_slug",
        "event_id",
    )
    rows_by_id: dict[str, dict[str, Any]] = {}
    for row in market_rows:
        payload = dict(zip(cols, row, strict=True))
        rows_by_id[str(payload["id"])] = payload
    return list(rows_by_id.values())


@dlt.source(name="polymarket")
def polymarket_markets_source(rows: Iterable[dict[str, Any]] = ()):
    @dlt.resource(
        name="markets",
        primary_key="id",
        write_disposition="merge",
        schema_contract=DLT_STRICT_SCHEMA_CONTRACT,
        columns={
            "id": {"data_type": "text"},
            "question": {"data_type": "text"},
            "category": {"data_type": "text"},
            "description": {"data_type": "text"},
            "outcomes": {"data_type": "text"},
            "volume": {"data_type": "double"},
            "active": {"data_type": "bool"},
            "closed": {"data_type": "bool"},
            "created_at": {"data_type": "timestamp", "timezone": False},
            "scraped_at": {"data_type": "timestamp", "timezone": False},
            "end_date": {"data_type": "timestamp", "timezone": False},
            "slug": {"data_type": "text"},
            "event_slug": {"data_type": "text"},
            "event_id": {"data_type": "text"},
        },
    )
    def markets():
        yield from rows

    return markets


__all__ = [
    "collect_raw_markets",
    "normalize_market_payloads_for_dlt",
    "polymarket_markets_source",
]
