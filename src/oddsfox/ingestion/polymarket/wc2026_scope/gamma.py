"""Gamma /markets batch helpers for WC2026 scope."""

from __future__ import annotations

import logging
from typing import Any, Iterable, Sequence

from oddsfox.ingestion.polymarket.errors import GammaRequestError

logger = logging.getLogger(__name__)


def _is_gamma_market_id(market_id: str) -> bool:
    """Gamma /markets?id=... expects numeric Polymarket market IDs."""
    return bool(str(market_id or "").strip().isdigit())


def _gamma_market_ids(market_ids: Iterable[str]) -> list[str]:
    valid = sorted({mid for mid in market_ids if _is_gamma_market_id(mid)})
    skipped = sorted(
        {str(mid) for mid in market_ids if not _is_gamma_market_id(str(mid))}
    )
    if skipped:
        logger.warning(
            "Skipping non-numeric registry market IDs for Gamma /markets: %s",
            skipped[:20] if len(skipped) > 20 else skipped,
        )
    return valid


def _chunk_market_ids(market_ids: Sequence[str], chunk_size: int) -> list[list[str]]:
    return [
        list(market_ids[i : i + chunk_size])
        for i in range(0, len(market_ids), chunk_size)
    ]


def _fetch_markets_batch_resilient(
    client: Any,
    chunk: Sequence[str],
    *,
    include_events: bool = True,
) -> list[dict[str, Any]]:
    if not chunk:
        return []
    from oddsfox.ingestion.polymarket.markets.backfill._gamma import (
        _fetch_markets_batch,
    )

    try:
        payload = _fetch_markets_batch(client, chunk, include_events=include_events)
        return list(payload or [])
    except GammaRequestError as exc:
        response = getattr(exc, "response", None)
        if response is None or response.status_code != 422:
            raise
        if len(chunk) == 1:
            logger.warning("Skipping invalid Gamma market id: %s", chunk[0])
            return []
        logger.warning(
            "Gamma /markets rejected batch of %s ids (422); retrying individually",
            len(chunk),
        )
        rows: list[dict[str, Any]] = []
        for market_id in chunk:
            rows.extend(
                _fetch_markets_batch_resilient(
                    client, [market_id], include_events=include_events
                )
            )
        return rows
