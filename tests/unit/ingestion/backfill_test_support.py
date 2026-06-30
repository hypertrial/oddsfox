"""Shared patch targets for backfill unit tests (patch where symbols are used)."""

from __future__ import annotations

from oddsfox.ingestion.polymarket.markets.backfill import (
    _events_fallback as bf_events_fallback,
)
from oddsfox.ingestion.polymarket.markets.backfill import (
    _gamma as bf_gamma,
)
from oddsfox.ingestion.polymarket.markets.backfill import (
    end_dates as bf_end_dates,
)
from oddsfox.ingestion.polymarket.markets.backfill import (
    event_slugs as bf_event_slugs,
)
from oddsfox.ingestion.polymarket.markets.backfill import (
    metadata as bf_metadata,
)
from oddsfox.ingestion.polymarket.markets.backfill import (
    slugs as bf_slugs,
)
from oddsfox.ingestion.polymarket.markets.backfill import (
    tokens as bf_tokens,
)

BACKFILL_ENTRYPOINT_MODULES = {
    "backfill_tokens": bf_tokens,
    "backfill_slugs": bf_slugs,
    "backfill_end_dates": bf_end_dates,
    "backfill_event_slugs": bf_event_slugs,
    "backfill_market_metadata": bf_metadata,
}


def patch_ensure_duck_db(monkeypatch) -> None:
    """Stub DuckDB init for all backfill entrypoint modules."""
    for mod in BACKFILL_ENTRYPOINT_MODULES.values():
        monkeypatch.setattr(mod, "ensure_duck_db", lambda: None)


def entrypoint_module_for(body: str):
    """Return the submodule under test based on backfill_* calls in a test body."""
    for name, mod in BACKFILL_ENTRYPOINT_MODULES.items():
        if f"bf.{name}" in body:
            return mod
    return None


__all__ = [
    "BACKFILL_ENTRYPOINT_MODULES",
    "bf_end_dates",
    "bf_event_slugs",
    "bf_events_fallback",
    "bf_gamma",
    "bf_metadata",
    "bf_slugs",
    "bf_tokens",
    "entrypoint_module_for",
    "patch_ensure_duck_db",
]
