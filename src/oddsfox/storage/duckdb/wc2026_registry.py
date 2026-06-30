"""DuckDB persistence for WC 2026 market registry (polymarket_ops)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Sequence

from oddsfox.storage.duckdb.connection import ensure_duck_db, get_connection
from oddsfox.storage.duckdb.schemas.constants import polymarket_ops_tbl

_TAB_REGISTRY = polymarket_ops_tbl("wc2026_market_registry")


@dataclass(frozen=True)
class RegistryRow:
    market_id: str
    event_slug: str | None
    event_id: str | None
    source: str


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def upsert_registry_rows(rows: Sequence[RegistryRow]) -> int:
    if not rows:
        return 0
    ensure_duck_db()
    now = _utc_now()
    payload = [(r.market_id, r.event_slug, r.event_id, r.source, now) for r in rows]
    with get_connection() as conn:
        conn.executemany(
            f"""
            INSERT INTO {_TAB_REGISTRY}
            (market_id, event_slug, event_id, source, refreshed_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(market_id) DO UPDATE SET
              event_slug=COALESCE(excluded.event_slug, {_TAB_REGISTRY}.event_slug),
              event_id=COALESCE(excluded.event_id, {_TAB_REGISTRY}.event_id),
              source=excluded.source,
              refreshed_at=excluded.refreshed_at
            """,
            payload,
        )
    return len(payload)


def get_registry_market_ids() -> List[str]:
    ensure_duck_db()
    with get_connection() as conn:
        result = conn.execute(
            f"SELECT market_id FROM {_TAB_REGISTRY} ORDER BY market_id"
        ).fetchall()
    return [str(row[0]) for row in result]


def registry_market_count() -> int:
    ensure_duck_db()
    with get_connection() as conn:
        row = conn.execute(f"SELECT COUNT(*) FROM {_TAB_REGISTRY}").fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def clear_registry() -> None:
    """Remove all registry rows (tests only)."""
    ensure_duck_db()
    with get_connection() as conn:
        conn.execute(f"DELETE FROM {_TAB_REGISTRY}")


def get_registry_event_slugs() -> List[str]:
    """Return distinct non-null event_slug values from the WC2026 registry."""
    ensure_duck_db()
    with get_connection() as conn:
        result = conn.execute(
            f"""
            SELECT DISTINCT event_slug
            FROM {_TAB_REGISTRY}
            WHERE event_slug IS NOT NULL AND TRIM(event_slug) != ''
            ORDER BY event_slug
            """
        ).fetchall()
    return [str(row[0]) for row in result]


__all__ = [
    "RegistryRow",
    "clear_registry",
    "get_registry_event_slugs",
    "get_registry_market_ids",
    "registry_market_count",
    "upsert_registry_rows",
]
