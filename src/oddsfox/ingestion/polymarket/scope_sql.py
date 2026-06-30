"""WC 2026 market scope SQL builders (no HTTP/registry dependencies)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

if TYPE_CHECKING:
    from oddsfox.ingestion.polymarket.wc2026_scope import Wc2026ScopeConfig

MARKET_SCOPE_ALL = "all"
MARKET_SCOPE_WC2026 = "wc2026"


def validate_market_scope(market_scope: str | None) -> str:
    scope = (market_scope or MARKET_SCOPE_ALL).strip().lower()
    allowed = {MARKET_SCOPE_ALL, MARKET_SCOPE_WC2026}
    if scope not in allowed:
        raise ValueError(
            f"market_scope must be one of {sorted(allowed)}, got {market_scope!r}"
        )
    return scope


def _sql_quote_list(values: Sequence[str]) -> str:
    if not values:
        return "NULL"
    parts = ", ".join("'" + v.replace("'", "''") + "'" for v in values)
    return parts


def _strict_scope_sql(alias: str = "m", config: Wc2026ScopeConfig | None = None) -> str:
    if config is None:
        from oddsfox.ingestion.polymarket.wc2026_scope import (
            load_wc2026_config,
        )

        config = load_wc2026_config()
    from oddsfox.storage.duckdb.schemas.constants import polymarket_ops_tbl

    registry = polymarket_ops_tbl("wc2026_market_registry")
    clauses: list[str] = [f"{alias}.id IN (SELECT market_id FROM {registry})"]
    if config.event_slugs:
        clauses.append(f"{alias}.event_slug IN ({_sql_quote_list(config.event_slugs)})")
    for prefix in config.event_slug_prefixes:
        escaped = prefix.replace("'", "''")
        clauses.append(f"LOWER(COALESCE({alias}.event_slug, '')) LIKE '{escaped}%'")
    if config.market_ids:
        clauses.append(f"{alias}.id IN ({_sql_quote_list(config.market_ids)})")
    inner = "(" + " OR ".join(clauses) + ")"
    return f"COALESCE({inner}, FALSE)"


def market_scope_sql(
    market_scope: str | None,
    alias: str = "m",
    *,
    config: Wc2026ScopeConfig | None = None,
) -> str:
    """Return SQL AND-clause fragment (includes leading AND) or empty string for `all`."""
    scope = validate_market_scope(market_scope)
    if scope == MARKET_SCOPE_ALL:
        return ""
    return f"AND {_strict_scope_sql(alias, config)}"


def market_scope_predicate_sql(
    market_scope: str | None,
    alias: str = "m",
    *,
    config: Wc2026ScopeConfig | None = None,
) -> str:
    """Return bare boolean SQL (no leading AND) for negation in exclusion counts."""
    scope = validate_market_scope(market_scope)
    if scope == MARKET_SCOPE_ALL:
        return "TRUE"
    return _strict_scope_sql(alias, config)
