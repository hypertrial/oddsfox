#!/usr/bin/env python3
"""Audit WC 2026 Polymarket scope: registry vs allowlist vs strict filter."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
for _rp in (REPO_ROOT, REPO_ROOT / "src"):
    if str(_rp) not in sys.path:
        sys.path.insert(0, str(_rp))

from oddsfox.ingestion.polymarket.wc2026_scope import (  # noqa: E402
    MARKET_SCOPE_WC2026,
    load_wc2026_config,
    market_scope_predicate_sql,
)
from oddsfox.storage.duckdb.connection import (  # noqa: E402
    ensure_duck_db,
    get_connection,
)
from oddsfox.storage.duckdb.schemas.constants import (  # noqa: E402
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)
from oddsfox.storage.duckdb.wc2026_registry import (  # noqa: E402
    registry_market_count,
)


def _registry_by_source(conn) -> dict[str, int]:
    tab = polymarket_ops_tbl("wc2026_market_registry")
    rows = conn.execute(
        f"""
        SELECT source, COUNT(*)::BIGINT
        FROM {tab}
        GROUP BY source
        ORDER BY source
        """
    ).fetchall()
    return {str(r[0]): int(r[1]) for r in rows}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fail-on-allowlist-gaps",
        action="store_true",
        help="Exit 1 when markets have allowlisted event_slug but are not strict-scoped.",
    )
    parser.add_argument(
        "--fail-on-discovery-rows",
        action="store_true",
        help="Exit 1 when registry still has source=discovery rows (stale warehouse).",
    )
    args = parser.parse_args()

    ensure_duck_db()
    cfg = load_wc2026_config()
    m = polymarket_raw_tbl("markets")
    strict_sql = market_scope_predicate_sql(MARKET_SCOPE_WC2026, "m")

    with get_connection() as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM {m}").fetchone()[0]
        strict_n = conn.execute(
            f"SELECT COUNT(*) FROM {m} m WHERE {strict_sql}"
        ).fetchone()[0]
        slug_list = ", ".join(f"'{s}'" for s in cfg.event_slugs)
        gap_n = 0
        if slug_list:
            gap_n = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {m} m
                WHERE lower(coalesce(m.event_slug, '')) IN ({slug_list})
                  AND NOT ({strict_sql})
                """
            ).fetchone()[0]
        by_source = _registry_by_source(conn)

    discovery_n = by_source.get("discovery", 0)

    print(f"Markets total: {total}")
    print(f"Registry rows: {registry_market_count()}")
    print(f"Strict wc2026 markets: {strict_n}")
    print(f"Allowlisted event_slug not strict-scoped: {gap_n}")
    print(f"Registry by source: {by_source}")
    print(f"Configured event_slugs: {cfg.event_slugs}")
    print(f"Configured prefixes: {cfg.event_slug_prefixes}")

    exit_code = 0
    if args.fail_on_allowlist_gaps and gap_n:
        exit_code = 1
    if args.fail_on_discovery_rows and discovery_n:
        exit_code = 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
