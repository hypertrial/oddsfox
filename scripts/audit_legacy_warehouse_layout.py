#!/usr/bin/env python3
"""Fail when a DuckDB file still uses deprecated warehouse layouts (pre-migration)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
for _rp in (REPO_ROOT, REPO_ROOT / "src"):
    if str(_rp) not in sys.path:
        sys.path.insert(0, str(_rp))

# Polymarket tables historically created in schema ``main``
_LEGACY_MAIN_POLYMARKET_TABLES = (
    "markets",
    "market_tokens",
    "odds_history",
    "token_odds_daily",
    "scrape_metadata",
    "token_sync_ledger",
    "token_sync_skips",
    "pipeline_run_events",
)

_LEGACY_DBT_SCHEMAS = (
    "main_staging",
    "main_intermediate",
    "main_marts_core",
    "main_marts_observability",
    "analytics",
    "analytics_staging",
    "analytics_intermediate",
    "analytics_marts_core",
    "analytics_marts_observability",
    "analytics_external",
)

_LEGACY_STATSBOMB_SCHEMAS = (
    "statsbomb_raw",
    "statsbomb_ops",
    "statsbomb_build",
)


def _schema_exists(conn, schema: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*) FROM information_schema.schemata
        WHERE schema_name = ?
        """,
        [schema],
    ).fetchone()
    return bool(row and row[0])


def _table_exists(conn, schema: str, name: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, name],
    ).fetchone()
    return bool(row and row[0])


def audit(conn) -> list[str]:
    findings: list[str] = []
    for table in _LEGACY_MAIN_POLYMARKET_TABLES:
        if _table_exists(conn, "main", table):
            findings.append(f"main.{table}")
    for schema in _LEGACY_DBT_SCHEMAS + _LEGACY_STATSBOMB_SCHEMAS:
        if _schema_exists(conn, schema):
            findings.append(f"schema:{schema}")
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duckdb-path",
        type=Path,
        default=None,
        help="DuckDB file (default: DUCKDB_PATH from settings / .env)",
    )
    args = parser.parse_args()

    import duckdb

    from oddsfox.config import settings

    duckdb_path = Path(args.duckdb_path or settings.DUCKDB_PATH).resolve()
    conn = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        findings = audit(conn)
    finally:
        conn.close()

    if findings:
        print(f"Legacy layout detected in {duckdb_path}:")
        for item in findings:
            print(f"  - {item}")
        print(
            "Run: python3 scripts/cleanup_legacy_warehouse.py "
            f"--duckdb-path {duckdb_path}"
        )
        return 1

    print(f"No legacy main/dbt/statsbomb layout in {duckdb_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
