#!/usr/bin/env python3
"""Delete polymarket_raw.odds_history rows older than a retention window."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

ensure_src_on_path()

import duckdb  # noqa: E402

from oddsfox.config import settings  # noqa: E402
from oddsfox.storage.duckdb.schemas.constants import polymarket_raw_tbl  # noqa: E402

_ODDS_HISTORY = polymarket_raw_tbl("odds_history")
_DEFAULT_RETENTION_DAYS = 365


def _cutoff_epoch(retention_days: int) -> int:
    if retention_days <= 0:
        raise ValueError("retention_days must be > 0")
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    return int(cutoff.timestamp())


def _scalar_int(conn: duckdb.DuckDBPyConnection, sql: str, *params: object) -> int:
    row = conn.execute(sql, params).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def prune_odds_history(
    conn: duckdb.DuckDBPyConnection,
    retention_days: int,
    *,
    dry_run: bool = False,
) -> dict[str, int]:
    """Delete odds_history rows older than retention_days. Returns counts."""
    cutoff = _cutoff_epoch(retention_days)
    total_before = _scalar_int(conn, f"SELECT COUNT(*) FROM {_ODDS_HISTORY}")
    to_delete = _scalar_int(
        conn,
        f"SELECT COUNT(*) FROM {_ODDS_HISTORY} WHERE timestamp < ?",
        cutoff,
    )
    if dry_run:
        return {
            "cutoff_epoch": cutoff,
            "total_before": total_before,
            "deleted": to_delete,
            "remaining": total_before,
        }

    conn.execute(
        f"DELETE FROM {_ODDS_HISTORY} WHERE timestamp < ?",
        [cutoff],
    )
    conn.execute("CHECKPOINT")
    remaining = _scalar_int(conn, f"SELECT COUNT(*) FROM {_ODDS_HISTORY}")
    return {
        "cutoff_epoch": cutoff,
        "total_before": total_before,
        "deleted": to_delete,
        "remaining": remaining,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duckdb-path",
        type=Path,
        default=None,
        help="DuckDB file (default: DUCKDB_PATH from settings / .env)",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=_DEFAULT_RETENTION_DAYS,
        help=f"Keep odds_history rows with timestamp >= now - N days (default: {_DEFAULT_RETENTION_DAYS})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report rows that would be deleted without writing",
    )
    args = parser.parse_args()
    duckdb_path = Path(args.duckdb_path or settings.DUCKDB_PATH).resolve()
    if not duckdb_path.exists():
        parser.error(f"DuckDB file does not exist: {duckdb_path}")

    try:
        with duckdb.connect(str(duckdb_path)) as conn:
            summary = prune_odds_history(
                conn,
                args.retention_days,
                dry_run=args.dry_run,
            )
    except ValueError as exc:
        parser.error(str(exc))

    cutoff_dt = datetime.fromtimestamp(
        summary["cutoff_epoch"], tz=timezone.utc
    ).isoformat()
    mode = "dry-run" if args.dry_run else "pruned"
    print(
        f"{mode}: {_ODDS_HISTORY} in {duckdb_path} "
        f"(retention_days={args.retention_days}, cutoff_utc={cutoff_dt})"
    )
    print(
        f"  total_before={summary['total_before']:,} "
        f"deleted={summary['deleted']:,} remaining={summary['remaining']:,}"
    )
    # ponytail: hard delete only; no archive/undo beyond --dry-run and compact backup.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
