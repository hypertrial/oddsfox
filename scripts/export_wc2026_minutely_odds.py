#!/usr/bin/env python3
"""
Export the full WC2026 minutely odds mart to a parquet file.

Reads ``polymarket_marts.wc2026_token_minutely_odds`` from the local DuckDB
warehouse and writes a single parquet file via DuckDB ``COPY``.

By default opens DuckDB **read-only**. Use ``--snapshot-copy`` when Dagster or
another job already holds a write connection on the live warehouse file.

Usage:
  python3 scripts/export_wc2026_minutely_odds.py
  python3 scripts/export_wc2026_minutely_odds.py --snapshot-copy
  python3 scripts/export_wc2026_minutely_odds.py --output /tmp/wc2026_minutely.parquet
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

REPO_ROOT: Final[Path] = ensure_src_on_path()

MART_SCHEMA: Final = "polymarket_marts"
MART_NAME: Final = "wc2026_token_minutely_odds"


def _snapshot_duckdb_files(src: Path, dest_dir: Path) -> Path:
    """Copy ``src`` and same-directory siblings (e.g. ``.wal``) for offline export."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    files = sorted(f for f in src.parent.glob(src.name + "*") if f.is_file())
    if not files:
        raise FileNotFoundError(
            f"No DuckDB files matched {src.name!r}* under {src.parent}"
        )
    for f in files:
        shutil.copy2(f, dest_dir / f.name)
    main = dest_dir / src.name
    if not main.is_file():
        raise FileNotFoundError(f"Expected {main} after snapshot copy")
    return main


def _mart_qualified_name() -> str:
    from oddsfox.storage.duckdb.profile.discovery import qualified_name

    return qualified_name(MART_SCHEMA, MART_NAME)


def mart_exists(conn: duckdb.DuckDBPyConnection) -> bool:
    row = conn.execute(
        """
        select count(*)
        from information_schema.tables
        where table_schema = ?
          and table_name = ?
        """,
        [MART_SCHEMA, MART_NAME],
    ).fetchone()
    return bool(row and row[0])


def export_minutely_odds_parquet(
    conn: duckdb.DuckDBPyConnection,
    output_path: Path,
) -> int:
    """Export the WC2026 minutely odds mart to ``output_path``; return row count."""
    if not mart_exists(conn):
        raise LookupError(
            f"Missing {MART_SCHEMA}.{MART_NAME}. Run dbt build or the pipeline first."
        )

    rel = _mart_qualified_name()
    row = conn.execute(f"select count(*) from {rel}").fetchone()
    row_count = int(row[0]) if row else 0

    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    conn.execute(
        f"""
        copy (select * from {rel})
        to ? (format parquet)
        """,
        [str(output_path)],
    )
    return row_count


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--duckdb-path",
        type=Path,
        default=None,
        help="DuckDB file (default: DUCKDB_PATH from settings / .env)",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=None,
        help=(
            "Destination parquet file "
            "(default: artifacts/wc2026_exports/wc2026_token_minutely_odds_<UTC>.parquet)"
        ),
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=REPO_ROOT / "artifacts" / "wc2026_exports",
        help="Directory for default timestamped output when --output is omitted",
    )
    p.add_argument(
        "--read-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Open DuckDB read-only (default: true). Use --no-read-only for read-write.",
    )
    p.add_argument(
        "--snapshot-copy",
        action="store_true",
        help=(
            "Copy the DuckDB file(s) into a temp folder under --output-dir, then "
            "export from the copy. Use when a writer already has the live file open."
        ),
    )
    args = p.parse_args()

    from oddsfox.config import settings
    from oddsfox.storage.duckdb import open_duckdb_connection

    duck = Path(args.duckdb_path or settings.DUCKDB_PATH).resolve()
    if args.output is not None:
        output_path = Path(args.output).resolve()
    else:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = args.output_dir / f"{MART_NAME}_{ts}.parquet"

    profile_path = duck
    snap_dir: Path | None = None
    if args.snapshot_copy:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        snap_dir = Path(
            tempfile.mkdtemp(
                prefix="wc2026_minutely_export_snap_",
                dir=str(args.output_dir),
            )
        )
        try:
            profile_path = _snapshot_duckdb_files(duck, snap_dir)
        except BaseException:
            shutil.rmtree(snap_dir, ignore_errors=True)
            raise

    conn = open_duckdb_connection(profile_path, read_only=args.read_only)
    try:
        row_count = export_minutely_odds_parquet(conn, output_path)
    except LookupError as exc:
        sys.stderr.write(f"{exc}\n")
        return 1
    finally:
        conn.close()
        if snap_dir is not None:
            shutil.rmtree(snap_dir, ignore_errors=True)

    print(f"Exported {row_count} rows to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
