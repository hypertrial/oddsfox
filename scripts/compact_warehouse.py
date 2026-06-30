#!/usr/bin/env python3
"""
Compact a DuckDB warehouse file by rewriting only live data into a fresh file.

DuckDB grows its single file as tables are rebuilt (dbt full-refresh, repeated
ingestion, DROP/CREATE) but never returns freed blocks to the OS, so the file
keeps the high-water mark of all historical churn. ``CHECKPOINT`` only reuses
free blocks in place; it does not shrink the file. The reliable way to reclaim
space is to copy the live catalog into a new database via ``COPY FROM DATABASE``
and swap it in.

This script:
  1. Opens the source read-only and records table/row counts + on-disk size.
  2. ``ATTACH``-es the source (read-only) and a fresh target, then runs
     ``COPY FROM DATABASE src TO dst``.
  3. Verifies the target has the same schemas, tables/views, and row counts.
  4. Atomically swaps the compacted file into place, moving the original aside to
     ``<name>.duckdb.pre_compact_backup`` (kept unless ``--no-backup``).

The source must NOT be open read-write by another process (stop Dagster first).

Usage:
  python3 scripts/compact_warehouse.py
  python3 scripts/compact_warehouse.py --duckdb-path oddsfox.duckdb
  python3 scripts/compact_warehouse.py --no-backup
  python3 scripts/compact_warehouse.py --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Final

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

REPO_ROOT: Final[Path] = ensure_src_on_path()

BACKUP_SUFFIX: Final[str] = ".pre_compact_backup"


def _human(n: int) -> str:
    f = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if f < 1024 or unit == "TiB":
            return f"{f:,.2f} {unit}"
        f /= 1024
    return f"{f:,.2f} TiB"


def _table_counts(conn) -> dict[tuple[str, str], int]:
    """Map (schema, table) -> row count for every base table in the database."""
    rows = conn.execute(
        """
        SELECT schema_name, table_name
        FROM duckdb_tables()
        WHERE NOT internal
        ORDER BY schema_name, table_name
        """
    ).fetchall()
    counts: dict[tuple[str, str], int] = {}
    for schema, table in rows:
        n = conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]
        counts[(str(schema), str(table))] = int(n)
    return counts


def _view_keys(conn) -> set[tuple[str, str]]:
    rows = conn.execute(
        """
        SELECT schema_name, view_name
        FROM duckdb_views()
        WHERE NOT internal
        """
    ).fetchall()
    return {(str(s), str(v)) for s, v in rows}


def _database_size(conn) -> tuple[int, int, int]:
    """Return (total_bytes, used_bytes, free_bytes) from PRAGMA database_size."""
    df = conn.execute("PRAGMA database_size").fetchdf()
    row = df.iloc[0]
    block_size = int(row["block_size"])
    total = int(row["total_blocks"]) * block_size
    used = int(row["used_blocks"]) * block_size
    free = int(row["free_blocks"]) * block_size
    return total, used, free


def main() -> int:
    import duckdb

    from oddsfox.config import settings

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--duckdb-path",
        type=Path,
        default=None,
        help="DuckDB file to compact (default: DUCKDB_PATH from settings / .env)",
    )
    p.add_argument(
        "--no-backup",
        action="store_true",
        help=(
            "Delete the pre-compaction backup after a successful swap "
            "(default: keep it next to the warehouse)."
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and verify the compacted copy but do not swap it in.",
    )
    args = p.parse_args()

    src = Path(args.duckdb_path or settings.DUCKDB_PATH).resolve()
    if not src.is_file():
        sys.stderr.write(f"DuckDB file not found: {src}\n")
        return 1

    wal = src.with_name(src.name + ".wal")
    if wal.exists() or src.with_name(src.name + "-wal").exists():
        sys.stderr.write(
            f"Refusing to compact: WAL file present next to {src.name}. "
            "Open the DB once to checkpoint, or stop the writer, then retry.\n"
        )
        return 1

    tmp = src.with_name(src.stem + ".compact_tmp" + src.suffix)
    backup = src.with_name(src.name + BACKUP_SUFFIX)
    for stale in (tmp, tmp.with_name(tmp.name + ".wal")):
        if stale.exists():
            stale.unlink()

    print(f"Source warehouse : {src}")
    try:
        ro = duckdb.connect(str(src), read_only=True)
    except duckdb.IOException as exc:
        sys.stderr.write(
            f"Cannot open {src} read-only (is Dagster or another writer running?): {exc}\n"
        )
        return 1
    try:
        src_total, src_used, src_free = _database_size(ro)
        src_counts = _table_counts(ro)
        src_views = _view_keys(ro)
    finally:
        ro.close()

    src_file_bytes = src.stat().st_size
    print(
        f"  file size      : {_human(src_file_bytes)} "
        f"(used {_human(src_used)}, free {_human(src_free)})"
    )
    print(f"  tables         : {len(src_counts)}  views: {len(src_views)}")
    print(f"  total rows     : {sum(src_counts.values()):,}")

    def _sql_str(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    print(f"Compacting into  : {tmp}")
    t0 = time.perf_counter()
    builder = duckdb.connect()  # in-memory coordinator
    try:
        builder.execute(f"ATTACH {_sql_str(str(src))} AS src (READ_ONLY)")
        builder.execute(f"ATTACH {_sql_str(str(tmp))} AS dst")
        builder.execute("COPY FROM DATABASE src TO dst")
        builder.execute("CHECKPOINT dst")
        builder.execute("DETACH src")
        builder.execute("DETACH dst")
    finally:
        builder.close()
    print(f"  copy completed in {time.perf_counter() - t0:,.1f}s")

    # Verify parity before swapping.
    chk = duckdb.connect(str(tmp), read_only=True)
    try:
        dst_total, dst_used, dst_free = _database_size(chk)
        dst_counts = _table_counts(chk)
        dst_views = _view_keys(chk)
    finally:
        chk.close()

    problems: list[str] = []
    if dst_counts != src_counts:
        missing = sorted(set(src_counts) - set(dst_counts))
        extra = sorted(set(dst_counts) - set(src_counts))
        mismatched = sorted(
            k
            for k in set(src_counts) & set(dst_counts)
            if src_counts[k] != dst_counts[k]
        )
        if missing:
            problems.append(f"missing tables: {missing}")
        if extra:
            problems.append(f"unexpected tables: {extra}")
        for k in mismatched:
            problems.append(
                f"row count mismatch {k[0]}.{k[1]}: "
                f"src={src_counts[k]:,} dst={dst_counts[k]:,}"
            )
    if dst_views != src_views:
        problems.append(
            f"view set differs: missing={sorted(src_views - dst_views)} "
            f"extra={sorted(dst_views - src_views)}"
        )

    if problems:
        sys.stderr.write("Verification FAILED; leaving source untouched:\n")
        for prob in problems:
            sys.stderr.write(f"  - {prob}\n")
        tmp.unlink(missing_ok=True)
        tmp.with_name(tmp.name + ".wal").unlink(missing_ok=True)
        return 2

    tmp_file_bytes = tmp.stat().st_size
    print(
        f"Compacted file   : {_human(tmp_file_bytes)} "
        f"(used {_human(dst_used)}, free {_human(dst_free)})"
    )
    saved = src_file_bytes - tmp_file_bytes
    pct = (saved / src_file_bytes * 100) if src_file_bytes else 0.0
    print(f"Verified parity  : {len(dst_counts)} tables, {len(dst_views)} views match")
    print(f"Reclaimable      : {_human(saved)} ({pct:,.1f}% smaller)")

    if args.dry_run:
        print(f"--dry-run: leaving compacted copy at {tmp} (no swap performed).")
        return 0

    # Swap: original -> backup, compacted -> original. os.replace is atomic per path.
    if backup.exists():
        backup.unlink()
    os.replace(src, backup)
    try:
        os.replace(tmp, src)
    except BaseException:
        # Roll back if the second move fails so the warehouse is never missing.
        os.replace(backup, src)
        raise

    print(f"Swapped in compacted warehouse at {src}")
    if args.no_backup:
        backup.unlink(missing_ok=True)
        print("Removed pre-compaction backup (--no-backup).")
    else:
        print(f"Original preserved at {backup} ({_human(src_file_bytes)}).")
        print("Delete it once you've confirmed the pipeline runs cleanly.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
