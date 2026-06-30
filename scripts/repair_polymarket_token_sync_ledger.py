#!/usr/bin/env python3
"""Rebuild a corrupted Polymarket token sync ledger and its primary-key index."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

ensure_src_on_path()

import duckdb  # noqa: E402

from oddsfox.config import settings  # noqa: E402

_SCHEMA = "polymarket_ops"
_TABLE = "token_sync_ledger"
_REBUILD_TABLE = "token_sync_ledger_rebuild"
_QUALIFIED_TABLE = f"{_SCHEMA}.{_TABLE}"
_QUALIFIED_REBUILD_TABLE = f"{_SCHEMA}.{_REBUILD_TABLE}"
_MIN_DUCKDB_VERSION = (1, 5, 2)
_EXPECTED_COLUMNS = (
    ("clobTokenId", "VARCHAR"),
    ("last_sync_timestamp", "BIGINT"),
    ("fully_checked", "BOOLEAN"),
    ("last_checked_at", "TIMESTAMP"),
    ("next_check_at", "TIMESTAMP"),
    ("empty_run_streak", "INTEGER"),
)
_COLUMN_LIST = ", ".join(name for name, _ in _EXPECTED_COLUMNS)


def _version_tuple(version: str) -> tuple[int, int, int]:
    parts = [int(part) for part in re.findall(r"\d+", version)[:3]]
    padded = (parts + [0, 0, 0])[:3]
    return padded[0], padded[1], padded[2]


def _require_supported_duckdb() -> None:
    if _version_tuple(duckdb.__version__) < _MIN_DUCKDB_VERSION:
        required = ".".join(str(part) for part in _MIN_DUCKDB_VERSION)
        raise RuntimeError(
            f"DuckDB {required}+ is required; found {duckdb.__version__}"
        )


def _table_columns(conn, table: str) -> tuple[tuple[str, str], ...]:
    rows = conn.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
        """,
        [_SCHEMA, table],
    ).fetchall()
    return tuple((str(name), str(data_type)) for name, data_type in rows)


def _has_expected_primary_key(conn, table: str) -> bool:
    rows = conn.execute(
        """
        SELECT constraint_type, constraint_column_names
        FROM duckdb_constraints()
        WHERE schema_name = ? AND table_name = ?
        """,
        [_SCHEMA, table],
    ).fetchall()
    return any(
        constraint_type == "PRIMARY KEY" and list(columns) == ["clobTokenId"]
        for constraint_type, columns in rows
    )


def _validate_schema(conn, table: str) -> None:
    columns = _table_columns(conn, table)
    if columns != _EXPECTED_COLUMNS:
        raise RuntimeError(
            f"Unexpected {_SCHEMA}.{table} schema: {columns!r}; "
            f"expected {_EXPECTED_COLUMNS!r}"
        )
    if not _has_expected_primary_key(conn, table):
        raise RuntimeError(f"{_SCHEMA}.{table} must have PRIMARY KEY (clobTokenId)")


def _scalar_int(conn, sql: str) -> int:
    row = conn.execute(sql).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _verify_copy(conn, expected_rows: int) -> None:
    copied_rows = _scalar_int(conn, f"SELECT COUNT(*) FROM {_QUALIFIED_REBUILD_TABLE}")
    if copied_rows != expected_rows:
        raise RuntimeError(
            f"Ledger copy count mismatch: source={expected_rows}, copy={copied_rows}"
        )
    distinct_keys = _scalar_int(
        conn,
        f"SELECT COUNT(DISTINCT clobTokenId) FROM {_QUALIFIED_REBUILD_TABLE}",
    )
    if distinct_keys != copied_rows:
        raise RuntimeError(
            f"Ledger copy has duplicate token IDs: rows={copied_rows}, "
            f"distinct={distinct_keys}"
        )
    null_keys = _scalar_int(
        conn,
        f"SELECT COUNT(*) FROM {_QUALIFIED_REBUILD_TABLE} WHERE clobTokenId IS NULL",
    )
    if null_keys:
        raise RuntimeError(f"Ledger copy has {null_keys} NULL token IDs")

    mismatched_rows = _scalar_int(
        conn,
        f"""
        SELECT COUNT(*)
        FROM (
            (SELECT {_COLUMN_LIST} FROM {_QUALIFIED_TABLE}
             EXCEPT ALL
             SELECT {_COLUMN_LIST} FROM {_QUALIFIED_REBUILD_TABLE})
            UNION ALL
            (SELECT {_COLUMN_LIST} FROM {_QUALIFIED_REBUILD_TABLE}
             EXCEPT ALL
             SELECT {_COLUMN_LIST} FROM {_QUALIFIED_TABLE})
        ) differences
        """,
    )
    if mismatched_rows:
        raise RuntimeError(f"Ledger copy differs from source in {mismatched_rows} rows")


def repair_token_sync_ledger(conn) -> dict[str, int]:
    """Transactionally rebuild the ledger and return row/index counts."""
    _require_supported_duckdb()
    _validate_schema(conn, _TABLE)
    source_rows = _scalar_int(conn, f"SELECT COUNT(*) FROM {_QUALIFIED_TABLE}")
    old_index_count = _scalar_int(
        conn,
        f"""
        SELECT COUNT(*)
        FROM duckdb_indexes()
        WHERE schema_name = '{_SCHEMA}' AND table_name = '{_TABLE}'
        """,
    )

    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(f"DROP TABLE IF EXISTS {_QUALIFIED_REBUILD_TABLE}")
        conn.execute(
            f"""
            CREATE TABLE {_QUALIFIED_REBUILD_TABLE} (
                clobTokenId TEXT PRIMARY KEY,
                last_sync_timestamp BIGINT,
                fully_checked BOOLEAN DEFAULT FALSE,
                last_checked_at TIMESTAMP,
                next_check_at TIMESTAMP,
                empty_run_streak INTEGER DEFAULT 0
            )
            """
        )
        conn.execute(
            f"""
            INSERT INTO {_QUALIFIED_REBUILD_TABLE} ({_COLUMN_LIST})
            SELECT {_COLUMN_LIST}
            FROM {_QUALIFIED_TABLE}
            """
        )
        _validate_schema(conn, _REBUILD_TABLE)
        _verify_copy(conn, source_rows)
        conn.execute(f"DROP TABLE {_QUALIFIED_TABLE}")
        conn.execute(f"ALTER TABLE {_QUALIFIED_REBUILD_TABLE} RENAME TO {_TABLE}")
        _validate_schema(conn, _TABLE)
        final_rows = _scalar_int(conn, f"SELECT COUNT(*) FROM {_QUALIFIED_TABLE}")
        if final_rows != source_rows:
            raise RuntimeError(
                f"Ledger swap count mismatch: source={source_rows}, final={final_rows}"
            )
        new_index_count = _scalar_int(
            conn,
            f"""
            SELECT COUNT(*)
            FROM duckdb_indexes()
            WHERE schema_name = '{_SCHEMA}' AND table_name = '{_TABLE}'
            """,
        )
        if new_index_count:
            raise RuntimeError(
                f"Ledger rebuild unexpectedly retained {new_index_count} secondary indexes"
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return {
        "rows": source_rows,
        "removed_secondary_indexes": old_index_count,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duckdb-path",
        type=Path,
        default=None,
        help="DuckDB file (default: DUCKDB_PATH from settings / .env)",
    )
    args = parser.parse_args()
    duckdb_path = Path(args.duckdb_path or settings.DUCKDB_PATH).resolve()
    if not duckdb_path.exists():
        parser.error(f"DuckDB file does not exist: {duckdb_path}")

    with duckdb.connect(str(duckdb_path)) as conn:
        summary = repair_token_sync_ledger(conn)
        conn.execute("CHECKPOINT")

    print(
        "Repaired polymarket_ops.token_sync_ledger in "
        f"{duckdb_path} (rows={summary['rows']}, "
        f"removed_secondary_indexes={summary['removed_secondary_indexes']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
