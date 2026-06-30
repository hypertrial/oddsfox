"""Read-only DuckDB snapshots for Dagster metadata and logs."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import duckdb

from oddsfox.storage.duckdb.connection import (
    POLYMARKET_OPS_SCHEMA,
    POLYMARKET_RAW_SCHEMA,
    get_connection,
    polymarket_ops_tbl,
    polymarket_q,
    polymarket_raw_tbl,
)
from oddsfox.storage.duckdb.schemas.dbt_schemas import (
    POLYMARKET_INTERMEDIATE_SCHEMA,
    POLYMARKET_MARTS_SCHEMA,
    POLYMARKET_OBSERVABILITY_SCHEMA,
    POLYMARKET_STAGING_SCHEMA,
)

logger = logging.getLogger(__name__)

_POLY_RAW_OPS_TABLES: tuple[tuple[str, str], ...] = (
    (POLYMARKET_RAW_SCHEMA, "markets"),
    (POLYMARKET_RAW_SCHEMA, "market_tokens"),
    (POLYMARKET_RAW_SCHEMA, "odds_history"),
    (POLYMARKET_RAW_SCHEMA, "token_odds_daily"),
    (POLYMARKET_OPS_SCHEMA, "wc2026_market_registry"),
    (POLYMARKET_OPS_SCHEMA, "token_sync_ledger"),
    (POLYMARKET_OPS_SCHEMA, "token_sync_skips"),
    (POLYMARKET_OPS_SCHEMA, "pipeline_run_events"),
    (POLYMARKET_OPS_SCHEMA, "sync_run_metrics"),
)
_RAW_TABLES_POLY: tuple[str, ...] = tuple(t for _, t in _POLY_RAW_OPS_TABLES)

_DBT_MODELS: tuple[tuple[str, str], ...] = (
    (POLYMARKET_STAGING_SCHEMA, "stg_polymarket_markets"),
    (POLYMARKET_STAGING_SCHEMA, "stg_polymarket_market_tokens"),
    (POLYMARKET_STAGING_SCHEMA, "stg_polymarket_odds"),
    (POLYMARKET_STAGING_SCHEMA, "stg_polymarket_odds_daily"),
    (POLYMARKET_STAGING_SCHEMA, "stg_polymarket_pipeline_run_events"),
    (POLYMARKET_STAGING_SCHEMA, "stg_polymarket_sync_ledger"),
    (POLYMARKET_STAGING_SCHEMA, "stg_polymarket_token_sync_skips"),
    (POLYMARKET_INTERMEDIATE_SCHEMA, "int_polymarket_wc2026_markets"),
    (POLYMARKET_INTERMEDIATE_SCHEMA, "int_polymarket_token_universe"),
    (POLYMARKET_INTERMEDIATE_SCHEMA, "int_polymarket_wc2026_token_universe"),
    (POLYMARKET_INTERMEDIATE_SCHEMA, "int_polymarket_token_timeseries"),
    (POLYMARKET_INTERMEDIATE_SCHEMA, "int_polymarket_token_daily_timeseries"),
    (POLYMARKET_MARTS_SCHEMA, "market_coverage"),
    (POLYMARKET_MARTS_SCHEMA, "wc2026_markets"),
    (POLYMARKET_MARTS_SCHEMA, "token_coverage"),
    (POLYMARKET_MARTS_SCHEMA, "token_latest_odds"),
    (POLYMARKET_MARTS_SCHEMA, "wc2026_whale_minutely_odds"),
    (POLYMARKET_OBSERVABILITY_SCHEMA, "sync_run_observability"),
)

_TAB_MT = polymarket_raw_tbl("market_tokens")
_TAB_OH = polymarket_raw_tbl("odds_history")
_TAB_TOD = polymarket_raw_tbl("token_odds_daily")
_TAB_LED = polymarket_ops_tbl("token_sync_ledger")
_TAB_SKP = polymarket_ops_tbl("token_sync_skips")

_MARKET_TOKEN_IDS_CTE = f"""
WITH market_token_ids AS (
    SELECT DISTINCT json_extract_string(je.value, '$') AS token_id
    FROM {_TAB_MT} mt
    CROSS JOIN LATERAL json_each(mt.clobTokenIds) AS je
    WHERE mt.clobTokenIds IS NOT NULL
      AND mt.clobTokenIds != '[]'
      AND LEFT(LTRIM(mt.clobTokenIds), 1) = '['
      AND json_extract_string(je.value, '$') IS NOT NULL
)
"""


def _scalar_int(conn, sql: str) -> int | None:
    try:
        row = conn.execute(sql).fetchone()
        if row is None or row[0] is None:
            return None
        return int(row[0])
    except duckdb.Error:
        return None
    except (TypeError, ValueError) as exc:
        logger.warning("unexpected value in _scalar_int: %s", exc)
        return None


def _table_row_count(conn, table: str) -> tuple[bool, int | None]:
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        if row is None or row[0] is None:
            return True, 0
        return True, int(row[0])
    except duckdb.Error:
        return False, None
    except (TypeError, ValueError) as exc:
        logger.warning("unexpected value in _table_row_count for %s: %s", table, exc)
        return False, None


def _dict_rows(conn, sql: str) -> dict[str, int] | None:
    try:
        rows = conn.execute(sql).fetchall()
    except duckdb.Error:
        return None
    except Exception as exc:
        logger.warning("unexpected error in _dict_rows: %s", exc)
        return None
    out: dict[str, int] = {}
    for key, value in rows:
        if key is None or value is None:
            continue
        out[str(key)] = int(value)
    return out


def _normalize_dt(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    text = str(value).strip()
    return text or None


def snapshot_raw_layer(conn=None, *, level: str = "full") -> dict[str, Any]:
    """Aggregate row counts and freshness markers for raw + operational tables."""
    snapshot_level = str(level or "full").strip().lower()
    if snapshot_level not in ("basic", "full"):
        raise ValueError("snapshot_raw_layer level must be 'basic' or 'full'")
    out: dict[str, Any] = {}

    def _fill(c) -> None:
        for schema, table in _POLY_RAW_OPS_TABLES:
            exists, n = _table_row_count(c, polymarket_q(schema, table))
            out[f"{table}_rows"] = n
            out[f"{table}_missing"] = not exists

        if snapshot_level == "basic":
            return

        out["market_tokens_distinct_tokens"] = _scalar_int(
            c,
            _MARKET_TOKEN_IDS_CTE + "SELECT COUNT(*) FROM market_token_ids",
        )
        out["ledger_distinct_tokens"] = _scalar_int(
            c,
            f"SELECT COUNT(DISTINCT clobTokenId) FROM {_TAB_LED}",
        )
        out["token_sync_skips_distinct_tokens"] = _scalar_int(
            c,
            f"SELECT COUNT(DISTINCT clobTokenId) FROM {_TAB_SKP}",
        )
        out["odds_history_distinct_tokens"] = _scalar_int(
            c,
            f"SELECT COUNT(DISTINCT clobTokenId) FROM {_TAB_OH}",
        )
        out["odds_history_max_ts"] = _normalize_dt(
            c.execute(f"SELECT MAX(timestamp) FROM {_TAB_OH}").fetchone()[0]
        )
        out["token_odds_daily_distinct_tokens"] = _scalar_int(
            c,
            f"SELECT COUNT(DISTINCT clobTokenId) FROM {_TAB_TOD}",
        )
        out["ledger_fully_checked_tokens"] = _scalar_int(
            c,
            f"SELECT COUNT(*) FROM {_TAB_LED} WHERE fully_checked = TRUE",
        )
        out["market_tokens_without_history"] = _scalar_int(
            c,
            _MARKET_TOKEN_IDS_CTE
            + f"""
            SELECT COUNT(*)
            FROM market_token_ids m
            LEFT JOIN (
                SELECT DISTINCT clobTokenId AS token_id
                FROM {_TAB_OH}
                WHERE clobTokenId IS NOT NULL
            ) h ON h.token_id = m.token_id
            WHERE h.token_id IS NULL
            """,
        )
        out["history_tokens_without_market_tokens"] = _scalar_int(
            c,
            _MARKET_TOKEN_IDS_CTE
            + f"""
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT clobTokenId AS token_id
                FROM {_TAB_OH}
                WHERE clobTokenId IS NOT NULL
            ) h
            LEFT JOIN market_token_ids m ON m.token_id = h.token_id
            WHERE m.token_id IS NULL
            """,
        )
        out["token_sync_skips_by_reason"] = _dict_rows(
            c,
            f"""
            SELECT COALESCE(reason, 'unknown') AS reason, COUNT(*) AS token_count
            FROM {_TAB_SKP}
            GROUP BY 1
            ORDER BY token_count DESC, reason ASC
            """,
        )

    if conn is not None:
        _fill(conn)
    else:
        with get_connection() as c:
            _fill(c)
    return out


def delta_raw_layer(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """Return only keys whose values changed between two snapshots."""
    delta: dict[str, Any] = {}
    for key in set(before) | set(after):
        if key.endswith("_missing"):
            continue
        if before.get(key) != after.get(key):
            delta[key] = {"before": before.get(key), "after": after.get(key)}
    return delta


def _qualified(schema: str, name: str) -> str:
    return f'"{schema}"."{name}"'


def snapshot_dbt_models(conn=None) -> dict[str, Any]:
    """Return row counts for modeled Polymarket dbt relations if they exist."""
    out: dict[str, Any] = {}

    def _fill(c) -> None:
        for schema, model in _DBT_MODELS:
            key = f"{schema}.{model}"
            try:
                row = c.execute(
                    f"SELECT COUNT(*) FROM {_qualified(schema, model)}"
                ).fetchone()
                out[key] = {
                    "exists": True,
                    "rows": int(row[0]) if row and row[0] is not None else 0,
                }
            except duckdb.Error:
                out[key] = {"exists": False, "rows": None}
            except (TypeError, ValueError) as exc:
                logger.warning("unexpected error counting dbt model %s: %s", key, exc)
                out[key] = {"exists": False, "rows": None}

    if conn is not None:
        _fill(conn)
    else:
        with get_connection() as c:
            _fill(c)
    return out


def delta_dbt_models(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """Return dbt model keys whose exists/rows changed."""
    delta: dict[str, Any] = {}
    for key in set(before) | set(after):
        if before.get(key) != after.get(key):
            delta[key] = {"before": before.get(key), "after": after.get(key)}
    return delta


def format_raw_snapshot_log(snapshot: dict[str, Any]) -> str:
    """Single-line key=value summary for raw layer operator logs."""
    parts: list[str] = []
    for table in _RAW_TABLES_POLY:
        parts.append(f"{table}={snapshot.get(f'{table}_rows')}")
    for extra in (
        "market_tokens_distinct_tokens",
        "odds_history_distinct_tokens",
        "token_odds_daily_distinct_tokens",
        "ledger_distinct_tokens",
        "odds_history_max_ts",
        "ledger_fully_checked_tokens",
        "market_tokens_without_history",
        "history_tokens_without_market_tokens",
        "token_sync_skips_distinct_tokens",
    ):
        if extra in snapshot:
            parts.append(f"{extra}={snapshot[extra]}")
    skip_reasons = snapshot.get("token_sync_skips_by_reason")
    if isinstance(skip_reasons, dict):
        rendered = ",".join(
            f"{reason}:{count}" for reason, count in skip_reasons.items()
        )
        parts.append(f"token_sync_skips_by_reason={{{rendered}}}")
    return " ".join(parts)


def format_dbt_snapshot_log(snapshot: dict[str, Any]) -> str:
    """Single-line summary for dbt model row counts."""
    parts: list[str] = []
    for key in sorted(snapshot):
        value = snapshot[key]
        if isinstance(value, dict):
            parts.append(f"{key}:exists={value.get('exists')},rows={value.get('rows')}")
        else:
            parts.append(f"{key}={value}")
    return "; ".join(parts)


__all__ = [
    "snapshot_raw_layer",
    "delta_raw_layer",
    "snapshot_dbt_models",
    "delta_dbt_models",
    "format_raw_snapshot_log",
    "format_dbt_snapshot_log",
]
