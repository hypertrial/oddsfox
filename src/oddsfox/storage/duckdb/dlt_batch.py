"""dlt batch landing helpers for DuckDB canonical table finalizers."""

from __future__ import annotations

from collections.abc import Sequence
from hashlib import blake2b
from typing import Any

import dlt
import duckdb

from oddsfox.storage.duckdb import connection as duckdb_connection
from oddsfox.storage.duckdb.schemas.constants import (
    POLYMARKET_OPS_SCHEMA,
    POLYMARKET_RAW_SCHEMA,
    polymarket_ops_tbl,
    polymarket_q,
    polymarket_raw_tbl,
)

DLT_STRICT_SCHEMA_CONTRACT = {
    "tables": "evolve",
    "columns": "freeze",
    "data_type": "freeze",
}

_TAB_MARKET_TOKENS = polymarket_raw_tbl("market_tokens")
_TAB_ODDS_HISTORY = polymarket_raw_tbl("odds_history")
_TAB_PIPELINE_RUN_EVENTS = polymarket_ops_tbl("pipeline_run_events")
_TAB_WC2026_MARKET_REGISTRY = polymarket_ops_tbl("wc2026_market_registry")

_PIPELINES: dict[tuple[str, str], dlt.Pipeline] = {}

MARKET_TOKEN_COLUMNS = {
    "market_id": {"data_type": "text"},
    "clobTokenIds": {"data_type": "text"},
    "updated_at": {"data_type": "timestamp"},
    "row_order": {"data_type": "bigint"},
}

ODDS_HISTORY_COLUMNS = {
    "clobTokenId": {"data_type": "text"},
    "timestamp": {"data_type": "bigint"},
    "price": {"data_type": "double"},
    "ingested_at": {"data_type": "timestamp"},
    "row_order": {"data_type": "bigint"},
}

PIPELINE_RUN_EVENT_COLUMNS = {
    "run_id": {"data_type": "text"},
    "task_name": {"data_type": "text"},
    "recorded_at": {"data_type": "timestamp"},
    "metrics_json": {"data_type": "text"},
}

WC2026_REGISTRY_COLUMNS = {
    "market_id": {"data_type": "text"},
    "event_slug": {"data_type": "text", "nullable": True},
    "event_id": {"data_type": "text", "nullable": True},
    "source": {"data_type": "text"},
    "refreshed_at": {"data_type": "timestamp"},
    "row_order": {"data_type": "bigint"},
}


def reset_dlt_batch_pipelines() -> None:
    """Clear cached pipelines; useful when tests swap DUCKDB_NAME."""
    _PIPELINES.clear()


def _pipeline(schema: str) -> dlt.Pipeline:
    duckdb_connection.ensure_duck_db()
    db_path = str(duckdb_connection.active_duckdb_path())
    key = (schema, db_path)
    if key not in _PIPELINES:
        path_hash = blake2b(db_path.encode("utf-8"), digest_size=4).hexdigest()
        _PIPELINES[key] = dlt.pipeline(
            pipeline_name=f"polymarket_{schema}_batch_v1_{path_hash}",
            destination=dlt.destinations.duckdb(credentials=db_path),
            dataset_name=schema,
        )
    return _PIPELINES[key]


def load_stage_rows(
    *,
    schema: str,
    stage_table: str,
    rows: Sequence[dict[str, Any]],
    columns: dict[str, dict[str, Any]],
) -> str:
    """Replace a dlt stage table and return its fully qualified DuckDB name."""
    if not rows:
        raise ValueError("rows must not be empty")
    pipe = _pipeline(schema)
    if pipe.has_pending_data:
        pipe.drop_pending_packages()
    pipe.run(
        list(rows),
        table_name=stage_table,
        write_disposition="replace",
        columns=columns,
        schema_contract=DLT_STRICT_SCHEMA_CONTRACT,
    )
    return polymarket_q(schema, stage_table)


def _with_row_order(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{**row, "row_order": idx} for idx, row in enumerate(rows)]


def load_market_tokens_stage(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    stage = load_stage_rows(
        schema=POLYMARKET_RAW_SCHEMA,
        stage_table="stage_market_tokens_v1",
        rows=_with_row_order(rows),
        columns=MARKET_TOKEN_COLUMNS,
    )
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {_TAB_MARKET_TOKENS}
        (market_id, clobTokenIds, updated_at)
        SELECT market_id, clob_token_ids, updated_at
        FROM (
            SELECT
                market_id,
                clob_token_ids,
                updated_at,
                row_number() OVER (
                    PARTITION BY market_id
                    ORDER BY updated_at DESC, row_order DESC
                ) AS rn
            FROM {stage}
        )
        WHERE rn = 1
        """
    )


def load_odds_history_stage(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    stage = prepare_odds_history_stage(rows)
    merge_odds_history_stage(conn, stage)


def prepare_odds_history_stage(rows: Sequence[dict[str, Any]]) -> str:
    """Load odds rows into a dlt stage table; call before ``BEGIN`` on ``conn``."""
    return load_stage_rows(
        schema=POLYMARKET_RAW_SCHEMA,
        stage_table="stage_odds_history_v1",
        rows=_with_row_order(rows),
        columns=ODDS_HISTORY_COLUMNS,
    )


def merge_odds_history_stage(conn: duckdb.DuckDBPyConnection, stage: str) -> None:
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {_TAB_ODDS_HISTORY}
        (clobTokenId, timestamp, price, ingested_at)
        SELECT clob_token_id, timestamp, price, ingested_at
        FROM (
            SELECT
                clob_token_id,
                timestamp,
                price,
                ingested_at,
                row_number() OVER (
                    PARTITION BY clob_token_id, timestamp
                    ORDER BY ingested_at DESC, row_order DESC
                ) AS rn
            FROM {stage}
        )
        WHERE rn = 1
        """
    )


def append_pipeline_run_event_stage(
    row: dict[str, Any],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    stage = load_stage_rows(
        schema=POLYMARKET_OPS_SCHEMA,
        stage_table="stage_pipeline_run_events_v1",
        rows=[row],
        columns=PIPELINE_RUN_EVENT_COLUMNS,
    )
    conn.execute(
        f"""
        INSERT INTO {_TAB_PIPELINE_RUN_EVENTS}
        (run_id, task_name, recorded_at, metrics_json)
        SELECT run_id, task_name, recorded_at, metrics_json
        FROM {stage}
        """
    )


def load_wc2026_registry_stage(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    stage = load_stage_rows(
        schema=POLYMARKET_OPS_SCHEMA,
        stage_table="stage_wc2026_market_registry_v1",
        rows=_with_row_order(rows),
        columns=WC2026_REGISTRY_COLUMNS,
    )
    conn.execute(
        f"""
        INSERT INTO {_TAB_WC2026_MARKET_REGISTRY}
        (market_id, event_slug, event_id, source, refreshed_at)
        SELECT market_id, event_slug, event_id, source, refreshed_at
        FROM (
            SELECT
                market_id,
                event_slug,
                event_id,
                source,
                refreshed_at,
                row_number() OVER (
                    PARTITION BY market_id
                    ORDER BY refreshed_at DESC, row_order DESC
                ) AS rn
            FROM {stage}
        )
        WHERE rn = 1
        ON CONFLICT(market_id) DO UPDATE SET
          event_slug=COALESCE(
              excluded.event_slug,
              {_TAB_WC2026_MARKET_REGISTRY}.event_slug
          ),
          event_id=COALESCE(
              excluded.event_id,
              {_TAB_WC2026_MARKET_REGISTRY}.event_id
          ),
          source=excluded.source,
          refreshed_at=excluded.refreshed_at
        """
    )


__all__ = [
    "DLT_STRICT_SCHEMA_CONTRACT",
    "MARKET_TOKEN_COLUMNS",
    "ODDS_HISTORY_COLUMNS",
    "PIPELINE_RUN_EVENT_COLUMNS",
    "WC2026_REGISTRY_COLUMNS",
    "append_pipeline_run_event_stage",
    "load_market_tokens_stage",
    "load_odds_history_stage",
    "load_stage_rows",
    "load_wc2026_registry_stage",
    "merge_odds_history_stage",
    "prepare_odds_history_stage",
    "reset_dlt_batch_pipelines",
]
