"""DuckDB schema names and qualified table helpers."""

from __future__ import annotations

POLYMARKET_RAW_SCHEMA = "polymarket_raw"
POLYMARKET_OPS_SCHEMA = "polymarket_ops"


def polymarket_q(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'


def polymarket_raw_tbl(name: str) -> str:
    return polymarket_q(POLYMARKET_RAW_SCHEMA, name)


def polymarket_ops_tbl(name: str) -> str:
    return polymarket_q(POLYMARKET_OPS_SCHEMA, name)


__all__ = [
    "POLYMARKET_OPS_SCHEMA",
    "POLYMARKET_RAW_SCHEMA",
    "polymarket_ops_tbl",
    "polymarket_q",
    "polymarket_raw_tbl",
]
