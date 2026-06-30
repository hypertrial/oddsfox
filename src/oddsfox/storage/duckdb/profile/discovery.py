"""Catalog discovery and column metadata for warehouse profiling."""

from __future__ import annotations

import re
from typing import Optional, Set

import duckdb

from .models import _SYSTEM_SCHEMAS, ColumnSpec, RelationInfo


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def qualified_name(schema: str, rel: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(rel)}"


def _is_system_schema(s: str) -> bool:
    if s in _SYSTEM_SCHEMAS:
        return True
    if s.startswith("duckdb_"):
        return True
    return False


_NUM_RE = re.compile(
    r"^(TINY|SMALL|BIG|HUGE)?INT(EGER)?$|^INTEGER$|^UTINYINT$|^"
    r"(DOUBLE|FLOAT|REAL|DECIMAL|NUMERIC)|^UHUGEINT$",
    re.I,
)
_DATE_RE = re.compile(r"^(DATE|TIME|TIMESTAMP|TIMESTAMPTZ|INTERVAL)", re.I)


def _classify_warehouse_type(data_type: str) -> str:
    t = (data_type or "").strip()
    t_upper = t.upper()
    if t_upper in ("BOOLEAN", "BOOL"):
        return "boolean"
    if _NUM_RE.search(t):
        return "numeric"
    if _DATE_RE.search(t):
        return "temporal"
    if t_upper.startswith("VARCHAR") or t_upper in (
        "BLOB",
        "CHAR",
        "UUID",
        "TEXT",
        "JSON",
    ):
        return "text"
    if "[]" in t or t_upper == "MAP" or t_upper == "UNION" or t_upper == "STRUCT":
        return "other"
    return "other"


def _fetch_relations(
    conn: duckdb.DuckDBPyConnection,
    *,
    schema_whitelist: Set[str],
    exclude: Set[str],
    include_views: bool,
) -> list[RelationInfo]:
    types = ["BASE TABLE"]
    if include_views:
        types.append("VIEW")
    type_placeholders = ", ".join(["?" for _ in types])
    q = f"""
    SELECT table_schema, table_name, table_type
    FROM information_schema.tables
    WHERE table_type IN ({type_placeholders})
    ORDER BY table_schema, table_name
    """
    rows = conn.execute(q, types).fetchall()
    out: list[RelationInfo] = []
    for sch, tname, ttype in rows:
        if not isinstance(sch, str) or not isinstance(tname, str) or ttype is None:
            continue
        if _is_system_schema(sch):
            continue
        if sch in exclude:
            continue
        if sch not in schema_whitelist:
            continue
        out.append(
            RelationInfo(table_schema=sch, table_name=tname, table_type=str(ttype))
        )
    return out


def discover_relations(
    conn: duckdb.DuckDBPyConnection,
    *,
    schema_whitelist: Set[str],
    exclude_schemas: Optional[Set[str]] = None,
    include_views: bool = True,
) -> list[RelationInfo]:
    """Return user relations visible in information_schema, filtered by allowlist."""
    ex = set(exclude_schemas or ())
    return _fetch_relations(
        conn,
        schema_whitelist=schema_whitelist,
        exclude=ex,
        include_views=include_views,
    )


def fetch_column_specs(
    conn: duckdb.DuckDBPyConnection, schema: str, table: str
) -> list[ColumnSpec]:
    rows = conn.execute(
        """
        SELECT column_name, ordinal_position, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
        """,
        [schema, table],
    ).fetchall()
    specs: list[ColumnSpec] = []
    for r in rows:
        name, pos, dtype, nul, default = (
            r[0],
            int(r[1]) if r[1] is not None else 0,
            r[2],
            r[3],
            r[4],
        )
        specs.append(
            ColumnSpec(
                name=str(name),
                ordinal_position=pos,
                data_type=str(dtype) if dtype is not None else "UNKNOWN",
                is_nullable=str(nul) if nul is not None else "YES",
                column_default=str(default) if default is not None else None,
            )
        )
    return specs
