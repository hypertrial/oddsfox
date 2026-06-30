"""Row counts and per-column statistics for warehouse profiling."""

from __future__ import annotations

from typing import Optional, Tuple

import duckdb

from .discovery import (
    _classify_warehouse_type,
    fetch_column_specs,
    qualified_name,
)
from .models import (
    _PROFILE_QUERY_ERRORS,
    ColumnProfile,
    ColumnSpec,
    ColumnStats,
    RelationInfo,
    RelationProfile,
    StatsLevel,
)


def _row_count(
    conn: duckdb.DuckDBPyConnection, qname: str
) -> Tuple[Optional[int], Optional[str]]:
    try:
        r = conn.execute(f"SELECT COUNT(*) FROM {qname}").fetchone()
        if r is None or r[0] is None:
            return 0, None
        return int(r[0]), None
    except _PROFILE_QUERY_ERRORS as e:
        return None, str(e)


def _from_clause(qname: str, sample_rows: Optional[int]) -> str:
    if sample_rows is None or sample_rows < 1:
        return qname
    n = int(sample_rows)
    return f"(SELECT * FROM {qname} LIMIT {n}) AS _sample"


def _collect_column_stats(
    conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    col: ColumnSpec,
    *,
    stats_level: StatsLevel,
    from_sql: str,
) -> ColumnStats:
    from .discovery import _quote_ident

    cname = _quote_ident(col.name)
    st = ColumnStats()
    if stats_level == StatsLevel.quick:
        return st

    kind = _classify_warehouse_type(col.data_type)

    try:
        n_non = conn.execute(f"SELECT COUNT({cname}) FROM {from_sql}").fetchone()
        n_total = conn.execute(f"SELECT COUNT(*) FROM {from_sql}").fetchone()
        n_non_i = int(n_non[0]) if n_non and n_non[0] is not None else 0
        n_total_i = int(n_total[0]) if n_total and n_total[0] is not None else 0
        st.non_null_count = n_non_i
        st.null_count = max(0, n_total_i - n_non_i)
        if n_total_i > 0:
            st.null_percent = round(100.0 * (n_total_i - n_non_i) / n_total_i, 3)
    except _PROFILE_QUERY_ERRORS as e:
        st.error = str(e)
        return st

    if (  # pragma: no branch
        stats_level == StatsLevel.standard or stats_level == StatsLevel.full
    ):
        try:
            r = conn.execute(
                f"SELECT approx_count_distinct({cname}) FROM {from_sql}"
            ).fetchone()
            if r and r[0] is not None:
                st.approx_distinct = int(r[0])
        except _PROFILE_QUERY_ERRORS as e:
            st.error = (st.error or "") + f"; approx_distinct: {e}"
        if kind == "numeric":
            _fill_numeric_aggregates(
                conn, from_sql, cname, st, full=stats_level == StatsLevel.full
            )
        elif kind == "temporal":
            _fill_temporal_minmax(conn, from_sql, cname, st)
        elif kind == "boolean":
            _fill_bool_counts(conn, from_sql, cname, st)
        elif kind == "text" and stats_level == StatsLevel.full:
            _fill_text_stats(conn, from_sql, cname, st)

    return st


def _fill_numeric_aggregates(
    conn: duckdb.DuckDBPyConnection,
    from_sql: str,
    cname: str,
    st: ColumnStats,
    *,
    full: bool,
) -> None:
    try:
        row = conn.execute(
            f"SELECT min({cname}), max({cname}), avg({cname}) FROM {from_sql}"
        ).fetchone()
        if row and len(row) >= 3:
            st.min_value, st.max_value, st.avg_value = row[0], row[1], row[2]
        if full:
            r2 = conn.execute(f"SELECT stddev_samp({cname}) FROM {from_sql}").fetchone()
            if r2 and r2[0] is not None:
                st.stddev_value = r2[0]
    except _PROFILE_QUERY_ERRORS as e:
        st.error = (st.error or "") + f"; num_agg: {e}"


def _fill_temporal_minmax(
    conn: duckdb.DuckDBPyConnection, from_sql: str, cname: str, st: ColumnStats
) -> None:
    try:
        row = conn.execute(
            f"SELECT min({cname})::VARCHAR, max({cname})::VARCHAR FROM {from_sql}"
        ).fetchone()
        if row and len(row) >= 2:
            st.min_value, st.max_value = row[0], row[1]
    except _PROFILE_QUERY_ERRORS as e:  # pragma: no cover
        st.error = (st.error or "") + f"; temporal: {e}"


def _fill_bool_counts(
    conn: duckdb.DuckDBPyConnection, from_sql: str, cname: str, st: ColumnStats
) -> None:
    try:
        row = conn.execute(
            f"SELECT count_if({cname} = TRUE), count_if({cname} = FALSE) FROM {from_sql}"
        ).fetchone()
        if row and len(row) >= 2 and row[0] is not None and row[1] is not None:
            st.true_count, st.false_count = int(row[0]), int(row[1])
    except _PROFILE_QUERY_ERRORS as e:  # pragma: no cover
        st.error = (st.error or "") + f"; bool: {e}"


def _fill_text_stats(
    conn: duckdb.DuckDBPyConnection, from_sql: str, cname: str, st: ColumnStats
) -> None:
    try:
        row = conn.execute(
            f"""
            SELECT
              min(char_length(CAST({cname} AS VARCHAR))),
              max(char_length(CAST({cname} AS VARCHAR))),
              avg(char_length(CAST({cname} AS VARCHAR))::DOUBLE)
            FROM {from_sql}
            """
        ).fetchone()
        if row and len(row) >= 3:  # pragma: no branch
            if row[0] is not None:  # pragma: no branch
                st.min_len = int(row[0])
            if row[1] is not None:  # pragma: no branch
                st.max_len = int(row[1])
            if row[2] is not None:  # pragma: no branch
                st.avg_len = float(row[2])
    except _PROFILE_QUERY_ERRORS as e:
        st.error = (st.error or "") + f"; text_len: {e}"
    try:  # pragma: no branch
        srow = conn.execute(
            f"SELECT {cname} FROM {from_sql} WHERE {cname} IS NOT NULL LIMIT 1"
        ).fetchone()
        if srow and srow[0] is not None:  # pragma: no branch
            s = str(srow[0])
            st.sample_value = s if len(s) <= 200 else s[:200] + "…"
    except _PROFILE_QUERY_ERRORS as e:
        st.error = (st.error or "") + f"; sample: {e}"


def profile_relation(
    conn: duckdb.DuckDBPyConnection,
    info: RelationInfo,
    *,
    stats_level: StatsLevel = StatsLevel.standard,
    sample_rows: Optional[int] = None,
    max_columns: Optional[int] = None,
) -> RelationProfile:
    qn = qualified_name(info.table_schema, info.table_name)
    from_sql = _from_clause(qn, sample_rows)
    prof = RelationProfile(
        table_schema=info.table_schema,
        table_name=info.table_name,
        table_type=info.table_type,
    )
    rc, err = _row_count(conn, qn)
    prof.row_count = rc
    if err is not None:
        prof.error = err
        return prof
    if rc is not None:  # pragma: no branch
        prof.is_empty = rc == 0
    try:
        specs = fetch_column_specs(conn, info.table_schema, info.table_name)
    except _PROFILE_QUERY_ERRORS as e:
        prof.error = f"columns: {e}"
        return prof
    if max_columns is not None and max_columns > 0:
        specs = specs[: int(max_columns)]
    prof.column_count = len(specs)
    for c in specs:
        cstats: Optional[ColumnStats] = None
        if stats_level != StatsLevel.quick:
            cstats = _collect_column_stats(
                conn,
                info.table_schema,
                info.table_name,
                c,
                stats_level=stats_level,
                from_sql=from_sql,
            )
        prof.columns.append(ColumnProfile(spec=c, stats=cstats))
    return prof
