import logging
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Tuple

from oddsfox.ingestion.polymarket.scope_sql import (
    MARKET_SCOPE_WC2026,
    market_scope_predicate_sql,
    market_scope_sql,
    validate_market_scope,
)
from oddsfox.storage.duckdb.connection import (
    ensure_duck_db,
    get_connection,
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)

logger = logging.getLogger(__name__)

_MARKETS_TABLE_MISSING_MSG = (
    "polymarket_raw.markets does not exist. Materialize the dlt_polymarket_markets "
    "Dagster asset first (or use the test helper create_test_markets_table)."
)

_TAB_MARKETS = polymarket_raw_tbl("markets")
_TAB_MARKET_TOKENS = polymarket_raw_tbl("market_tokens")
_TAB_TOKEN_SYNC_LEDGER = polymarket_ops_tbl("token_sync_ledger")
_TAB_TOKEN_SYNC_SKIPS = polymarket_ops_tbl("token_sync_skips")
_TAB_MARKET_METADATA_UNRESOLVED = polymarket_ops_tbl("market_metadata_unresolved")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _fetch_market_ids(base_query: str, limit: Optional[int] = None) -> List[str]:
    query = base_query
    if limit:
        query += f" LIMIT {int(limit)}"
    with get_connection() as conn:
        rows = conn.execute(query).fetchall()
        return [row[0] for row in rows]


def _market_scope_where_clause(market_scope: str | None, alias: str = "m") -> str:
    return market_scope_sql(market_scope, alias)


def _ended_market_where_clause(
    ended_market_grace_days: int | None,
    alias: str = "m",
) -> str:
    if ended_market_grace_days is None:
        return ""
    days = max(0, int(ended_market_grace_days))
    return (
        f"AND NOT ({alias}.end_date IS NOT NULL "
        f"AND {alias}.end_date < CURRENT_TIMESTAMP - INTERVAL {days} DAY)"
    )


def _validate_volume_bound(value: float | None, *, name: str) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a number, got {value!r}") from exc
    if parsed < 0:
        raise ValueError(f"{name} must be >= 0, got {parsed}")
    return parsed


def _volume_where_clause(
    min_volume: float | None,
    max_volume: float | None,
    alias: str = "m",
) -> str:
    """Return SQL AND-clause fragments for market volume bounds (inlined literals)."""
    min_volume = _validate_volume_bound(min_volume, name="min_volume")
    max_volume = _validate_volume_bound(max_volume, name="max_volume")
    if min_volume is None and max_volume is None:
        return ""
    clauses: list[str] = []
    if min_volume is not None:
        clauses.append(f"COALESCE({alias}.volume, 0) >= {min_volume}")
    if max_volume is not None:
        clauses.append(f"COALESCE({alias}.volume, 0) < {max_volume}")
    return "AND " + " AND ".join(clauses)


def get_market_count() -> int:
    ensure_duck_db()
    with get_connection() as conn:
        result = conn.execute(f"SELECT COUNT(*) FROM {_TAB_MARKETS}").fetchone()
        return int(result[0]) if result else 0


def get_all_market_ids() -> set:
    """
    Get all existing market IDs as a set for efficient lookup.
    Used for early stopping in incremental syncs.
    """
    ensure_duck_db()
    with get_connection() as conn:
        rows = conn.execute(f"SELECT id FROM {_TAB_MARKETS}").fetchall()
        return {row[0] for row in rows}


def _markets_table_exists(conn) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'polymarket_raw' AND table_name = 'markets'
        """
    ).fetchone()
    return bool(row and row[0])


def _require_markets_table(conn) -> None:
    if not _markets_table_exists(conn):
        raise RuntimeError(_MARKETS_TABLE_MISSING_MSG)


def _persist_market_tokens(conn, token_data: Iterable[Tuple]) -> None:
    token_data = list(token_data)
    if not token_data:
        return
    now = _utc_now()
    token_rows = [(mid, toks, now) for mid, toks in token_data]
    conn.executemany(
        f"""
        INSERT OR REPLACE INTO {_TAB_MARKET_TOKENS}
        (market_id, clobTokenIds, updated_at)
        VALUES (?, ?, ?)
        """,
        token_rows,
    )


def save_market_tokens_batch(token_data: Iterable[Tuple]) -> None:
    """Persist CLOB token mappings without touching polymarket_raw.markets."""
    token_data = list(token_data)
    if not token_data:
        return
    ensure_duck_db()
    with get_connection() as conn:
        _persist_market_tokens(conn, token_data)


def save_markets_batch(market_data: Iterable[Tuple], token_data: Iterable[Tuple]):
    """
    Save a batch of markets and token mappings into DuckDB.
    Args:
        market_data: sequence matching markets table structure
        token_data: sequence of (market_id, token_json)
    """
    market_data = list(market_data)
    token_data = list(token_data)
    if not market_data:
        return

    # Normalize tuples so they always match the markets schema (14 columns with event_id).
    # Older callers (and some tests) still pass 10-13 tuples without newer metadata cols.
    normalized_market_data: List[Tuple] = []
    for row in market_data:
        if len(row) == 14:
            normalized_market_data.append(row)
        elif len(row) == 13:
            normalized_market_data.append((*row, None))
        elif len(row) == 12:
            expanded = list(row)
            expanded.insert(10, None)
            normalized_market_data.append((*expanded, None))
        elif len(row) == 11:
            expanded = list(row)
            expanded.insert(10, None)
            normalized_market_data.append((*expanded, None, None))
        elif len(row) == 10:
            normalized_market_data.append((*row, None, None, None, None))
        else:
            raise ValueError(
                f"Expected 10-14 columns for markets insert, got {len(row)}"
            )

    # Normalize empty-string end_date to None to satisfy DuckDB timestamp parsing.
    def _sanitize_end_date(rec: Tuple) -> Tuple:
        rec_list = list(rec)
        end_val = rec_list[10]
        if not end_val or (isinstance(end_val, str) and end_val.strip() == ""):
            rec_list[10] = None
        return tuple(rec_list)

    normalized_market_data = [_sanitize_end_date(r) for r in normalized_market_data]

    ensure_duck_db()
    with get_connection() as conn:
        _require_markets_table(conn)
        conn.executemany(
            f"""
            INSERT INTO {_TAB_MARKETS}
            (id, question, category, description, outcomes, volume, active,
             closed, created_at, scraped_at, end_date, slug, event_slug, event_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              question=excluded.question,
              category=excluded.category,
              description=excluded.description,
              outcomes=excluded.outcomes,
              volume=excluded.volume,
              active=excluded.active,
              closed=excluded.closed,
              created_at=excluded.created_at,
              scraped_at=excluded.scraped_at,
             end_date=COALESCE(excluded.end_date, markets.end_date),
             slug=COALESCE(NULLIF(excluded.slug, ''), markets.slug),
             event_slug=COALESCE(NULLIF(excluded.event_slug, ''), markets.event_slug),
             event_id=COALESCE(NULLIF(excluded.event_id, ''), markets.event_id)
            """,
            normalized_market_data,
        )
        _persist_market_tokens(conn, token_data)


def get_markets_without_tokens(limit: Optional[int] = None) -> List[str]:
    ensure_duck_db()
    return _fetch_market_ids(
        f"""
        SELECT id
        FROM {_TAB_MARKETS}
        WHERE id NOT IN (SELECT market_id FROM {_TAB_MARKET_TOKENS})
    """,
        limit=limit,
    )


def get_markets_missing_any_metadata(
    *,
    include_tokens: bool = True,
    include_slugs: bool = True,
    include_event_slugs: bool = True,
    include_end_dates: bool = True,
    limit: Optional[int] = None,
    market_scope: str = "all",
) -> List[str]:
    """Return market ids missing any requested metadata field."""
    ensure_duck_db()
    predicates: list[str] = []
    if include_tokens:
        predicates.append(f"id NOT IN (SELECT market_id FROM {_TAB_MARKET_TOKENS})")
    if include_slugs:
        predicates.append("(slug IS NULL OR slug = '')")
    if include_event_slugs:
        predicates.append(
            f"""(
                (m.event_slug IS NULL OR m.event_slug = '')
                AND NOT EXISTS (
                    SELECT 1
                    FROM {_TAB_MARKET_METADATA_UNRESOLVED} u
                    WHERE u.market_id = m.id
                      AND u.field_name = 'event_slug'
                      AND u.next_retry_at > CURRENT_TIMESTAMP
                )
            )"""
        )
    if include_end_dates:
        predicates.append("(m.end_date IS NULL OR CAST(m.end_date AS VARCHAR) = '')")
    if not predicates:
        return []
    predicates = [
        p.replace("id NOT IN", "m.id NOT IN").replace("(slug IS", "(m.slug IS")
        for p in predicates
    ]
    scope_clause = _market_scope_where_clause(market_scope, "m")
    query = f"""
        SELECT m.id
        FROM {_TAB_MARKETS} m
        WHERE ({" OR ".join(predicates)})
        {scope_clause}
    """
    if limit:
        query += f" LIMIT {int(limit)}"
    with get_connection() as conn:
        rows = conn.execute(query).fetchall()
        return [str(row[0]) for row in rows]


def get_markets_with_tokens() -> List[Tuple[str, str, Optional[str], Optional[bool]]]:
    """
    Get all markets that have associated tokens, including creation date and closed status.
    """
    ensure_duck_db()
    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT mt.market_id, mt.clobTokenIds, m.created_at, m.closed
            FROM {_TAB_MARKET_TOKENS} mt
            JOIN {_TAB_MARKETS} m ON mt.market_id = m.id
            WHERE mt.clobTokenIds IS NOT NULL AND mt.clobTokenIds != '[]'
            """
        ).fetchall()
        return rows


def iter_markets_with_tokens(
    page_size: int = 5_000,
    *,
    cutoff_created_at: Optional[str] = None,
    json_array_only: bool = False,
    market_scope: str = "all",
    ended_market_grace_days: int | None = None,
    min_volume: float | None = None,
    max_volume: float | None = None,
):
    """
    Stream markets that have token mappings in bounded pages.

    This avoids loading the full market-token corpus into memory for large syncs.
    """
    ensure_duck_db()
    query_parts = [
        f"""
        SELECT mt.market_id, mt.clobTokenIds, m.created_at, m.closed
        FROM {_TAB_MARKET_TOKENS} mt
        JOIN {_TAB_MARKETS} m ON mt.market_id = m.id
        WHERE mt.clobTokenIds IS NOT NULL AND mt.clobTokenIds != '[]'
        """
    ]
    params: List = []
    if cutoff_created_at:
        query_parts.append("AND m.created_at >= ?")
        params.append(cutoff_created_at)
    if json_array_only:
        # Fast pre-filter before Python JSON decoding.
        query_parts.append("AND LEFT(LTRIM(mt.clobTokenIds), 1) = '['")
    query_parts.append(_market_scope_where_clause(market_scope, "m"))
    query_parts.append(_volume_where_clause(min_volume, max_volume, "m"))
    query_parts.append(_ended_market_where_clause(ended_market_grace_days, "m"))
    query = "\n".join(query_parts)
    with get_connection() as conn:
        cursor = conn.execute(query, params) if params else conn.execute(query)
        while True:
            rows = cursor.fetchmany(page_size)
            if not rows:
                break
            yield rows


def iter_due_market_tokens(
    page_size: int = 5_000,
    *,
    cutoff_created_at: Optional[str] = None,
    market_scope: str = "all",
    ended_market_grace_days: int | None = None,
    min_volume: float | None = None,
    max_volume: float | None = None,
):
    """
    Stream only market-token pairs that are due for routine odds syncing.

    A token is due when it has no ledger row yet or its next_check_at is NULL / in
    the past. Persisted skip reasons and fully checked closed tokens are excluded
    here so routine runs do not revisit them.
    """
    ensure_duck_db()
    query_parts = [
        f"""
        SELECT
            mt.market_id,
            json_extract_string(je.value, '$') AS clobTokenId,
            m.created_at,
            m.closed
        FROM {_TAB_MARKET_TOKENS} mt
        JOIN {_TAB_MARKETS} m ON mt.market_id = m.id
        CROSS JOIN LATERAL json_each(mt.clobTokenIds) AS je
        LEFT JOIN {_TAB_TOKEN_SYNC_LEDGER} l
          ON l.clobTokenId = json_extract_string(je.value, '$')
        LEFT JOIN {_TAB_TOKEN_SYNC_SKIPS} s
          ON s.clobTokenId = json_extract_string(je.value, '$')
        WHERE mt.clobTokenIds IS NOT NULL
          AND mt.clobTokenIds != '[]'
          AND LEFT(LTRIM(mt.clobTokenIds), 1) = '['
          AND json_extract_string(je.value, '$') IS NOT NULL
          AND s.clobTokenId IS NULL
          AND NOT (COALESCE(m.closed, FALSE) = TRUE AND COALESCE(l.fully_checked, FALSE) = TRUE)
          AND (l.clobTokenId IS NULL OR l.next_check_at IS NULL OR l.next_check_at <= CURRENT_TIMESTAMP)
        """
    ]
    params: List = []
    if cutoff_created_at:
        query_parts.append("AND m.created_at >= ?")
        params.append(cutoff_created_at)
    query_parts.append(_market_scope_where_clause(market_scope, "m"))
    query_parts.append(_volume_where_clause(min_volume, max_volume, "m"))
    query_parts.append(_ended_market_where_clause(ended_market_grace_days, "m"))
    query = "\n".join(query_parts)
    with get_connection() as conn:
        cursor = conn.execute(query, params) if params else conn.execute(query)
        while True:
            rows = cursor.fetchmany(page_size)
            if not rows:
                break
            yield rows


def count_due_market_token_exclusions(
    *,
    cutoff_created_at: Optional[str] = None,
    market_scope: str = "all",
    ended_market_grace_days: int | None = None,
    min_volume: float | None = None,
    max_volume: float | None = None,
) -> dict[str, int]:
    """Count due-token candidates skipped by routine scope/freshness filters."""
    scope = validate_market_scope(market_scope)
    ensure_duck_db()
    params: List = []
    base_where = [
        "mt.clobTokenIds IS NOT NULL",
        "mt.clobTokenIds != '[]'",
        "LEFT(LTRIM(mt.clobTokenIds), 1) = '['",
        "json_extract_string(je.value, '$') IS NOT NULL",
        "s.clobTokenId IS NULL",
        "NOT (COALESCE(m.closed, FALSE) = TRUE AND COALESCE(l.fully_checked, FALSE) = TRUE)",
        "(l.clobTokenId IS NULL OR l.next_check_at IS NULL OR l.next_check_at <= CURRENT_TIMESTAMP)",
    ]
    if cutoff_created_at:
        base_where.append("m.created_at >= ?")
        params.append(cutoff_created_at)
    base_sql = " AND ".join(base_where)
    join_sql = f"""
        FROM {_TAB_MARKET_TOKENS} mt
        JOIN {_TAB_MARKETS} m ON mt.market_id = m.id
        CROSS JOIN LATERAL json_each(mt.clobTokenIds) AS je
        LEFT JOIN {_TAB_TOKEN_SYNC_LEDGER} l
          ON l.clobTokenId = json_extract_string(je.value, '$')
        LEFT JOIN {_TAB_TOKEN_SYNC_SKIPS} s
          ON s.clobTokenId = json_extract_string(je.value, '$')
    """
    scope_skip = 0
    ended_skip = 0
    scoped = scope == MARKET_SCOPE_WC2026
    with get_connection() as conn:
        if scoped:
            predicate = market_scope_predicate_sql(market_scope, "m")
            row = conn.execute(
                f"""
                SELECT COUNT(*)
                {join_sql}
                WHERE {base_sql}
                  AND NOT ({predicate})
                """,
                params,
            ).fetchone()
            scope_skip = int(row[0]) if row and row[0] is not None else 0
        if ended_market_grace_days is not None:
            days = max(0, int(ended_market_grace_days))
            scope_condition = (
                market_scope_predicate_sql(market_scope, "m") if scoped else "TRUE"
            )
            row = conn.execute(
                f"""
                SELECT COUNT(*)
                {join_sql}
                WHERE {base_sql}
                  AND ({scope_condition})
                  AND m.end_date IS NOT NULL
                  AND m.end_date < CURRENT_TIMESTAMP - INTERVAL {days} DAY
                """,
                params,
            ).fetchone()
            ended_skip = int(row[0]) if row and row[0] is not None else 0
    return {"scope_skip": scope_skip, "ended_market_skip": ended_skip}


def count_candidate_market_tokens(
    *,
    cutoff_created_at: Optional[str] = None,
    market_scope: str = "all",
    ended_market_grace_days: int | None = None,
    due_only: bool = True,
    min_volume: float | None = None,
    max_volume: float | None = None,
) -> dict[str, int]:
    """Count market-token candidates for odds sync planning (approximate upper bound).

    When ``due_only`` is True, mirrors ``iter_due_market_tokens`` (ledger/skip filters).
    When False, mirrors ``iter_markets_with_tokens`` with ``json_array_only=True``.
    Per-token planner drops (recent_skip, invalid id, dup) are not reflected here.
    """
    validate_market_scope(market_scope)
    ensure_duck_db()
    params: List = []
    if due_only:
        base_where = [
            "mt.clobTokenIds IS NOT NULL",
            "mt.clobTokenIds != '[]'",
            "LEFT(LTRIM(mt.clobTokenIds), 1) = '['",
            "json_extract_string(je.value, '$') IS NOT NULL",
            "s.clobTokenId IS NULL",
            "NOT (COALESCE(m.closed, FALSE) = TRUE AND COALESCE(l.fully_checked, FALSE) = TRUE)",
            "(l.clobTokenId IS NULL OR l.next_check_at IS NULL OR l.next_check_at <= CURRENT_TIMESTAMP)",
        ]
        if cutoff_created_at:
            base_where.append("m.created_at >= ?")
            params.append(cutoff_created_at)
        query = f"""
            SELECT COUNT(*), COUNT(DISTINCT mt.market_id)
            FROM {_TAB_MARKET_TOKENS} mt
            JOIN {_TAB_MARKETS} m ON mt.market_id = m.id
            CROSS JOIN LATERAL json_each(mt.clobTokenIds) AS je
            LEFT JOIN {_TAB_TOKEN_SYNC_LEDGER} l
              ON l.clobTokenId = json_extract_string(je.value, '$')
            LEFT JOIN {_TAB_TOKEN_SYNC_SKIPS} s
              ON s.clobTokenId = json_extract_string(je.value, '$')
            WHERE {" AND ".join(base_where)}
            {_market_scope_where_clause(market_scope, "m")}
            {_volume_where_clause(min_volume, max_volume, "m")}
            {_ended_market_where_clause(ended_market_grace_days, "m")}
        """
    else:
        base_where = [
            "mt.clobTokenIds IS NOT NULL",
            "mt.clobTokenIds != '[]'",
            "LEFT(LTRIM(mt.clobTokenIds), 1) = '['",
        ]
        if cutoff_created_at:
            base_where.append("m.created_at >= ?")
            params.append(cutoff_created_at)
        query = f"""
            SELECT
                COALESCE(SUM(json_array_length(mt.clobTokenIds)), 0),
                COUNT(*)
            FROM {_TAB_MARKET_TOKENS} mt
            JOIN {_TAB_MARKETS} m ON mt.market_id = m.id
            WHERE {" AND ".join(base_where)}
            {_market_scope_where_clause(market_scope, "m")}
            {_volume_where_clause(min_volume, max_volume, "m")}
        """
    with get_connection() as conn:
        row = conn.execute(query, params).fetchone()
    candidate_tokens = int(row[0]) if row and row[0] is not None else 0
    candidate_markets = int(row[1]) if row and row[1] is not None else 0
    return {
        "candidate_tokens": candidate_tokens,
        "candidate_markets": candidate_markets,
    }


def delete_orphan_market_tokens() -> int:
    """Remove raw ``market_tokens`` rows with no parent row in ``markets`` (referential repair)."""
    ensure_duck_db()
    with get_connection() as conn:
        n = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM {_TAB_MARKET_TOKENS} mt
            WHERE NOT EXISTS (SELECT 1 FROM {_TAB_MARKETS} m WHERE m.id = mt.market_id)
            """
        ).fetchone()[0]
        n = int(n)
        if n:
            conn.execute(
                f"""
                DELETE FROM {_TAB_MARKET_TOKENS} mt
                WHERE NOT EXISTS (SELECT 1 FROM {_TAB_MARKETS} m WHERE m.id = mt.market_id)
                """
            )
            logger.info(
                "Removed %s market_tokens row(s) with no matching markets.id",
                n,
            )
        return n


def save_tokens_batch(token_data: List[Tuple[str, str]]):
    if not token_data:
        return
    ensure_duck_db()
    mids = list({mid for mid, _ in token_data})
    placeholders = ",".join("?" * len(mids))
    with get_connection() as conn:
        valid = {
            r[0]
            for r in conn.execute(
                f"SELECT id FROM {_TAB_MARKETS} WHERE id IN ({placeholders})",
                mids,
            ).fetchall()
        }
    filtered = [(mid, toks) for mid, toks in token_data if mid in valid]
    dropped = len(token_data) - len(filtered)
    if dropped:
        logger.warning(
            "save_tokens_batch: skipping %s row(s) whose market_id is not in markets",
            dropped,
        )
    if not filtered:
        return
    now = _utc_now()
    rows = [(mid, toks, now) for mid, toks in filtered]
    with get_connection() as conn:
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO {_TAB_MARKET_TOKENS}
            (market_id, clobTokenIds, updated_at)
            VALUES (?, ?, ?)
            """,
            rows,
        )


def get_markets_without_slugs(limit: Optional[int] = None) -> List[str]:
    ensure_duck_db()
    return _fetch_market_ids(
        f"""
        SELECT id
        FROM {_TAB_MARKETS}
        WHERE (slug IS NULL OR slug = '')
          AND TRY_CAST(id AS BIGINT) IS NOT NULL
          AND TRY_CAST(id AS BIGINT) > 0
    """,
        limit=limit,
    )


def get_markets_without_event_slugs(limit: Optional[int] = None) -> List[str]:
    ensure_duck_db()
    return _fetch_market_ids(
        f"""
        SELECT id
        FROM {_TAB_MARKETS}
        WHERE (event_slug IS NULL OR event_slug = '')
          AND TRY_CAST(id AS BIGINT) IS NOT NULL
          AND TRY_CAST(id AS BIGINT) > 0
        ORDER BY TRY_CAST(id AS BIGINT)
    """,
        limit=limit,
    )


def get_markets_without_end_date(limit: Optional[int] = None) -> List[str]:
    ensure_duck_db()
    return _fetch_market_ids(
        f"""
        SELECT id
        FROM {_TAB_MARKETS}
        WHERE (end_date IS NULL OR CAST(end_date AS VARCHAR) = '')
    """,
        limit=limit,
    )


def save_slugs_batch(slug_data: List[Tuple[str, str]]):
    if not slug_data:
        return
    ensure_duck_db()
    with get_connection() as conn:
        conn.executemany(f"UPDATE {_TAB_MARKETS} SET slug = ? WHERE id = ?", slug_data)


def save_event_slugs_batch(event_slug_data: List[Tuple[str, str]]):
    if not event_slug_data:
        return
    ensure_duck_db()
    with get_connection() as conn:
        conn.executemany(
            f"UPDATE {_TAB_MARKETS} SET event_slug = ? WHERE id = ?", event_slug_data
        )
        conn.executemany(
            f"""
            DELETE FROM {_TAB_MARKET_METADATA_UNRESOLVED}
            WHERE market_id = ? AND field_name = 'event_slug'
            """,
            [(market_id,) for _, market_id in event_slug_data],
        )


def save_end_dates_batch(end_date_data: List[Tuple[str, str]]):
    if not end_date_data:
        return
    ensure_duck_db()
    with get_connection() as conn:
        conn.executemany(
            f"UPDATE {_TAB_MARKETS} SET end_date = ? WHERE id = ?", end_date_data
        )


def mark_market_metadata_unresolved(
    rows: List[Tuple[str, str, str]],
    *,
    retry_after_hours: int = 168,
) -> None:
    if not rows:
        return
    ensure_duck_db()
    retry_hours = max(1, int(retry_after_hours))
    with get_connection() as conn:
        conn.executemany(
            f"""
            INSERT INTO {_TAB_MARKET_METADATA_UNRESOLVED} (
                market_id,
                field_name,
                reason,
                attempts,
                last_checked_at,
                next_retry_at
            )
            VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + (? * INTERVAL '1 hour'))
            ON CONFLICT(market_id, field_name) DO UPDATE SET
                reason = excluded.reason,
                attempts = COALESCE(attempts, 0) + 1,
                last_checked_at = excluded.last_checked_at,
                next_retry_at = excluded.next_retry_at
            """,
            [
                (market_id, field_name, reason, retry_hours)
                for market_id, field_name, reason in rows
            ],
        )


__all__ = [
    "get_market_count",
    "get_all_market_ids",
    "save_markets_batch",
    "save_market_tokens_batch",
    "delete_orphan_market_tokens",
    "get_markets_without_tokens",
    "get_markets_missing_any_metadata",
    "get_markets_with_tokens",
    "iter_markets_with_tokens",
    "count_due_market_token_exclusions",
    "save_tokens_batch",
    "get_markets_without_slugs",
    "get_markets_without_event_slugs",
    "get_markets_without_end_date",
    "save_slugs_batch",
    "save_event_slugs_batch",
    "save_end_dates_batch",
    "mark_market_metadata_unresolved",
]
