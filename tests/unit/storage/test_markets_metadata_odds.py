"""Coverage for storage/duckdb markets, metadata, odds modules."""

from __future__ import annotations

import importlib
import json
from datetime import datetime, timezone

import duckdb
import pytest

import oddsfox.storage.duckdb.markets as markets
import oddsfox.storage.duckdb.metadata as metadata
import oddsfox.storage.duckdb.odds as odds_mod
from oddsfox.config._reload_settings import reload_all_settings_modules
from oddsfox.storage.duckdb.connection import (
    get_connection,
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)
from oddsfox.storage.duckdb.schemas.polymarket import create_test_markets_table
from oddsfox.storage.duckdb.wc2026_registry import (
    RegistryRow,
    upsert_registry_rows,
)

T_M = polymarket_raw_tbl("markets")
T_MT = polymarket_raw_tbl("market_tokens")
T_TOD = polymarket_raw_tbl("token_odds_daily")
T_LED = polymarket_ops_tbl("token_sync_ledger")
T_SK = polymarket_ops_tbl("token_sync_skips")
T_PRE = polymarket_ops_tbl("pipeline_run_events")
T_UNR = polymarket_ops_tbl("market_metadata_unresolved")


@pytest.fixture
def duck(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "unit.duckdb"))
    import oddsfox.storage.duckdb.connection as connection

    reload_all_settings_modules()
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
    importlib.reload(connection)
    connection.ensure_duck_db()
    with get_connection() as conn:
        create_test_markets_table(conn)
    yield connection
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False


def _insert_minimal_market(conn, mid="m1", **kwargs):
    defaults = dict(
        id=mid,
        question="Q",
        category="c",
        description="d",
        outcomes="[]",
        volume=1.0,
        active=True,
        closed=False,
        created_at=datetime.now(timezone.utc),
        scraped_at=datetime.now(timezone.utc),
        end_date=None,
        slug=None,
        event_slug=None,
        event_id=None,
    )
    defaults.update(kwargs)
    conn.execute(
        f"""INSERT OR REPLACE INTO {T_M}
        (id, question, category, description, outcomes, volume, active, closed,
         created_at, scraped_at, end_date, slug, event_slug, event_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            defaults["id"],
            defaults["question"],
            defaults["category"],
            defaults["description"],
            defaults["outcomes"],
            defaults["volume"],
            defaults["active"],
            defaults["closed"],
            defaults["created_at"],
            defaults["scraped_at"],
            defaults["end_date"],
            defaults["slug"],
            defaults["event_slug"],
            defaults["event_id"],
        ],
    )


def test_save_markets_batch_requires_dlt_markets_table(duck):
    import oddsfox.storage.duckdb.markets as markets_mod

    with duck.get_connection() as conn:
        conn.execute(f"DROP TABLE IF EXISTS {T_M}")
    with pytest.raises(RuntimeError, match="dlt_polymarket_markets"):
        markets_mod.save_markets_batch(
            [
                (
                    "a",
                    "q",
                    "c",
                    "d",
                    "[]",
                    1.0,
                    True,
                    False,
                    "2024-01-01 00:00:00",
                    "2024-01-01 00:00:00",
                    None,
                    None,
                    None,
                    None,
                )
            ],
            [],
        )


def test_markets_count_and_ids(duck):
    assert markets.get_market_count() == 0
    markets.save_markets_batch(
        [
            (
                "a",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2020-01-01 00:00:00",
                "2020-01-01 00:00:00",
                None,
                None,
                None,
            )
        ],
        [],
    )
    assert markets.get_market_count() == 1
    assert "a" in markets.get_all_market_ids()


def test_save_markets_batch_tuple_shapes_10_11_12(duck):
    row10 = (
        "x4",
        "q",
        "c",
        "d",
        "[]",
        1.0,
        True,
        False,
        "2020-01-01 00:00:00",
        "2020-01-01 00:00:00",
    )
    row11 = row10 + ("s",)
    row12 = row11 + ("es",)
    markets.save_markets_batch([row10, row11, row12], [("x4", '["t"]')])


def test_save_markets_batch_invalid_length_raises(duck):
    with pytest.raises(ValueError, match="Expected 10-14"):
        markets.save_markets_batch([("a", "b")], [])


def test_save_markets_batch_empty_returns(duck):
    markets.save_markets_batch([], [])


def test_end_date_sanitize(duck):
    markets.save_markets_batch(
        [
            (
                "ed",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2020-01-01 00:00:00",
                "2020-01-01 00:00:00",
                "",
                None,
                None,
            )
        ],
        [],
    )


def test_end_date_sanitize_whitespace_only(duck):
    """_sanitize_end_date: str with only whitespace -> None (73-77 branches)."""
    markets.save_markets_batch(
        [
            (
                "ew",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2020-01-01 00:00:00",
                "2020-01-01 00:00:00",
                "   ",
                None,
                None,
            )
        ],
        [],
    )


def test_end_date_preserved_when_present(duck):
    markets.save_markets_batch(
        [
            (
                "ep",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2020-01-01 00:00:00",
                "2020-01-01 00:00:00",
                "2026-01-01 00:00:00",
                None,
                None,
            )
        ],
        [],
    )


def test_get_markets_without_tokens_and_save_tokens(duck):
    with markets.get_connection() as conn:
        _insert_minimal_market(conn, "nm")
        mids = markets.get_markets_without_tokens(limit=5)
        assert "nm" in mids
    markets.save_tokens_batch([("nm", '["tok"]')])
    assert markets.get_markets_without_tokens(limit=5) == []


def test_delete_orphan_market_tokens(duck):
    assert markets.delete_orphan_market_tokens() == 0
    with markets.get_connection() as conn:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {T_MT} (market_id, clobTokenIds, updated_at)
            VALUES ('orphan_only', '["t"]', CURRENT_TIMESTAMP)
            """
        )
    assert markets.delete_orphan_market_tokens() == 1
    assert markets.delete_orphan_market_tokens() == 0


def test_save_tokens_batch_skips_unknown_market_id(duck, caplog):
    caplog.set_level("WARNING")
    markets.save_tokens_batch([("unknown_mid", '["x"]')])
    with markets.get_connection() as conn:
        n = conn.execute(
            f"SELECT COUNT(*) FROM {T_MT} WHERE market_id = 'unknown_mid'"
        ).fetchone()[0]
    assert int(n) == 0
    assert any("skipping" in r.message for r in caplog.records)


def test_get_markets_with_tokens_and_iter(duck):
    markets.save_markets_batch(
        [
            (
                "gw",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-01 00:00:00",
                "2024-01-01 00:00:00",
                None,
                None,
                None,
            )
        ],
        [("gw", '["a","b"]')],
    )
    rows = markets.get_markets_with_tokens()
    assert len(rows) >= 1
    pages = list(
        markets.iter_markets_with_tokens(
            page_size=1, cutoff_created_at="2020-01-01 00:00:00", json_array_only=True
        )
    )
    assert pages


def test_slug_event_end_date_queries(duck):
    with markets.get_connection() as conn:
        conn.execute(
            f"""INSERT OR REPLACE INTO {T_M}
            (id, question, category, description, outcomes, volume, active, closed,
             created_at, scraped_at, end_date, slug, event_slug)
            VALUES ('99', 'q','c','d','[]',1.0,1,0,
             '2024-01-01','2024-01-01', NULL, '', '')"""
        )
    assert "99" in markets.get_markets_without_slugs(limit=10)
    assert "99" in markets.get_markets_without_event_slugs(limit=10)
    assert "99" in markets.get_markets_without_end_date(limit=10)
    markets.save_slugs_batch([("slug99", "99")])
    markets.save_event_slugs_batch([("es99", "99")])
    markets.save_end_dates_batch([("2025-12-31 00:00:00", "99")])


def test_save_sync_run_metrics_history_json_not_list(duck):
    """Parsed history is valid JSON but not a list — skip list branch (143-144)."""
    metadata._metadata_set("sync_metrics:nl2:history", json.dumps({"a": 1}))
    metadata.save_sync_run_metrics("nl2", {"x": 1}, history_limit=5)


def test_save_sync_run_metrics_history_list_mixed_types(duck):
    """History JSON is a list: keep only dict items (lines 143-144)."""
    metadata._metadata_set(
        "sync_metrics:mix:history",
        json.dumps([{"ok": 1}, "not-a-dict", {"ok": 2}]),
    )
    metadata.save_sync_run_metrics("mix", {"n": 1}, history_limit=5)
    raw = metadata._metadata_get("sync_metrics:mix:history")
    assert raw and '"n"' in raw


def test_save_sync_run_metrics_corrupt_history_json(duck):
    metadata._metadata_set("sync_metrics:hist:last", json.dumps({"a": 1}))
    metadata._metadata_set("sync_metrics:hist:history", "{not-json")
    metadata.save_sync_run_metrics("hist", {"b": 2}, history_limit=5)
    raw = metadata._metadata_get("sync_metrics:hist:history")
    assert raw and "b" in raw


def test_get_sync_run_metrics_non_dict_payload(duck):
    metadata._metadata_set("sync_metrics:nd:last", json.dumps([1, 2, 3]))
    assert metadata.get_sync_run_metrics("nd") is None


def test_save_sync_run_metrics_zero_history_limit(duck):
    metadata.save_sync_run_metrics("zlim", {"x": 1}, history_limit=0)


def test_append_pipeline_run_event_inserts_row(duck):
    rid = metadata.append_pipeline_run_event("sync_odds", {"rows": 1})
    with metadata.get_connection() as conn:
        n = conn.execute(
            f"SELECT COUNT(*) FROM {T_PRE} WHERE run_id = ?", [rid]
        ).fetchone()[0]
    assert n == 1


def test_save_sync_run_metrics_preserves_nested_planning_payload(duck):
    payload = {
        "planning": {"plans": 2, "closed_done": 1},
        "planning_context": {
            "market_tokens_distinct_tokens": 10,
            "planned_vs_market_tokens": 0.2,
        },
        "invalid_tokens": 1,
    }
    metadata.save_sync_run_metrics("sync_odds", payload, history_limit=2)
    saved = metadata.get_sync_run_metrics("sync_odds")
    assert saved is not None
    assert saved["planning"]["plans"] == 2
    assert saved["planning_context"]["market_tokens_distinct_tokens"] == 10
    assert saved["invalid_tokens"] == 1


def test_save_sync_run_metrics_pipeline_append_failure_continues(monkeypatch, duck):
    def boom(*_a, **_k):
        raise RuntimeError("simulated append failure")

    monkeypatch.setattr(metadata, "append_pipeline_run_event", boom)
    metadata.save_sync_run_metrics("append_fail", {"x": 1})
    assert metadata.get_sync_run_metrics("append_fail") is not None


def test_markets_empty_saves_noop(duck):
    markets.save_tokens_batch([])
    markets.save_slugs_batch([])
    markets.save_event_slugs_batch([])
    markets.save_end_dates_batch([])


def test_iter_markets_json_array_only_no_cutoff(duck):
    markets.save_markets_batch(
        [
            (
                "ja",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-01 00:00:00",
                "2024-01-01 00:00:00",
                None,
                None,
                None,
            )
        ],
        [("ja", '["tok"]')],
    )
    pages = list(markets.iter_markets_with_tokens(page_size=10, json_array_only=True))
    assert pages


def test_iter_markets_with_cutoff_without_json_array_filter(duck):
    markets.save_markets_batch(
        [
            (
                "jb",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            )
        ],
        [("jb", '["tok"]')],
    )
    pages = list(
        markets.iter_markets_with_tokens(
            page_size=10,
            cutoff_created_at="2024-01-01 00:00:00",
            json_array_only=False,
        )
    )
    assert pages


def test_iter_due_market_tokens_filters_due_closed_and_skipped(duck):
    markets.save_markets_batch(
        [
            (
                "due_market",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            ),
            (
                "future_market",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            ),
            (
                "closed_market",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                True,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            ),
            (
                "skip_market",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            ),
        ],
        [
            ("due_market", '["tok_due"]'),
            ("future_market", '["tok_future"]'),
            ("closed_market", '["tok_closed"]'),
            ("skip_market", '["tok_skip"]'),
        ],
    )
    with markets.get_connection() as conn:
        conn.execute(
            f"""
            INSERT INTO {T_LED}
            (clobTokenId, next_check_at, fully_checked)
            VALUES
            ('tok_future', CURRENT_TIMESTAMP + INTERVAL 1 DAY, FALSE),
            ('tok_closed', CURRENT_TIMESTAMP - INTERVAL 1 DAY, TRUE)
            """
        )
        conn.execute(
            f"INSERT INTO {T_SK} (clobTokenId, reason) VALUES ('tok_skip', 'bad token')"
        )
    pages = list(
        markets.iter_due_market_tokens(
            page_size=10,
            cutoff_created_at="2024-01-01 00:00:00",
        )
    )
    flat = [row for page in pages for row in page]
    assert len(flat) == 1
    assert flat[0][0] == "due_market"
    assert flat[0][1] == "tok_due"
    assert flat[0][3] is False


def test_iter_due_market_tokens_without_cutoff(duck):
    markets.save_markets_batch(
        [
            (
                "no_cutoff_market",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            )
        ],
        [("no_cutoff_market", '["tok_nocutoff"]')],
    )
    pages = list(markets.iter_due_market_tokens(page_size=10))
    flat = [row for page in pages for row in page]
    assert any(row[1] == "tok_nocutoff" for row in flat)


def test_iter_due_market_tokens_scopes_wc2026_and_counts_scope_skip(duck):
    markets.save_markets_batch(
        [
            (
                "wc_market",
                "FIFA World Cup 2026 winner",
                "sports",
                "World Cup 2026",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                "world-cup-2026-winner",
                "2026-fifa-world-cup-winner-595",
                None,
            ),
            (
                "other_market",
                "Premier League winner",
                "sports",
                "Club market",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                "premier-league-winner",
                None,
                None,
            ),
        ],
        [("wc_market", '["tok_wc"]'), ("other_market", '["tok_other"]')],
    )
    upsert_registry_rows(
        [
            RegistryRow(
                "wc_market",
                "2026-fifa-world-cup-winner-595",
                None,
                "seed",
            )
        ]
    )
    pages = list(
        markets.iter_due_market_tokens(
            page_size=10,
            cutoff_created_at="2024-01-01 00:00:00",
            market_scope="wc2026",
        )
    )
    flat = [row for page in pages for row in page]
    assert [row[1] for row in flat] == ["tok_wc"]
    counts = markets.count_due_market_token_exclusions(
        cutoff_created_at="2024-01-01 00:00:00",
        market_scope="wc2026",
    )
    assert counts["scope_skip"] == 1


def test_iter_due_market_tokens_skips_ended_markets_after_grace(duck):
    markets.save_markets_batch(
        [
            (
                "ended_old",
                "FIFA World Cup 2026 old",
                "sports",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                "2000-01-01 00:00:00",
                "world-cup-2026-old",
                "2026-fifa-world-cup-winner-595",
                None,
            ),
            (
                "future_end",
                "FIFA World Cup 2026 future",
                "sports",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                "2999-01-01 00:00:00",
                "world-cup-2026-future",
                "2026-fifa-world-cup-winner-595",
                None,
            ),
        ],
        [("ended_old", '["tok_old"]'), ("future_end", '["tok_future_end"]')],
    )
    pages = list(
        markets.iter_due_market_tokens(
            page_size=10,
            market_scope="wc2026",
            ended_market_grace_days=7,
        )
    )
    flat = [row for page in pages for row in page]
    assert [row[1] for row in flat] == ["tok_future_end"]
    counts = markets.count_due_market_token_exclusions(
        market_scope="wc2026",
        ended_market_grace_days=7,
    )
    assert counts["ended_market_skip"] == 1


def test_count_candidate_market_tokens_due_only(duck):
    markets.save_markets_batch(
        [
            (
                "due_market",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            ),
            (
                "future_market",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            ),
            (
                "closed_market",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                True,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            ),
            (
                "skip_market",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            ),
        ],
        [
            ("due_market", '["tok_due"]'),
            ("future_market", '["tok_future"]'),
            ("closed_market", '["tok_closed"]'),
            ("skip_market", '["tok_skip"]'),
        ],
    )
    with markets.get_connection() as conn:
        conn.execute(
            f"""
            INSERT INTO {T_LED}
            (clobTokenId, next_check_at, fully_checked)
            VALUES
            ('tok_future', CURRENT_TIMESTAMP + INTERVAL 1 DAY, FALSE),
            ('tok_closed', CURRENT_TIMESTAMP - INTERVAL 1 DAY, TRUE)
            """
        )
        conn.execute(
            f"INSERT INTO {T_SK} (clobTokenId, reason) VALUES ('tok_skip', 'bad token')"
        )
    counts = markets.count_candidate_market_tokens(
        cutoff_created_at="2024-01-01 00:00:00",
        due_only=True,
    )
    assert counts == {"candidate_tokens": 1, "candidate_markets": 1}


def test_count_candidate_market_tokens_force_mode(duck):
    markets.save_markets_batch(
        [
            (
                "m_two_tok",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            ),
            (
                "m_one_tok",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            ),
        ],
        [
            ("m_two_tok", '["tok_a", "tok_b"]'),
            ("m_one_tok", '["tok_c"]'),
        ],
    )
    counts = markets.count_candidate_market_tokens(
        cutoff_created_at="2024-01-01 00:00:00",
        due_only=False,
    )
    assert counts == {"candidate_tokens": 3, "candidate_markets": 2}


def test_count_candidate_market_tokens_without_cutoff(duck):
    markets.save_markets_batch(
        [
            (
                "m_nocutoff",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
                None,
                None,
                None,
            ),
        ],
        [("m_nocutoff", '["tok_nocutoff"]')],
    )
    counts = markets.count_candidate_market_tokens(due_only=True)
    assert counts == {"candidate_tokens": 1, "candidate_markets": 1}


def test_volume_where_clause_helpers():
    assert markets._volume_where_clause(None, None) == ""
    clause = markets._volume_where_clause(100_000.0, None)
    assert "COALESCE(m.volume, 0) >= 100000.0" in clause
    clause = markets._volume_where_clause(None, 100_000.0)
    assert "COALESCE(m.volume, 0) < 100000.0" in clause
    with pytest.raises(ValueError, match="min_volume"):
        markets._validate_volume_bound("bad", name="min_volume")
    with pytest.raises(ValueError, match="max_volume"):
        markets._volume_where_clause(None, -5.0)


def test_iter_markets_with_tokens_volume_bounds(duck):
    markets.save_markets_batch(
        [
            (
                "m_whale",
                "q",
                "c",
                "d",
                "[]",
                150_000.0,
                True,
                False,
                "2024-01-01 00:00:00",
                "2024-01-01 00:00:00",
                None,
                None,
                None,
                None,
            ),
            (
                "m_small",
                "q",
                "c",
                "d",
                "[]",
                500.0,
                True,
                False,
                "2024-01-01 00:00:00",
                "2024-01-01 00:00:00",
                None,
                None,
                None,
                None,
            ),
        ],
        [
            ("m_whale", '["tok_whale"]'),
            ("m_small", '["tok_small"]'),
        ],
    )
    whale_pages = list(
        markets.iter_markets_with_tokens(
            page_size=10,
            json_array_only=True,
            min_volume=100_000.0,
        )
    )
    whale_ids = {row[0] for page in whale_pages for row in page}
    assert whale_ids == {"m_whale"}

    daily_pages = list(
        markets.iter_markets_with_tokens(
            page_size=10,
            json_array_only=True,
            max_volume=100_000.0,
        )
    )
    daily_ids = {row[0] for page in daily_pages for row in page}
    assert daily_ids == {"m_small"}


def test_count_candidate_market_tokens_force_mode_without_cutoff(duck):
    markets.save_markets_batch(
        [
            (
                "m_force_nocutoff",
                "q",
                "c",
                "d",
                "[]",
                1.0,
                True,
                False,
                "2024-01-01 00:00:00",
                "2024-01-01 00:00:00",
                None,
                None,
                None,
                None,
            ),
        ],
        [("m_force_nocutoff", '["tok_force"]')],
    )
    counts = markets.count_candidate_market_tokens(due_only=False)
    assert counts == {"candidate_tokens": 1, "candidate_markets": 1}


def test_get_markets_missing_any_metadata_individual_field_predicates(duck):
    with duck.get_connection() as conn:
        _insert_minimal_market(
            conn,
            "missing-all",
            slug="",
            event_slug="event-present",
            end_date=None,
        )

    assert markets.get_markets_missing_any_metadata(
        include_tokens=True,
        include_slugs=False,
        include_event_slugs=False,
        include_end_dates=False,
    ) == ["missing-all"]
    assert markets.get_markets_missing_any_metadata(
        include_tokens=False,
        include_slugs=True,
        include_event_slugs=False,
        include_end_dates=False,
    ) == ["missing-all"]
    assert markets.get_markets_missing_any_metadata(
        include_tokens=False,
        include_slugs=False,
        include_event_slugs=False,
        include_end_dates=True,
    ) == ["missing-all"]


def test_market_metadata_unresolved_cooldown_suppresses_event_slug_retry(duck):
    with duck.get_connection() as conn:
        _insert_minimal_market(
            conn,
            "cooldown",
            question="World Cup 2026 winner",
            slug="world-cup-2026-winner",
            event_slug="2026-fifa-world-cup-winner-595",
        )
        conn.execute(f"UPDATE {T_M} SET event_slug = NULL WHERE id = 'cooldown'")
    upsert_registry_rows([RegistryRow("cooldown", None, None, "seed")])
    markets.save_tokens_batch([("cooldown", '["tok"]')])

    assert (
        markets.get_markets_missing_any_metadata(
            include_tokens=False,
            include_slugs=False,
            include_event_slugs=False,
            include_end_dates=False,
        )
        == []
    )
    markets.mark_market_metadata_unresolved([])

    assert markets.get_markets_missing_any_metadata(
        include_tokens=False,
        include_slugs=False,
        include_event_slugs=True,
        include_end_dates=False,
        market_scope="wc2026",
        limit=10,
    ) == ["cooldown"]

    markets.mark_market_metadata_unresolved(
        [("cooldown", "event_slug", "missing")],
        retry_after_hours=168,
    )
    markets.mark_market_metadata_unresolved(
        [("cooldown", "event_slug", "still missing")],
        retry_after_hours=168,
    )
    with markets.get_connection() as conn:
        attempts, reason = conn.execute(
            f"SELECT attempts, reason FROM {T_UNR} WHERE market_id = 'cooldown'"
        ).fetchone()
    assert attempts == 2
    assert reason == "still missing"
    assert (
        markets.get_markets_missing_any_metadata(
            include_tokens=False,
            include_slugs=False,
            include_event_slugs=True,
            include_end_dates=False,
            market_scope="wc2026",
        )
        == []
    )

    with markets.get_connection() as conn:
        conn.execute(
            f"UPDATE {T_UNR} SET next_retry_at = TIMESTAMP '2000-01-01 00:00:00'"
        )
    assert markets.get_markets_missing_any_metadata(
        include_tokens=False,
        include_slugs=False,
        include_event_slugs=True,
        include_end_dates=False,
        market_scope="wc2026",
    ) == ["cooldown"]

    markets.save_event_slugs_batch([("world-cup-2026", "cooldown")])
    with markets.get_connection() as conn:
        remaining = conn.execute(f"SELECT count(*) FROM {T_UNR}").fetchone()[0]
    assert remaining == 0


def test_fetch_market_ids_no_limit(duck):
    assert isinstance(markets._fetch_market_ids("SELECT 1 WHERE 1=0", limit=None), list)


def test_metadata_helpers(duck):
    assert metadata._metadata_get("missing") is None
    metadata._metadata_set("k", "v")
    assert metadata._metadata_get("k") == "v"

    assert metadata.get_backfill_fully_checked("t") is None
    metadata.set_backfill_fully_checked("t", True)
    assert metadata.get_backfill_fully_checked("t") is True
    metadata.set_backfill_fully_checked("t", False)
    assert metadata.get_backfill_fully_checked("t") is False

    assert metadata.get_backfill_progress("p") == 0
    metadata.set_backfill_progress("p", 3)
    assert metadata.get_backfill_progress("p") == 3

    metadata.save_sync_run_metrics("job", {"a": 1}, history_limit=2)
    m = metadata.get_sync_run_metrics("job")
    assert m is not None and m.get("a") == 1

    metadata._metadata_set("sync_metrics:bad:last", "not-json")
    assert metadata.get_sync_run_metrics("bad") is None

    metadata._metadata_set(
        "sync_metrics:badh:last",
        json.dumps([1, 2, 3]),
    )
    assert metadata.get_sync_run_metrics("badh") is None


def test_get_backfill_progress_invalid(duck):
    metadata._metadata_set("backfill:x:progress", "x")
    assert metadata.get_backfill_progress("x") == 0


def test_get_sync_run_metrics_missing_returns_none(duck):
    assert metadata.get_sync_run_metrics("missing") is None


def test_odds_chunked_raises():
    with pytest.raises(ValueError):
        list(odds_mod._chunked(["a"], 0))


def test_odds_latest_and_tokens(duck):
    odds_mod.save_odds_batch([("tok", 100, 0.5)])
    assert "tok" in odds_mod.get_latest_timestamps()
    assert "tok" in odds_mod.get_tokens_with_data()
    odds_mod.mark_tokens_fully_checked(["tok"])
    assert "tok" in odds_mod.get_fully_checked_tokens()
    odds_mod.save_skipped_tokens([("t2", "reason")])
    assert odds_mod.get_skipped_tokens()["t2"] == "reason"


def test_refresh_token_odds_daily_and_backfill(duck):
    odds_mod.save_odds_batch(
        [
            ("tok", 1710000000, 0.4),
            ("tok", 1710000300, 0.6),
            ("tok", 1710086400, 0.2),
        ]
    )
    with odds_mod.get_connection() as conn:
        odds_mod.refresh_token_odds_daily(
            [
                ("tok", odds_mod._epoch_to_utc_date(1710000000)),
                ("tok", odds_mod._epoch_to_utc_date(1710086400)),
            ],
            conn,
        )
        rows = conn.execute(
            f"""
            SELECT odds_date_utc, open_price, high_price, low_price, close_price, avg_price, observed_points
            FROM {T_TOD}
            WHERE clobTokenId = 'tok'
            ORDER BY odds_date_utc
            """
        ).fetchall()
    assert rows[0][1:] == (0.4, 0.6, 0.4, 0.6, 0.5, 2)
    assert rows[1][1:] == (0.2, 0.2, 0.2, 0.2, 0.2, 1)

    count = odds_mod.backfill_token_odds_daily_from_history()
    assert count >= 2


def test_save_odds_bulk_appender_fallback_without_appender(duck, monkeypatch):
    with odds_mod.get_connection() as conn:
        monkeypatch.delattr(duckdb, "Appender", raising=False)
        odds_mod.save_odds_bulk_appender([("z", 1, 0.1)], conn)


def test_save_odds_bulk_upsert_paths(duck):
    with odds_mod.get_connection() as conn:
        odds_mod.save_odds_bulk_upsert(
            [("z", 1, 0.1), ("z", 1, 0.2)], conn, assume_deduped=False
        )
        odds_mod.save_odds_bulk_upsert([("z", 2, 0.3)], conn, assume_deduped=True)


def test_reconcile_ledger(duck):
    odds_mod.save_odds_batch([("r", 50, 0.4)])
    summary = odds_mod.reconcile_token_sync_ledger_from_history()
    assert "scanned_tokens" in summary


def test_get_token_sync_snapshot_empty():
    assert odds_mod.get_token_sync_snapshot([]) == ({}, set(), {})


def test_get_token_sync_snapshot_with_reconcile(duck):
    odds_mod.save_odds_batch([("snap", 10, 0.5)])
    odds_mod.save_sync_status_batch([("snap", 5)])
    a, b, c = odds_mod.get_token_sync_snapshot(
        ["snap"], reconcile_with_history=True, repair_ledger=True
    )
    assert "snap" in a


def test_storage_duckdb_package_import():
    import oddsfox.storage.duckdb as pkg

    assert pkg.ensure_duck_db
    assert pkg.save_markets_batch
