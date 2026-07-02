import duckdb
import pytest

from oddsfox.storage.duckdb import observability as obs
from oddsfox.storage.duckdb.connection import init_duck_db
from oddsfox.storage.duckdb.observability import (
    delta_dbt_models,
    delta_raw_layer,
    format_dbt_snapshot_log,
    format_raw_snapshot_log,
    snapshot_dbt_models,
    snapshot_raw_layer,
)
from oddsfox.storage.duckdb.schemas.polymarket import create_test_markets_table


def test_snapshot_raw_layer_counts_polymarket_tables(
    tmp_path, monkeypatch, isolated_env
):
    import oddsfox.storage.duckdb.connection as conn_mod

    db_path = tmp_path / "obs.duckdb"
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    conn_mod.reset_duckdb_connection_state()
    init_duck_db()

    with duckdb.connect(str(db_path)) as conn:
        create_test_markets_table(conn)
        conn.execute(
            """
            insert into polymarket_raw.markets (
                id, question, category, description, outcomes, volume, active,
                closed, created_at, scraped_at, end_date, slug, event_slug, event_id
            )
            values (
                'm1', 'q', 'cat', 'desc', '[]', 1.0, true, false,
                current_timestamp, current_timestamp, current_timestamp,
                'slug', 'event', 'event-id'
            )
            """
        )

        snapshot = snapshot_raw_layer(conn=conn, level="basic")

    assert snapshot["markets_rows"] == 1
    assert snapshot["markets_missing"] is False
    assert "wc2026_market_registry_rows" in snapshot
    assert "market_tokens_distinct_tokens" not in snapshot


def test_delta_raw_layer_ignores_missing_flags():
    assert delta_raw_layer(
        {"markets_rows": 1, "markets_missing": True},
        {"markets_rows": 2, "markets_missing": False},
    ) == {"markets_rows": {"before": 1, "after": 2}}


def test_snapshot_dbt_models_reports_missing_relations(tmp_path):
    with duckdb.connect(str(tmp_path / "dbt.duckdb")) as conn:
        snapshot = snapshot_dbt_models(conn=conn)

    assert snapshot["polymarket_staging.stg_polymarket_markets"] == {
        "exists": False,
        "rows": None,
    }


def test_dbt_delta_and_formatters():
    before = {"polymarket_marts.wc2026_markets": {"exists": False, "rows": None}}
    after = {"polymarket_marts.wc2026_markets": {"exists": True, "rows": 3}}

    assert delta_dbt_models(before, after) == {
        "polymarket_marts.wc2026_markets": {
            "before": {"exists": False, "rows": None},
            "after": {"exists": True, "rows": 3},
        }
    }
    assert "markets=2" in format_raw_snapshot_log({"markets_rows": 2})
    assert "wc2026_markets:exists=True,rows=3" in format_dbt_snapshot_log(after)


def test_observability_scalar_and_row_count_error_branches(caplog):
    class NoneRowConn:
        def execute(self, *_args, **_kwargs):
            return self

        def fetchone(self):
            return None

    class BadValueConn(NoneRowConn):
        def fetchone(self):
            return ("bad-int",)

    class DuckErrorConn(NoneRowConn):
        def execute(self, *_args, **_kwargs):
            raise duckdb.Error("boom")

    assert obs._scalar_int(NoneRowConn(), "select 1") is None
    assert obs._table_row_count(NoneRowConn(), "x") == (True, 0)
    assert obs._scalar_int(DuckErrorConn(), "select 1") is None
    assert obs._table_row_count(DuckErrorConn(), "x") == (False, None)

    caplog.set_level("WARNING")
    assert obs._scalar_int(BadValueConn(), "select 1") is None
    assert obs._table_row_count(BadValueConn(), "x") == (False, None)
    assert "unexpected value" in caplog.text


def test_observability_dict_rows_and_datetime_format_branches(caplog):
    class RowsConn:
        def execute(self, *_args, **_kwargs):
            return self

        def fetchall(self):
            return [(None, 1), ("missing", None), ("ok", 2)]

    class DuckErrorConn(RowsConn):
        def execute(self, *_args, **_kwargs):
            raise duckdb.Error("boom")

    class RuntimeErrorConn(RowsConn):
        def execute(self, *_args, **_kwargs):
            raise RuntimeError("boom")

    assert obs._dict_rows(RowsConn(), "select") == {"ok": 2}
    assert obs._dict_rows(DuckErrorConn(), "select") is None

    caplog.set_level("WARNING")
    assert obs._dict_rows(RuntimeErrorConn(), "select") is None
    assert "unexpected error" in caplog.text

    assert obs._normalize_dt(None) is None
    assert obs._normalize_dt(" ") is None
    assert "T" in obs._normalize_dt(obs.datetime(2026, 1, 1))


def test_snapshot_raw_layer_rejects_invalid_level():
    with pytest.raises(ValueError, match="snapshot_raw_layer level"):
        snapshot_raw_layer(conn=object(), level="deep")


def test_snapshot_dbt_models_handles_unexpected_count_value(caplog):
    class BadCountConn:
        def execute(self, *_args, **_kwargs):
            return self

        def fetchone(self):
            return ("bad-int",)

    caplog.set_level("WARNING")
    snapshot = snapshot_dbt_models(conn=BadCountConn())

    assert snapshot["polymarket_staging.stg_polymarket_markets"] == {
        "exists": False,
        "rows": None,
    }
    assert "unexpected error counting dbt model" in caplog.text


def test_formatters_render_skip_reasons_and_plain_values():
    raw = format_raw_snapshot_log(
        {
            "markets_rows": 1,
            "odds_history_max_ts": "123",
            "token_sync_skips_by_reason": {"empty": 2, "error": 1},
        }
    )
    dbt = format_dbt_snapshot_log({"plain": 3})

    assert "token_sync_skips_by_reason={empty:2,error:1}" in raw
    assert "odds_history_max_ts=123" in raw
    assert dbt == "plain=3"
