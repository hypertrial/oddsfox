import duckdb

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
    conn_mod._SCHEMA_INITIALIZED = False
    conn_mod._SCHEMA_LOGGED = False
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
