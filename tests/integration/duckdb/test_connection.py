import duckdb

import oddsfox.storage.duckdb.connection as connection
from oddsfox.storage.duckdb.connection import init_duck_db


def test_init_duck_db_creates_polymarket_schemas_only(tmp_path, monkeypatch):
    db_path = tmp_path / "oddsfox.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    connection.reset_duckdb_connection_state()

    init_duck_db()

    with duckdb.connect(str(db_path)) as conn:
        schemas = {
            row[0]
            for row in conn.execute(
                """
                select schema_name
                from information_schema.schemata
                where schema_name not in ('information_schema', 'main', 'pg_catalog')
                """
            ).fetchall()
        }
        tables = {
            (row[0], row[1])
            for row in conn.execute(
                """
                select table_schema, table_name
                from information_schema.tables
                where table_schema in ('polymarket_raw', 'polymarket_ops')
                """
            ).fetchall()
        }

    assert schemas == {"polymarket_raw", "polymarket_ops"}
    assert {
        ("polymarket_raw", "market_tokens"),
        ("polymarket_raw", "odds_history"),
        ("polymarket_raw", "token_odds_daily"),
        ("polymarket_ops", "wc2026_market_registry"),
        ("polymarket_ops", "token_sync_ledger"),
        ("polymarket_ops", "token_sync_skips"),
        ("polymarket_ops", "pipeline_run_events"),
        ("polymarket_ops", "sync_run_metrics"),
    } <= tables
    assert ("polymarket_raw", "markets") not in tables
