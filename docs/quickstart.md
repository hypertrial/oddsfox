# Quickstart

## 1. Install

```bash
uv sync --extra dev
```

The default warehouse is `oddsfox.duckdb` in the repo root.

## 2. Configure

```bash
cp .env.example .env
```

For a local dry run, keep schedules disabled:

```dotenv
POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED=false
POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED=false
```

CLOB credentials are optional unless a live authenticated flow requires them.

## 3. Validate dbt

```bash
uv run make dbt-parse
```

To build models before live dlt ingestion, initialize the DuckDB bootstrap tables first:

```bash
uv run python - <<'PY'
import oddsfox.storage.duckdb.connection as connection
connection._SCHEMA_INITIALIZED = False
connection._SCHEMA_LOGGED = False
connection.init_duck_db()
PY
uv run make dbt-build
```

## 4. Start Dagster

```bash
uv run make dagster-dev
```

Open the Dagster UI shown in the terminal. Materialize `dlt_polymarket_markets` before `polymarket_markets_snapshot`.

## 5. Run the Pipeline

For a full manual run, launch `wc2026_polymarket_full_pipeline`.

For a safer staged run:

1. `polymarket_ingest_full_refresh_events`
2. `polymarket_minutely_odds_ingest`
3. `dbt_full_refresh`

Leave schedules off until these jobs complete successfully.
