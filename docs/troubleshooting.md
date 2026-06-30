# Troubleshooting

## DuckDB Lock Errors

Only one read-write connection can hold the DuckDB file.

Fix:

1. Stop Dagster and any Python shells using the warehouse.
2. Retry the job.
3. Use `scripts/profile_warehouse.py --snapshot-copy` for read-only inspection while another process is active.

## dbt Cannot Find Profile

Use the packaged profiles directory:

```bash
uv run make dbt-parse
```

If running dbt directly:

```bash
uv run python -m dbt.cli.main parse --project-dir dbt --profiles-dir dbt/profiles
```

## dlt Market Schema Conflict

If dlt cannot load `polymarket_raw.markets`, drop the existing table and rerun `dlt_polymarket_markets`:

```sql
DROP TABLE IF EXISTS polymarket_raw.markets;
```

The dlt asset normally handles legacy bootstrap tables automatically.

## Stale Warehouse

For local development, the simplest reset is to stop Dagster and remove the DuckDB file:

```bash
rm -f oddsfox.duckdb oddsfox.duckdb.wal oddsfox.duckdb-shm
```

Then rerun the quickstart.

## API or Network Failures

- Lower `MARKETS_REQUESTS_PER_SECOND` or `ODDS_REQUESTS_PER_SECOND`.
- Re-run the failed Dagster job; token sync state is ledgered.
- Check `polymarket_ops.pipeline_run_events` and `polymarket_ops.sync_run_metrics` for the latest run payloads.

## Large Warehouse File

DuckDB files do not always shrink after rebuilds. Stop writers, then run:

```bash
uv run python scripts/compact_warehouse.py
```
