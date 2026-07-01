# Troubleshooting

Use this page when a local run fails. Most fixes assume schedules are disabled
and only one process is writing to the DuckDB warehouse. The current runbooks
target the Polymarket/WC2026 implementation.

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

## dlt Market Merge Duplicate Key

If `dlt_polymarket_markets` fails with:

```text
Constraint Error: Duplicate key "id: …" violates unique constraint.
```

the warehouse likely has a legacy app-owned unique index (`idx_markets_id`) on dlt-owned `polymarket_raw.markets`. dlt merge loads manage id uniqueness via `primary_key=id`; an external unique index rejects re-discovery of existing markets.

Fix:

1. Pull the latest code (schema init and the dlt asset drop the legacy index automatically).
2. Stop Dagster, then rerun `dlt_polymarket_markets`.

Manual recovery if needed:

```sql
DROP INDEX IF EXISTS polymarket_raw.idx_markets_id;
```

The dlt asset also clears pending failed load packages before re-extracting.

## dlt ContainerInjectableContextMangled

If `dlt_polymarket_markets` fails during extract with:

```text
ContainerInjectableContextMangled: When restoring context `DestinationCapabilitiesContext` ...
```

an older build nested a second dlt pipeline (registry staging) inside the markets pipeline extract. Pull the latest code: the asset now fetches and normalizes markets before calling `dlt.run()`, so only one dlt pipeline runs at a time.

Fix:

1. Pull the latest code.
2. Stop Dagster, then rerun `dlt_polymarket_markets`.

## dlt Market Schema Conflict

If dlt cannot load `polymarket_raw.markets` because of a bootstrap schema mismatch, drop the existing table and rerun `dlt_polymarket_markets`:

```sql
DROP TABLE IF EXISTS polymarket_raw.markets;
```

The dlt asset normally handles legacy bootstrap tables automatically.

## Markets vs Snapshot Responsibilities

- `dlt_polymarket_markets` owns `polymarket_raw.markets` rows (dlt merge on `id`).
- `polymarket_markets_snapshot` refreshes the WC2026 registry and writes `polymarket_raw.market_tokens` only; it does not upsert markets rows.

If markets metadata looks stale after a snapshot run, materialize `dlt_polymarket_markets` first.

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

DuckDB files do not always shrink after rebuilds or deletes. Stop writers, then:

1. Prune old raw odds points (default: keep the trailing 365 days):

```bash
uv run make prune-odds-history
# or preview first:
uv run python scripts/prune_odds_history.py --dry-run
```

2. Reclaim dead space left in the file:

```bash
uv run make compact-warehouse
```

After upgrading to a version that drops redundant `odds_history` indexes, the next
read-write process start removes ~1.45 GiB of legacy index data automatically; run
`make compact-warehouse` once to shrink the file on disk.
