# Metadata and manifests

## Purpose

The `_metadata/` directory is oddsfox's local catalog: run commit log, sync checkpoints, schema registry, data-quality findings, and the published lake contract. Analysts use it to understand what synced successfully and to resume incremental jobs.

Path helpers: [`src/paths.rs`](../src/paths.rs). Record shapes: [`src/manifest/records.rs`](../src/manifest/records.rs).

## Metadata files

| File | Purpose |
|------|---------|
| `contract.json` | Published bronze + metadata schema contract |
| `runs.parquet` | Commit log for sync/compute commands |
| `sync_state.parquet` | Per-token price checkpoints and user sync watermarks |
| `schemas.parquet` | Registered Arrow schemas per bronze table |
| `data_quality.parquet` | Quality check results from `compute all` |
| `version.parquet` | oddsfox, schema, and layout version stamps |
| `.oddsfox.lock` | Single-writer lock during manifest updates |

DuckDB catalog file `catalog.duckdb` lives at the **lake root** (not under `_metadata/`). See [storage.md](storage.md) and [interfaces.md](interfaces.md).

## Run commit log (`runs.parquet`)

Each sync, snapshot, compute, or watch command:

1. Appends a row with `status=started`
2. Writes Parquet under `run=<run_id>/`
3. Appends `status=complete` with `rows_written`, or `status=failed` on error

Fields: `run_id`, `command`, `started_at`, `finished_at`, `status`, `rows_written`, `oddsfox_version`.

Run-partitioned bronze and gold tables are visible to DuckDB **only** when their `run_id` appears in completed runs. Crashed jobs leave orphan partitions that `check` reports and `repair` quarantines.

```bash
oddsfox check --out ~/.oddsfox
```

## Sync state (`sync_state.parquet`)

Tracks incremental cursors so reruns skip already-fetched data.

| Use | `source` | `cursor_key` | `cursor_value` |
|-----|----------|--------------|----------------|
| Price resume | e.g. `polymarket` | token id | range + fidelity checkpoint |
| User fill watermark | user source | user id | last-seen fill timestamp |

Passing `--since` on `sync user` overrides the stored watermark. Price sync uses `--overwrite` to ignore checkpoints.

## Schema registry (`schemas.parquet`)

One row per bronze table: `table`, `schema_version`, `column_count`, `columns_json`, `updated_at`. Updated when tables are written.

Inspect live Arrow types:

```bash
oddsfox schema markets
oddsfox schema prices
```

## Data quality (`data_quality.parquet`)

Populated by quality checks during `compute all`. Each row: `check_name`, `entity_type`, `entity_id`, `severity`, `message`, `checked_at`.

Example checks include unresolved closed markets and markets missing outcomes.

## Version stamp (`version.parquet`)

Records `oddsfox_version`, `schema_version` (`prediction-market-v3`), `lake_layout_version` (`medallion-v2`), `created_at`, `updated_at`.

## Lake contract (`contract.json`)

Machine-readable schema contract for bronze tables and metadata. Bump `lake_contract_version()` in [`src/contract/mod.rs`](../src/contract/mod.rs) on breaking column changes.

```bash
oddsfox contract --out ~/.oddsfox
oddsfox contract --out ~/.oddsfox > /tmp/contract.json
```

Human-readable column listing also lives in [`tests/contract.golden.json`](../tests/contract.golden.json) (used by CI).

## Inspecting metadata

Row counts per bronze table:

```bash
oddsfox stats --out ~/.oddsfox
```

Health check (incomplete runs, orphan partitions, temp files, missing contract):

```bash
oddsfox check --out ~/.oddsfox
oddsfox repair --out ~/.oddsfox
```

Query manifest data via DuckDB after registering views, or read Parquet directly:

```bash
oddsfox duckdb --out ~/.oddsfox
# In DuckDB shell:
# SELECT * FROM read_parquet('~/.oddsfox/_metadata/runs.parquet') ORDER BY started_at DESC LIMIT 10;
```

## Related docs

- [storage.md](storage.md) — where run and token partitions live
- [schema.md](schema.md) — table semantics
- [architecture.md](architecture.md) — run-commit overview
