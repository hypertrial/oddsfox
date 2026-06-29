# Storage layer

## Purpose

The storage layer defines how oddsfox lays out Parquet on disk across bronze and gold tiers. Parquet is the canonical format — columnar, analytical, and compatible with DuckDB, Polars, and Pandas.

Authoritative path helpers live in [`src/paths.rs`](../src/paths.rs). Lake layout version: `medallion-v2`.

## Current state (v0.2)

- **Medallion layout:** `bronze/`, `gold/`, optional `silver/` (scaffolded, not populated in v0.2)
- **Bronze:** ten prediction-market tables under `bronze/{table}/`
- **Gold:** derived metrics and user PnL under `gold/{name}/`
- **Raw cache:** `_raw/{source}/` for API and WebSocket captures
- **Metadata:** `_metadata/` for manifests, contract, and DuckDB catalog
- **Quarantine:** `_quarantine/bad_rows/`, `_quarantine/bad_files/`, `_quarantine/orphan_runs/`
- **Compression:** ZSTD ([`src/parquet_props.rs`](../src/parquet_props.rs))
- **Atomic writes:** temp `*.parquet.tmp` files renamed into place

Inspect row counts:

```bash
oddsfox stats --out ~/.oddsfox
```

## Full lake layout

Default lake root is `~/.oddsfox` (override with `--out` or `[data].home` in config).

```text
~/.oddsfox/
  oddsfox.toml
  catalog.duckdb
  logs/

  bronze/
    events/run=<run_id>/part.parquet
    markets/run=<run_id>/part.parquet
    outcomes/run=<run_id>/part.parquet
    resolutions/run=<run_id>/part.parquet
    orderbooks/run=<run_id>/part.parquet
    book_levels/run=<run_id>/part.parquet
    trades/run=<run_id>/part.parquet
    user_fills/run=<run_id>/part.parquet
    user_positions/run=<run_id>/part.parquet
    prices/token=<token_id>/part.parquet

  gold/
    metric_points/run=<run_id>/part.parquet
    calibration/run=<run_id>/part.parquet
    accuracy/run=<run_id>/part.parquet
    liquidity_rollup/run=<run_id>/part.parquet   # view registered when present
    user_pnl/run=<run_id>/part.parquet

  silver/                    # scaffolded; no v0.2 writes yet

  _raw/
    gamma/ ...
    clob/ ...
    kalshi/ ...
    websocket/ ...

  _metadata/
    contract.json
    runs.parquet
    sync_state.parquet
    schemas.parquet
    data_quality.parquet
    version.parquet
    .oddsfox.lock

  _quarantine/
    bad_rows/{table}/bad_rows-<run_id>.jsonl
    bad_files/{source}/...
    orphan_runs/...
```

See [metadata.md](metadata.md) for manifest file semantics.

## Partitioning rules

### Run-partitioned snapshots

Most bronze tables and all gold tables use **run snapshots**:

```text
bronze/{table}/run=<run_id>/part.parquet
gold/{name}/run=<run_id>/part.parquet
```

Tables written this way: `events`, `markets`, `outcomes`, `resolutions`, `orderbooks`, `book_levels`, `trades`, `user_fills`, `user_positions`.

Each sync or compute command appends a run to `_metadata/runs.parquet`. DuckDB bronze views filter to **completed** runs only — partial runs from crashes are invisible until `oddsfox repair` quarantines orphan partitions. See [architecture.md](architecture.md) and [cli.md](cli.md#restart-behavior).

### Token-partitioned prices

`bronze/prices` is the exception: one file per outcome token.

```text
bronze/prices/token=<token_id>/part.parquet
```

Price sync merges into existing token files using per-token checkpoints in `_metadata/sync_state.parquet`. Re-running the same range/fidelity is a no-op; existing token parquet is also skipped when checkpoints were not flushed yet (for example Ctrl+C after price sync); use `--overwrite` to refetch. Active rolling sync merges inside the requested window instead of skipping.

`collect hourly` writes one file per token per UTC hour under a separate windows tree (same bronze `prices` schema):

```text
bronze/prices/windows/{source}/{token_id}/{start_ts}.parquet
```

Analysts normally do not read these paths directly. Query `bronze_prices`; DuckDB reads both legacy token partitions and hourly window files without a schema change. Re-running the same token/hour replaces the deterministic hourly file, so restart recovery does not create duplicate stored rows.

### Silver layer

`silver/` directories are created by `oddsfox init` but v0.2 does not write silver Parquet. Normalization happens at ingest into bronze.

## Bronze table inventory

| Table | Purpose |
|-------|---------|
| `events` | Event metadata (title, tags, lifecycle) |
| `markets` | Market questions, volume, liquidity, resolution flags |
| `outcomes` | Outcome names and `token_id` per market |
| `prices` | Time series of implied probabilities per token |
| `orderbooks` | Book snapshot summaries (spread, depth) |
| `book_levels` | Individual bid/ask levels per snapshot |
| `trades` | Kalshi trade prints |
| `resolutions` | Winning outcome and resolution timestamps |
| `user_fills` | Read-only user trade fills |
| `user_positions` | Read-only user position snapshots |

Column details: [schema.md](schema.md).

## Gold table inventory

| Table | Written by | Purpose |
|-------|------------|---------|
| `metric_points` | `compute liquidity` | Spread, depth, relative spread per token |
| `calibration` | `compute calibration` | Prediction buckets vs observed resolution rates |
| `accuracy` | `compute accuracy` | Brier score and log loss on resolved markets |
| `user_pnl` | `sync user` / PnL refresh | Per-user, per-market PnL rollup |
| `liquidity_rollup` | — | DuckDB view slot; not populated separately in v0.2 |

## Raw and quarantine

- **`_raw/`** — optional JSON captures from upstream APIs and WebSocket `watch` sessions, retained per `[data].raw_retention_days`.
- **`_quarantine/bad_rows/`** — rows that failed validation during ingest.
- **`_quarantine/bad_files/`** — unparseable raw files.
- **`_quarantine/orphan_runs/`** — run partitions moved by `oddsfox repair` when no matching completed manifest run exists.

```bash
oddsfox check --out ~/.oddsfox
oddsfox repair --out ~/.oddsfox
```

## Related docs

- [metadata.md](metadata.md) — runs, sync state, contract
- [schema.md](schema.md) — columns and join keys
- [interfaces.md](interfaces.md) — DuckDB views over this layout
