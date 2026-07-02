# Warehouse

The local warehouse is DuckDB. By default it is `oddsfox.duckdb` in the repo
root. OddsFox is designed for prediction-market data; the v0.1.x warehouse
schemas and relation names are Polymarket-specific because that is the first
shipped adapter. For public mart guarantees, see
[Data Contracts](data-contracts.md).

## Raw Tables

Schema: `polymarket_raw`

- `markets`: dlt-owned Gamma market landing table with frozen column/type contract.
- `market_tokens`: one row per market with CLOB token JSON; current batches land through dlt staging and finalize into this canonical table with `INSERT OR REPLACE`.
- `odds_history`: point-in-time CLOB token prices. Indexed by the composite
  primary key `(clobTokenId, timestamp)` for idempotent upserts. Operators may
  prune rows older than 365 days with `make prune-odds-history` (manual; not automatic).
  Current batches land through dlt staging, then finalize with duplicate `(clobTokenId, timestamp)` last-write-wins semantics.
- `token_odds_daily`: daily token aggregates rebuilt by custom SQL finalizers from
  canonical `odds_history`.

## Ops Tables

Schema: `polymarket_ops`

- `wc2026_market_registry`: market ids admitted to WC2026 scope; current batches
  land through dlt staging before the canonical upsert preserves existing non-null event fields.
- `token_sync_ledger`: per-token sync progress kept in custom SQL because cursor
  and scheduler-state merges are stateful.
- `token_sync_skips`: persisted skip reasons kept in custom SQL to preserve `created_at`.
- `pipeline_run_events`: append-only run metrics landed through dlt staging.
- `sync_run_metrics`: latest sync metrics and short history.
- `scrape_metadata`: small key/value metadata used by backfill progress helpers.
- `market_metadata_unresolved`: retry ledger for unresolved metadata fields.

## dbt Schemas

- `polymarket_staging`
- `polymarket_intermediate`
- `polymarket_marts`
- `polymarket_observability`

## dbt Intermediate

Schema: `polymarket_intermediate`

- `int_polymarket_token_universe`: canonical one-row-per-token join of market tokens to market labels, state, and volume.
- `int_polymarket_wc2026_token_universe`: WC2026-scoped subset of the token universe.
- `int_polymarket_token_timeseries` / `int_polymarket_token_daily_timeseries`: token-level point and daily odds joined to the token universe.

## dbt Marts

Schema: `polymarket_marts`

- `market_coverage`: market-level daily odds coverage rolled up from `token_coverage`.
- `token_coverage`: token-level health and coverage, including daily coverage,
  sync ledger state, persisted skip reason, gap diagnostics, and market fully
  checked rollups.
- `wc2026_token_minutely_odds`: full minutely odds time series for all WC2026 tokens (dbt
  view over `int_polymarket_token_timeseries`; not materialized to save disk).
- `wc2026_token_daily_odds`: full daily OHLC odds time series for all WC2026 tokens (dbt
  view over `int_polymarket_token_daily_timeseries`; not materialized to save disk).
- `wc2026_markets`: scoped WC2026 market universe.
- `wc2026_whale_minutely_odds`: minutely odds for high-volume WC2026 markets (dbt
  view over `wc2026_token_minutely_odds`; not materialized to save disk).

Schema: `polymarket_observability`

- `sync_run_observability`: run-level ingestion and odds-sync telemetry.

## dlt Landing And Canonical Tables

Canonical raw and ops table names and schemas remain stable. dlt owns batch
landing for `markets`, `market_tokens`, `odds_history`,
`wc2026_market_registry`, and `pipeline_run_events`; stage tables and `_dlt*`
metadata tables are internal implementation details.

`polymarket_raw.markets` is created by `dlt_polymarket_markets`. The
`dbt-build-ci` target creates an empty source fixture only in its disposable
DuckDB database.

Manual reset:

```sql
DROP TABLE IF EXISTS polymarket_raw.markets;
```

Then materialize `dlt_polymarket_markets`.
