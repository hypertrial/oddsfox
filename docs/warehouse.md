# Warehouse

The local warehouse is DuckDB. By default it is `oddsfox.duckdb` in the repo root.

## Raw Tables

Schema: `polymarket_raw`

- `markets`: dlt-owned Gamma market landing table.
- `market_tokens`: one row per market with CLOB token JSON.
- `odds_history`: point-in-time CLOB token prices.
- `token_odds_daily`: daily token aggregates.

## Ops Tables

Schema: `polymarket_ops`

- `wc2026_market_registry`: market ids admitted to WC2026 scope.
- `token_sync_ledger`: per-token sync progress.
- `token_sync_skips`: persisted skip reasons.
- `pipeline_run_events`: append-only run metrics.
- `sync_run_metrics`: latest sync metrics and short history.
- `scrape_metadata`: small key/value metadata used by legacy-compatible helpers.
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
- `token_latest_odds`: latest daily and point-in-time odds per WC2026 token.
- `wc2026_markets`: scoped WC2026 market universe.
- `wc2026_whale_minutely_odds`: minutely odds for high-volume WC2026 markets.

Schema: `polymarket_observability`

- `sync_run_observability`: run-level ingestion and odds-sync telemetry.

## dlt-Owned Markets Table

`polymarket_raw.markets` is created by `dlt_polymarket_markets`.

Local dbt smoke builds may create an empty bootstrap table before dlt runs. The dlt asset drops that legacy bootstrap table when it lacks dlt metadata columns, then recreates the table through dlt.

Manual reset:

```sql
DROP TABLE IF EXISTS polymarket_raw.markets;
```

Then materialize `dlt_polymarket_markets`.
