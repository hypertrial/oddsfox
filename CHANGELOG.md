# Changelog

## Unreleased

### Breaking

- Removed ignored `--db` flags from `quickstart` and `serve`. Use `oddsfox duckdb` / `oddsfox sql --db` for catalog paths; `serve` reads Parquet directly.
- Incremental reads now ignore run-partitioned bronze data unless its manifest run is marked complete; `repair` quarantines orphan run partitions instead of trying a network resync.

### Changed

- Added a local Usage dashboard and `GET /usage` endpoint showing recent runs, failures, row counts, lake health issues, and suggested next commands.
- Sync and backfill progress lines (`sync markets complete`, `sync prices progress`, Kalshi price sync) now include RFC3339 UTC timestamps, matching `collect hourly` output.
- Backfill now logs DuckDB catalog registration and lake check steps after price sync completes.
- `collect hourly` now fetches price history in 7-day API chunks (Polymarket `interval=max`) instead of one call per UTC hour per token; hourly parquet layout and cursor keys are unchanged.
- Added `collect hourly --active` to collect only tokens from markets where `active = true`.
- Added `collect hourly` for durable UTC-hour price collection across Polymarket and Kalshi with per-token resume cursors.
- `quickstart` now starts the local read-only UI after building the demo lake.
- `sql` now prints tab-separated multi-column output with headers and a configurable `--limit`.
- README and CLI docs now lead with quickstart and release installer usage for analyst onboarding.
- Price sync resume now uses per-token range/fidelity checkpoints instead of file existence alone.
- Price sync skips existing token parquet files on resume when checkpoints match or when a backfill was interrupted before sync state flushed; use `--overwrite` to refetch.
- Price sync resume loads sync state once, skips tokens in a fast pre-pass, and batch-writes checkpoints instead of one read/write per token.
- Raw JSON captures are written through temp files and atomically renamed into place.

## 0.2.0 — 2026-06-28

Kalshi and user PnL release:

- Kalshi markets, events, outcomes, resolutions sync
- Kalshi candlestick prices, trades, and order book snapshots
- Shared prediction-market lake contract (`source` column, prefixed Kalshi ids)
- Read-only user fills/positions sync for Polymarket wallets and Kalshi keys
- `gold_user_pnl` and `pnl` command across sources
- DuckDB views for all bronze tables and gold metrics

## 0.1.0 — 2026-06-27

Initial release:

- Medallion lake (bronze/silver/gold) with DuckDB views
- Gamma market sync, CLOB price/book sync, WebSocket watch
- Liquidity, accuracy, and calibration metrics
- CLI, local HTTP API, minimal web UI
- Read-only — no trading
