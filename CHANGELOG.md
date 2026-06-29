# Changelog

## Unreleased

### Breaking

- Removed ignored `--db` flags from `quickstart` and `serve`. Use `oddsfox duckdb` / `oddsfox sql --db` for catalog paths; `serve` reads Parquet directly.
- Incremental reads now ignore run-partitioned bronze data unless its manifest run is marked complete; `repair` quarantines orphan run partitions instead of trying a network resync.

### Changed

- Price sync resume now uses per-token range/fidelity checkpoints instead of file existence alone.
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
