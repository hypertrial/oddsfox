# oddsfox — Agent Instructions

## Project snapshot

**oddsfox** is a read-only, local-first prediction-market analytics node (v0.2.0).

- **Stack:** Rust 2021, Tokio, Clap, Arrow/Parquet 58, bundled DuckDB, reqwest, axum, fixture integration tests
- **Pattern:** Mirrors [gdeltlake](../gdeltlake/) medallion lake conventions
- **User docs:** [README.md](README.md), [docs/](docs/README.md)

## Commands

```bash
cargo test --verbose
cargo clippy --all-targets -- -D warnings
```

Optional smoke (needs network):

```bash
oddsfox init
oddsfox sync markets --active
oddsfox sync prices --active
oddsfox sync user --source polymarket --user 0xabc... --limit 100
oddsfox pnl --source all --format json
oddsfox serve
```

Active minute refresh (last 24h, both sources):

```bash
oddsfox backfill --source all --active
```

Analyst backfill (needs network; long-running):

```bash
oddsfox backfill --fidelity 60 --limit 10
oddsfox backfill --source kalshi --fidelity 60 --limit 25
```

## Architecture

| Area | Key files | Responsibility |
|------|-----------|----------------|
| CLI | `src/main.rs`, `src/cli.rs` | Subcommands |
| Gamma sync | `src/gamma/`, `src/sync.rs` | Polymarket events/markets metadata |
| Kalshi sync | `src/kalshi/` | Kalshi markets, prices, trades, books |
| CLOB | `src/clob/` | REST books/prices + WebSocket watch |
| Normalize | `src/normalize/` | JSON → Arrow batches |
| Metrics | `src/metrics/` | Liquidity, accuracy, calibration |
| Lake | `src/paths.rs`, `src/parquet.rs`, `src/manifest/` | Medallion storage |
| User PnL | `src/user/`, `src/data/` | Read-only user fills/positions + PnL |
| API/UI | `src/server/`, `src/web/` | Read-only axum + static UI |

## Boundaries

### Always do

- Run `cargo test` and `cargo clippy --all-targets -- -D warnings` before finishing
- Preserve read-only scope — no trading, signing, or wallets
- Add tests for parse, schema, manifest, or metric changes

### Never do

- Add trading, order submission, or wallet integration in v0.2.x
- Ship bundled Polymarket datasets
- Skip clippy/tests to merge

## Contract golden

```bash
UPDATE_GOLDEN=1 cargo test contract_matches_golden_file
```
