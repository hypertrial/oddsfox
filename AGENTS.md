# oddsfox — Agent Instructions

## Project snapshot

**oddsfox** is a read-only, local-first Polymarket analytics node (v0.1.0).

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
oddsfox serve
```

## Architecture

| Area | Key files | Responsibility |
|------|-----------|----------------|
| CLI | `src/main.rs`, `src/cli.rs` | Subcommands |
| Gamma sync | `src/gamma/`, `src/sync.rs` | Events/markets metadata |
| CLOB | `src/clob/` | REST books/prices + WebSocket watch |
| Normalize | `src/normalize/` | JSON → Arrow batches |
| Metrics | `src/metrics/` | Liquidity, accuracy, calibration |
| Lake | `src/paths.rs`, `src/parquet.rs`, `src/manifest/` | Medallion storage |
| API/UI | `src/server/`, `src/web/` | Read-only axum + static UI |

## Boundaries

### Always do

- Run `cargo test` and `cargo clippy --all-targets -- -D warnings` before finishing
- Preserve read-only scope — no trading, signing, or wallets
- Add tests for parse, schema, manifest, or metric changes

### Never do

- Add trading, order submission, or wallet integration in v0.1.x
- Ship bundled Polymarket datasets
- Skip clippy/tests to merge

## Contract golden

```bash
UPDATE_GOLDEN=1 cargo test contract_matches_golden_file
```
