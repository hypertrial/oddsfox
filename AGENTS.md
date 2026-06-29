# oddsfox — Agent Instructions

## Project snapshot

**oddsfox** is a read-only, local-first prediction-market analytics node (v0.2.0).

- **Stack:** Rust 2021, Tokio, Clap, Arrow/Parquet 58, bundled DuckDB, reqwest, axum, fixture integration tests
- **Pattern:** Mirrors [gdeltlake](../gdeltlake/) medallion lake conventions
- **User docs:** [README.md](README.md), [docs/](docs/README.md)
- **Agent style:** [Ponytail](https://github.com/DietrichGebert/ponytail) — lazy senior dev mode (`.cursor/rules/ponytail.mdc`)

## Commands

```bash
cargo test --verbose
cargo clippy --all-targets -- -D warnings
```

Local binary note:

- `cargo build` updates `target/debug/oddsfox`, not the `oddsfox` binary on PATH.
- If `oddsfox <new-command>` reports `unrecognized subcommand`, compare `./target/debug/oddsfox --help` with `oddsfox --help`.
- Run `cargo install --path .` when the PATH binary should use the current checkout.

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

Durable hourly collection (per-token resume cursors):

```bash
oddsfox collect hourly --source all --since 2024-01-01
oddsfox collect hourly --source all --once
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

## Evolution policy

**No legacy support.** oddsfox is pre-1.0 and moves forward only.

- **Delete, don't shim.** Remove dead flags, compat aliases, ignored CLI options, and deprecated code paths instead of warning or no-oping.
- **Breaking changes are expected.** CLI flags, lake layout, and schemas may change without migration layers. Update docs and tests in the same PR.
- **No backward-compat branches.** Do not add `if old_behavior` forks, silent fallbacks for removed flags, or dual code paths "for users on the old command."
- **Prefer one correct path.** When behavior changes (e.g. `serve` reads Parquet directly), remove the old surface entirely rather than accepting it and printing a warning.
- **Document breaks in CHANGELOG** when user-visible; internal refactors need no entry.

## Boundaries

### Always do

- Run `cargo test` and `cargo clippy --all-targets -- -D warnings` before finishing
- Preserve read-only scope — no trading, signing, or wallets
- Add tests for parse, schema, manifest, or metric changes
- Follow [Ponytail](https://github.com/DietrichGebert/ponytail): minimum code that works; deletion over addition; reuse before rewrite

### Never do

- Add trading, order submission, or wallet integration in v0.2.x
- Ship bundled Polymarket datasets
- Skip clippy/tests to merge
- Preserve legacy CLI flags, compat shims, or deprecated APIs "just in case"

## Contract golden

```bash
UPDATE_GOLDEN=1 cargo test contract_matches_golden_file
```
