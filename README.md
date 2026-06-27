# oddsfox

[![CI](https://github.com/hypertrial/oddsfox/actions/workflows/ci.yml/badge.svg)](https://github.com/hypertrial/oddsfox/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A local-first Rust research kit for Polymarket.

## What it does

- Syncs Polymarket events and markets from Gamma
- Stores prices, order books, trades, and resolutions locally in a medallion lake
- Computes liquidity and forecasting metrics
- Exposes CLI, SQL, local HTTP API, and minimal web UI

## What it does not do

- Does not place trades
- Does not provide financial advice
- Does not redistribute Polymarket data
- Does not bypass API limits or geo restrictions

## Install

```bash
cargo install --path .
```

First build compiles bundled DuckDB and may take several minutes.

## Quickstart

```bash
oddsfox init
oddsfox sync markets --active
oddsfox snapshot books --active --top-volume 50
oddsfox sync prices --active --interval 1d --fidelity 60
oddsfox compute liquidity --active
oddsfox serve
```

Or one shot:

```bash
oddsfox quickstart
```

## CLI commands

| Command | Description |
|---------|-------------|
| `init` | Scaffold lake at `~/.oddsfox` |
| `quickstart` | Init → sync → snapshot → compute → duckdb |
| `sync markets` | Sync Gamma events/markets/outcomes |
| `sync prices` | Sync CLOB price history |
| `snapshot books` | Fetch order book snapshots |
| `watch` | Record WebSocket market events |
| `compute liquidity/accuracy/calibration/all` | Derive gold metrics |
| `search`, `market`, `event`, `resolved`, `top` | Explore local data |
| `check`, `repair`, `clean`, `stats` | Lake maintenance |
| `duckdb`, `sql` | Query via DuckDB |
| `serve` | Local read-only HTTP API + UI |

## Lake layout

```text
~/.oddsfox/
  oddsfox.toml
  catalog.duckdb
  bronze/ silver/ gold/
  _raw/ _metadata/ _quarantine/
```

See [docs/](docs/README.md) for architecture and roadmap.

## License

MIT — see [LICENSE](LICENSE).
