# oddsfox

[![CI](https://github.com/hypertrial/oddsfox/actions/workflows/ci.yml/badge.svg)](https://github.com/hypertrial/oddsfox/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A self-hosted, MIT-licensed FOSS data lake creator for prediction-market research.
It builds a local Parquet + DuckDB lake so analysts can make sense of Polymarket and Kalshi end-to-end.

## What it does

- Syncs Polymarket events and markets from Gamma
- Syncs Kalshi markets, candlesticks, trades, and order book snapshots
- Syncs read-only user positions/fills for user-supplied Polymarket wallets and Kalshi keys
- Stores prices, order books, trades, and resolutions locally in a medallion lake
- Computes user PnL by source or across Polymarket and Kalshi
- Computes liquidity and forecasting metrics
- Exposes CLI, SQL, local HTTP API, and minimal web UI
- Keeps the full workflow local: fetch, normalize, catalog, compute, query, and serve

## What it does not do

- Does not place trades
- Does not provide financial advice
- Does not redistribute Polymarket or Kalshi data
- Does not bypass API limits or geo restrictions
- Does not submit orders, custody wallets, or host user account data

## Install

Prebuilt release installer:

```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/hypertrial/oddsfox/releases/latest/download/oddsfox-installer.sh | sh
```

From a source checkout:

```bash
cargo install --path .
```

First build compiles bundled DuckDB and may take several minutes.

## Quickstart

One command builds a small active-market lake, creates DuckDB views, and starts the local UI:

```bash
oddsfox quickstart
```

Open <http://127.0.0.1:8787>. `quickstart` keeps serving until you stop it.

For durable hourly collection across every discovered Polymarket and Kalshi market:

```bash
oddsfox collect hourly --source all --since 2024-01-01
```

The collector stores per-token hourly cursors, so restarting the same command continues from the next uncollected UTC hour.

For active 1-minute prices over the last 24 hours (both sources):

```bash
oddsfox sync markets --active
oddsfox sync prices --active
oddsfox sync markets --source kalshi --status open
oddsfox sync prices --active --source kalshi
# or: oddsfox backfill --source all --active
```

For a longer-running full analyst lake (all markets + CLOB price history + DuckDB catalog):

```bash
oddsfox backfill --fidelity 60
oddsfox backfill --source kalshi --fidelity 60 --limit 25
```

For Kalshi:

```bash
oddsfox sync markets --source kalshi --status open --limit 100
oddsfox sync prices --source kalshi --market KXEXAMPLE-26 --series KXEXAMPLE --period 60
oddsfox sync trades --source kalshi --market KXEXAMPLE-26
oddsfox snapshot books --source kalshi --market KXEXAMPLE-26 --depth 20
```

For user PnL:

```bash
oddsfox sync user --source polymarket --user 0xabc... --limit 100
oddsfox sync user --source kalshi --limit 100
oddsfox pnl --source all --format json
```

`sync user` is safe to rerun: fills are deduplicated, latest positions win, and stored watermarks avoid refetching old activity unless `--since` is passed.

## CLI commands

| Command | Description |
|---------|-------------|
| `init` | Scaffold lake at `~/.oddsfox` |
| `backfill` | Init → sync markets → sync price history → DuckDB catalog |
| `collect hourly` | Continuously collect hourly prices with per-token resume cursors |
| `quickstart` | Init → sync → snapshot → compute → DuckDB → serve |
| `sync markets` | Sync Polymarket or Kalshi events/markets/outcomes |
| `sync prices` | Sync Polymarket CLOB or Kalshi candlestick price history |
| `sync trades` | Sync Kalshi trades |
| `sync user` | Sync read-only user fills/positions |
| `snapshot books` | Fetch order book snapshots |
| `watch` | Record WebSocket market events |
| `compute liquidity/accuracy/calibration/all` | Derive gold metrics |
| `search`, `market`, `event`, `resolved`, `top` | Explore local data |
| `check`, `repair`, `clean`, `stats`, `head` | Lake maintenance |
| `duckdb`, `sql` | Query via DuckDB |
| `pnl` | Summarize user PnL |
| `serve` | Local read-only HTTP API + UI |

## Lake layout

```text
~/.oddsfox/
  oddsfox.toml
  catalog.duckdb
  bronze/ silver/ gold/
  _raw/ _metadata/ _quarantine/
```

See [docs/](docs/README.md) for architecture, storage, schema, and API reference.

## Documentation

| Topic | Document |
|-------|----------|
| Index | [docs/README.md](docs/README.md) |
| Storage layout | [docs/storage.md](docs/storage.md) |
| Schema and joins | [docs/schema.md](docs/schema.md) |
| DuckDB and HTTP API | [docs/interfaces.md](docs/interfaces.md) |
| CLI workflows | [docs/cli.md](docs/cli.md) |
| Examples | [examples/](examples/) |

## License

MIT — see [LICENSE](LICENSE).
