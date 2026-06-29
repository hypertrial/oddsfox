# oddsfox

[![CI](https://github.com/hypertrial/oddsfox/actions/workflows/ci.yml/badge.svg)](https://github.com/hypertrial/oddsfox/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A local-first prediction-market analytics lake for Polymarket and Kalshi.
oddsfox fetches market data into Parquet, exposes DuckDB views, and gives analysts a CLI, SQL, local API, and small web UI.

## What it does

- Builds a local Parquet lake for events, markets, outcomes, prices, books, trades, resolutions, and user PnL
- Collects Polymarket Gamma/CLOB data and Kalshi market-data reads
- Runs durable hourly collection with per-token resume cursors
- Computes liquidity, accuracy, calibration, and user PnL outputs
- Keeps analysis local: fetch, normalize, query, serve

## What it does not do

- Does not place trades or provide financial advice
- Does not custody wallets, submit orders, or sign trades
- Does not redistribute Polymarket or Kalshi data
- Does not bypass API limits, access controls, or geo restrictions

## Install

Prebuilt release installer:

```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/hypertrial/oddsfox/releases/latest/download/oddsfox-installer.sh | sh
```

From a source checkout:

```bash
cargo install --path .
```

First source build compiles bundled DuckDB and may take several minutes. `cargo build` only updates `target/debug/oddsfox`; run `cargo install --path .` when you want the `oddsfox` command on your PATH to use the checkout.

## Start Here

### 1. Try it now

Build a small active-market lake and start the local UI:

```bash
oddsfox quickstart
```

Open <http://127.0.0.1:8787>. `quickstart` keeps serving until you stop it.

### 2. Collect production data

Run durable hourly collection across every discovered Polymarket and Kalshi market:

```bash
oddsfox collect hourly --source all --since 2024-01-01
```

The first run requires `--since`. Restarting the same command resumes from each token's next uncollected UTC hour. For cron or CI, run one catch-up pass:

```bash
oddsfox collect hourly --source all --once
```

### 3. Refresh active markets

For a rolling 24-hour active-market refresh at 1-minute fidelity:

```bash
oddsfox backfill --source all --active
```

Equivalent explicit commands:

```bash
oddsfox sync markets --active
oddsfox sync prices --active --source polymarket
oddsfox sync markets --source kalshi --status open
oddsfox sync prices --active --source kalshi
```

### 4. Query the lake

Print TSV with headers:

```bash
oddsfox sql "SELECT market_id, question, volume_24h FROM bronze_markets ORDER BY volume_24h DESC NULLS LAST" --limit 10
```

Open DuckDB views:

```bash
oddsfox duckdb --out ~/.oddsfox
```

Serve the local API and UI:

```bash
oddsfox serve --port 8787
```

### 5. Optional analyst workflows

Kalshi single-market workflow:

```bash
oddsfox sync markets --source kalshi --status open --limit 100
oddsfox sync prices --source kalshi --market KXEXAMPLE-26 --series KXEXAMPLE --period 60
oddsfox sync trades --source kalshi --market KXEXAMPLE-26
oddsfox snapshot books --source kalshi --market KXEXAMPLE-26 --depth 20
```

User PnL:

```bash
oddsfox sync user --source polymarket --user 0xabc... --limit 100
oddsfox sync user --source kalshi --limit 100
oddsfox pnl --source all --format json
```

Metrics:

```bash
oddsfox compute liquidity --active
oddsfox compute accuracy --since 2024-01-01
oddsfox compute calibration --since 2024-01-01
```

## Which command should I use?

| Analyst task | Command |
|--------------|---------|
| First local demo | `oddsfox quickstart` |
| Durable all-market hourly collection | `oddsfox collect hourly --source all --since YYYY-MM-DD` |
| One collector catch-up pass | `oddsfox collect hourly --source all --once` |
| Rolling active-market refresh | `oddsfox backfill --source all --active` |
| One source/range price fetch | `oddsfox sync prices ...` |
| User PnL inputs and rollup | `oddsfox sync user`, then `oddsfox pnl` |
| SQL from the shell | `oddsfox sql "SELECT ..."` |
| Interactive DuckDB | `oddsfox duckdb` |
| Local API and UI | `oddsfox serve` |
| Lake health | `oddsfox check`, `oddsfox repair`, `oddsfox stats`, `oddsfox head` |

## Lake Layout

```text
~/.oddsfox/
  oddsfox.toml
  catalog.duckdb
  bronze/ silver/ gold/
  _raw/ _metadata/ _quarantine/
```

Main SQL views:

- `bronze_markets`, `bronze_outcomes`, `bronze_prices`
- `bronze_orderbooks`, `bronze_book_levels`, `bronze_trades`, `bronze_resolutions`
- `gold_metric_points`, `gold_calibration`, `gold_accuracy`, `gold_user_pnl`

## Documentation

| Need | Document |
|------|----------|
| Analyst docs index | [docs/README.md](docs/README.md) |
| Workflows and commands | [docs/cli.md](docs/cli.md) |
| SQL, DuckDB, API | [docs/interfaces.md](docs/interfaces.md) |
| Tables and joins | [docs/schema.md](docs/schema.md) |
| Storage layout | [docs/storage.md](docs/storage.md) |
| Operations and cursors | [docs/operations.md](docs/operations.md) |
| Copy-paste SQL | [examples/starter_queries.sql](examples/starter_queries.sql) |

## License

MIT - see [LICENSE](LICENSE).
