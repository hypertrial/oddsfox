# OddsFox

[![CI](https://github.com/hypertrial/oddsfox/actions/workflows/ci.yml/badge.svg)](https://github.com/hypertrial/oddsfox/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.10%2B-00d7f7)](pyproject.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-00d7f7)](LICENSE)

OddsFox is an open-source, local-first data pipeline for prediction-market
data.

It uses Dagster to orchestrate dlt ingestion, DuckDB storage, Python sync
ledgers, and dbt analytics models. Version `0.1.0` starts with FIFA World Cup
2026 Polymarket markets and odds, but the project direction is broader:
inspectable prediction-market pipelines that run locally first.

## Quickstart

```bash
uv sync --extra dev
cp .env.example .env
uv run make dbt-parse
uv run make dagster-dev
```

Schedules are disabled by default. Keep them off until manual jobs and dbt
builds are healthy.

Read the full [Getting Started guide](docs/quickstart.md).

## Architecture

OddsFox keeps the data stack local and inspectable:

- Prediction-market APIs feed raw DuckDB tables; v0.1.0 ships Polymarket
  Gamma and CLOB ingestion.
- Dagster coordinates market discovery, registry refresh, odds sync, and dbt.
- dbt models staging, intermediate, mart, and observability schemas.
- Operator scripts inspect, compact, prune, and repair local warehouse state.

See [Architecture](docs/architecture.md) and [Warehouse](docs/warehouse.md).

## Data Outputs

Current Polymarket analytics outputs live in `polymarket_marts`:

- `token_coverage`: token-level coverage and health.
- `market_coverage`: market-level daily coverage rollup.
- `wc2026_token_minutely_odds`: full WC2026 minutely odds time series.
- `wc2026_token_daily_odds`: full WC2026 daily OHLC odds time series.
- `wc2026_markets`: scoped WC2026 market universe.
- `wc2026_whale_minutely_odds`: high-volume WC2026 minutely odds.

See [Data Contracts](docs/data-contracts.md).

## Development

```bash
uv run make lint
uv run make test
uv run make docs-check
uv run make dbt-parse
```

For full dbt build parity with CI, initialize DuckDB bootstrap tables before
`uv run make dbt-build`. See [Development](docs/development.md) and
[CONTRIBUTING.md](CONTRIBUTING.md).

## Community

- [Docs](docs/index.md)
- [Contributing](CONTRIBUTING.md)
- [Security](SECURITY.md)
- [Changelog](CHANGELOG.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [License](LICENSE)

The v0.1.0 repo intentionally excludes optional soccer context sources,
simulations, allocation tooling, website integration, and generated historical
docs.
