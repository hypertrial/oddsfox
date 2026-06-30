# OddsFox

OddsFox v0.1.0 is a local Python data project for FIFA World Cup 2026 Polymarket market and odds ingestion.

It uses Dagster, dlt, dbt, and DuckDB. The repo intentionally excludes optional soccer context sources, simulations, allocation tooling, website integration, and generated historical docs.

## Operator Docs

- [Operator manual](docs/index.md)
- [Quickstart](docs/quickstart.md)
- [Operations](docs/operations.md)
- [Warehouse](docs/warehouse.md)
- [Configuration](docs/configuration.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Scripts](docs/scripts.md)

## Common Commands

```bash
uv sync --extra dev
uv run make docs-serve
uv run make dagster-dev
uv run make dbt-parse
uv run make dbt-build
uv run make test
```

Schedules are disabled by default and controlled through `.env`.
