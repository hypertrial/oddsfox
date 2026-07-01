<section class="of-hero">
<img class="of-hero__logo" src="assets/images/oddsfox-white.png" alt="OddsFox logo">
<div>
<h1 class="of-hero__title">OddsFox</h1>
<div class="of-hero__tagline">Open-source prediction-market data pipeline</div>
<div class="of-hero__subtitle">Dagster orchestration, dlt ingestion, DuckDB storage, dbt marts, and operator-first repair paths.</div>
</div>
</section>

<div class="of-badges">
<a class="of-badge of-badge--green" href="https://github.com/hypertrial/oddsfox/actions/workflows/ci.yml">docs passing</a>
<a class="of-badge" href="https://github.com/hypertrial/oddsfox/blob/main/pyproject.toml">python 3.10+</a>
<a class="of-badge" href="https://github.com/hypertrial/oddsfox/blob/main/LICENSE">MIT</a>
<a class="of-badge of-badge--orange" href="https://github.com/hypertrial/oddsfox/blob/main/CHANGELOG.md">v0.1.0</a>
</div>

OddsFox is an open-source, local-first data pipeline for prediction-market
data. It uses Dagster for orchestration, dlt for raw market landing, DuckDB for
the local warehouse, Python for odds sync ledgers and retry logic, and dbt for
analytics models.

The current v0.1.0 implementation starts with FIFA World Cup 2026 Polymarket
markets and odds. WC2026-specific pages document the shipped adapter, jobs, and
marts rather than the full long-term scope of the project.

## Key features

- **Local-first:** run the pipeline and inspect the warehouse on one machine.
- **Dagster-orchestrated:** assets, jobs, and schedules are explicit and testable.
- **DuckDB-backed:** raw, ops, staging, intermediate, mart, and observability schemas live in one local file.
- **dbt-tested marts:** coverage, latest odds, health, and observability models build with singular data tests.
- **Prediction-market focused:** token coverage, odds freshness, market health, and scoped marts are first-class outputs.
- **Current Polymarket adapter:** v0.1.0 ships WC2026 Polymarket discovery, CLOB odds sync, and dbt marts.
- **Safe by default:** schedules are disabled until manual jobs and dbt builds are healthy.

## Example workflow

```bash
uv sync --extra dev
cp .env.example .env
uv run make dbt-parse
uv run make dagster-dev
```

## Philosophy

OddsFox favors boring local operations over distributed infrastructure. The
warehouse is inspectable, token sync is ledgered, schedules are opt-in, and
repair scripts are part of the operator surface.

## Start here

| Goal | Page |
| --- | --- |
| Run OddsFox locally | [Getting Started](quickstart.md) |
| Understand the system | [Architecture](architecture.md) |
| Operate Dagster jobs | [Operations](operations.md) |
| Inspect data outputs | [Warehouse](warehouse.md) and [Data Contracts](data-contracts.md) |
| Contribute safely | [Development Guide](development.md) |

## Community

We appreciate focused issues and pull requests. Start with
[Community](community.md) and [Development Guide](development.md).

## License

OddsFox is licensed under the
[MIT License](https://github.com/hypertrial/oddsfox/blob/main/LICENSE).
