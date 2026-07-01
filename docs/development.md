# Development

Use this page when changing code, dbt models, docs, or orchestration behavior.
OddsFox is a prediction-market pipeline; v0.1.0 development mostly touches the
Polymarket/WC2026 adapter, marts, and orchestration. For operator setup, start
with [Quickstart](quickstart.md).

## Repo Layout

| Path | Purpose |
| --- | --- |
| `src/oddsfox` | Python package for config, ingestion, storage, resources, and orchestration. |
| `dbt` | DuckDB dbt project, profiles, macros, models, and singular tests. |
| `docs` | MkDocs site content and OddsFox dark CSS. |
| `scripts` | Operator utilities for warehouse inspection, compaction, pruning, repair, and scope audits. |
| `tests` | Unit, integration, dbt, Dagster, and repo policy tests. |

## Local Setup

```bash
uv sync --extra dev
cp .env.example .env
```

Keep schedules disabled for local development unless intentionally testing live
ingestion:

```dotenv
POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED=false
POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED=false
```

## Quality Gate

Run the same checks CI runs:

```bash
uv run make lint
uv run make test
uv run make integration-dbt
uv run make docs-check
uv run make dbt-parse
uv run make dbt-build-ci
```

`dbt-build-ci` bootstraps a disposable DuckDB database under `.cache/` before
running dbt build.

## Targeted Test Commands

| Target | Use |
| --- | --- |
| `uv run make unit-core` | Config, resource, and storage unit tests. |
| `uv run make unit-ingest` | Polymarket ingestion and odds sync tests. |
| `uv run make unit-orchestration` | Dagster asset, job, and schedule tests. |
| `uv run make integration-dbt` | DuckDB and dbt smoke tests. |
| `uv run make integration-dagster` | Dagster integration smoke tests. |
| `uv run make dbt-build-ci` | Bootstrap disposable DuckDB and run dbt build. |

## Pull Request Expectations

- Keep PRs focused and update docs for behavior or operator workflow changes.
- Add or update tests for changed behavior.
- Do not commit `.env`, local DuckDB files, generated dbt targets, `site/`, or
  data exports.
- Follow [CONTRIBUTING](https://github.com/hypertrial/oddsfox/blob/main/CONTRIBUTING.md)
  for the full contribution workflow.
