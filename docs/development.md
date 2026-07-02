# Development

Use this page when changing code, dbt models, docs, or orchestration behavior.
OddsFox is a prediction-market pipeline; v0.1.x development mostly touches the
Polymarket/WC2026 adapter, marts, and orchestration. For operator setup, start
with [Quickstart](quickstart.md).

## Repo Layout

| Path | Purpose |
| --- | --- |
| `src/oddsfox` | Python package for config, ingestion, storage, resources, and orchestration. |
| `dbt` | DuckDB dbt project, profiles, macros, models, and data tests. |
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
uv run make costguard
```

`dbt-build-ci` bootstraps a disposable DuckDB database under `.cache/` before
running dbt build.
Costguard is a dbt/CI guardrail, not an odds ingestion runtime dependency.
Install the pinned local scanner with:

```bash
curl -fsSL https://raw.githubusercontent.com/hypertrial/costguard/main/scripts/install.sh | sh -s -- v2.5.0
```

Two stricter local gates are available but not enforced by GitHub Actions yet:

```bash
uv run make integration-dagster
uv run make coverage
```

`integration-dagster` executes every registered public Dagster job with
deterministic temp resources. `coverage` enforces 100% branch coverage for
product-core package code; warehouse profiling helpers are smoke-tested instead.
These gates are intentionally local for now so GitHub Actions stays aligned
with the current CI budget.

Costguard medium/low findings are measured dbt debt, not automatic
materialization work. Before changing dbt materializations or adding
incremental models, capture the failing advisory, dbt build runtime, and
warehouse/profile size evidence that justifies the change.

## dbt Materialization Debt

Treat Costguard medium/low advisories as measurement prompts. Current measured
debt includes:

- `SQLCOST039`: `int_polymarket_token_universe` is a heavily reused view.
- `SQLCOST040`: `token_coverage` and `wc2026_markets` rebuild as full tables.
- Low advisories: repeated CTEs in mart comparison tests and an `ORDER BY`
  without `LIMIT` in `token_coverage`.

Do not change materializations on advisory text alone. Capture dbt build
runtime, relevant relation sizes from `scripts/profile_warehouse.py`, and the
Costguard finding before switching a model to table or incremental.

## Targeted Test Commands

| Target | Use |
| --- | --- |
| `uv run make unit-core` | Config, resource, and storage unit tests. |
| `uv run make unit-ingest` | Polymarket ingestion and odds sync tests. |
| `uv run make unit-orchestration` | Dagster asset, job, and schedule tests. |
| `uv run make integration-dbt` | DuckDB and dbt smoke tests. |
| `uv run make integration-dagster` | Dagster integration smoke tests. |
| `uv run make dbt-build-ci` | Bootstrap disposable DuckDB and run dbt build. |
| `uv run make costguard` | Run the pinned dbt cost guardrail locally. |
| `uv run make coverage` | Local 100% product-core branch coverage gate. |

## Pull Request Expectations

- Keep PRs focused and update docs for behavior or operator workflow changes.
- Breaking changes are OK in v0.1.x; document them in CHANGELOG and data
  contracts — do not add legacy fallbacks unless the PR explicitly scopes compat
  work.
- Add or update tests for changed behavior.
- Do not commit `.env`, local DuckDB files, generated dbt targets, `site/`, or
  data exports.
- Follow [CONTRIBUTING](https://github.com/hypertrial/oddsfox/blob/main/CONTRIBUTING.md)
  for the full contribution workflow.
