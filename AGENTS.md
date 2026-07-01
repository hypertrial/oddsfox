# AGENTS.md

OddsFox is an open-source, local-first prediction-market data pipeline.
Version `0.1.0` starts with FIFA World Cup 2026 Polymarket markets and odds.
Stack: **Dagster** (orchestration), **dlt** (market landing), **dbt** +
**DuckDB** (warehouse/analytics), **uv** (deps), **Ruff** + **sqlfluff**
(lint), **pytest** (tests).

## Setup

```bash
uv sync --extra dev
cp .env.example .env
```

Default warehouse: `oddsfox.duckdb` in the repo root. Keep schedules disabled in local dev and CI unless intentionally running live ingestion:

```dotenv
POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED=false
POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED=false
```

## AI agent guidance

Cursor loads [Ponytail](https://github.com/DietrichGebert/ponytail) from [`.cursor/rules/ponytail.mdc`](.cursor/rules/ponytail.mdc) (`alwaysApply: true`). It complements this file: reuse existing code, prefer stdlib and installed deps, and keep diffs minimal. Do not cut validation, error handling, security, or tests that cover real behavior. Mark intentional shortcuts with a `ponytail:` comment (name the ceiling and upgrade path).

Other agents should read this `AGENTS.md` at the repo root.

## Quality gate (run before finishing work)

Mirrors [`.github/workflows/ci.yml`](.github/workflows/ci.yml). Use `uv run make …` from the repo root.

```bash
uv run make lint
uv run make test
uv run make docs-check
uv run make dbt-parse
```

For full dbt build parity with CI, bootstrap DuckDB schemas first:

```bash
uv run python - <<'PY'
import oddsfox.storage.duckdb.connection as connection
connection._SCHEMA_INITIALIZED = False
connection._SCHEMA_LOGGED = False
connection.init_duck_db()
PY
uv run make dbt-build
```

### Targeted commands

| Target | Purpose |
|--------|---------|
| `make format` | Ruff format + dbt parse + sqlfluff fix |
| `make unit-core` | Config, resources, storage unit tests |
| `make unit-ingest` | Ingestion unit tests |
| `make unit-orchestration` | Orchestration/Dagster unit tests |
| `make integration-dbt` | DuckDB + dbt integration smoke |
| `make integration-dagster` | Dagster integration smoke |
| `make dagster-dev` | Local Dagster UI |
| `make docs-serve` | MkDocs dev server |

## Project layout

```
.cursor/rules/     # Cursor agent rules (ponytail)
src/oddsfox/
  config/          # Settings barrel (settings.py re-exports warehouse + polymarket)
  ingestion/       # dlt sources, Polymarket fetch/sync/backfill, odds engine
  orchestration/   # Dagster assets, jobs, schedules, dbt wiring
  resources/       # HTTP, outbound URL, progress guardrails
  storage/duckdb/  # Connection, schemas, markets/odds persistence, profiling
dbt/
  models/polymarket/{staging,intermediate,marts,observability}/
  tests/           # Singular dbt data tests (assert_*)
tests/
  unit/            # Mocked config, ingestion, storage, orchestration
  integration/     # DuckDB/dbt/Dagster smoke (temp databases)
  dbt/             # dbt project structure checks
docs/              # Project docs (MkDocs)
scripts/           # Warehouse audits, repairs, profiling (not CI gate)
```

Imports use src-layout paths: `from oddsfox.config.settings import …`, not relative imports across package boundaries.

## Code style

**Python** ([pyproject.toml](pyproject.toml)):

- Ruff: `target-version = "py310"`, line length 88, double quotes, isort (`extend-select = ["I"]`).
- Run `make format` before committing Python changes; `make lint` checks format + ruff check.
- Match existing module boundaries; avoid drive-by refactors outside the task scope.

**dbt SQL**:

- sqlfluff: DuckDB dialect, dbt templater, max line length 130.
- Lint/fix: `dbt/models`, `dbt/tests` only (see Makefile).
- Layer naming: `polymarket_staging`, `polymarket_intermediate`, `polymarket_marts`, `polymarket_observability`.

**Tests** ([tests/README.md](tests/README.md)):

- Default `make test` excludes `integration`, `performance`, `slow`, `repo_check`.
- Mark slow/external tests with the appropriate pytest marker; do not widen default test scope without reason.
- Add or update tests for behavior changes in the matching `tests/unit/` or `tests/integration/` subtree.

## Orchestration guardrails

Asset order (routine pipeline):

1. `dlt_polymarket_markets`
2. `polymarket_markets_snapshot`
3. `polymarket_wc2026_registry`
4. `polymarket_market_metadata_backfill`
5. `polymarket_token_odds_history`
6. `polymarket_token_odds_history_minutely`
7. `polymarket_dbt`

`polymarket_odds_repair` is a repair asset, not part of the routine full pipeline.

Key jobs: `polymarket_ingest_incremental`, `polymarket_ingest_full_refresh_events`, `polymarket_minutely_odds_ingest`, `dbt_full_refresh`, `wc2026_polymarket_full_pipeline`.

Schedules target `polymarket_minutely_odds_ingest` and are **stopped by default**. Do not enable live/minutely schedules in code or `.env` unless the task explicitly requires it.

DuckDB is local-only runtime state. For read-only inspection prefer `scripts/profile_warehouse.py` over opening the warehouse read-write.

## Do not

- Commit `.env`, secrets, `*.duckdb` / WAL/SHM files, parquet/CSV exports, or other local artifacts (see [`.gitignore`](.gitignore)).
- Set `CLOB_API_KEY`, `CLOB_API_SECRET`, or `CLOB_API_PASSPHRASE` for docs, dbt, or mocked tests.
- Invent commands outside the Makefile; if a check is missing, add a Makefile target rather than documenting one-off scripts as the gate.
- Add runtime scope such as soccer context, simulations, allocation, or web integration without explicit product direction; v0.1.0 still ships the WC2026 Polymarket ingest and warehouse implementation.

## Pull requests

1. Branch from `main`.
2. Keep changes focused; one concern per PR when possible.
3. Ensure the quality gate passes locally.
4. Update docs in `docs/` when behavior, configuration, or project positioning changes.

## Further reading

- [OddsFox docs](docs/index.md) — overview, runbooks, warehouse, troubleshooting
- [CONTRIBUTING.md](CONTRIBUTING.md) — contributor workflow and CI parity
- [docs/operations.md](docs/operations.md) — assets, jobs, schedules, recovery
- [docs/configuration.md](docs/configuration.md) — `.env` reference
