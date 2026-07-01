# Contributing to OddsFox

Thank you for your interest in contributing. OddsFox is an open-source,
local-first prediction-market data pipeline built with Dagster, dlt, dbt, and
DuckDB. Version `0.1.0` starts with FIFA World Cup 2026 Polymarket markets and
odds.

## Development setup

```bash
uv sync --extra dev
cp .env.example .env
```

The default warehouse is `oddsfox.duckdb` in the repo root. Keep schedules disabled in local dev and CI unless you intentionally run live ingestion:

```dotenv
POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED=false
POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED=false
```

See [docs/quickstart.md](docs/quickstart.md) and [docs/configuration.md](docs/configuration.md) for full operator setup.

## AI-assisted development

If you use Cursor, [Ponytail](https://github.com/DietrichGebert/ponytail) loads from [`.cursor/rules/ponytail.mdc`](.cursor/rules/ponytail.mdc). Repo-specific guardrails (layout, quality gate, orchestration limits) live in [AGENTS.md](AGENTS.md).

## Quality gate

Run these before opening a pull request (they mirror [`.github/workflows/ci.yml`](.github/workflows/ci.yml)):

```bash
uv run make lint
uv run make test
uv run make docs-check
uv run make dbt-parse
```

For full CI parity with dbt build, initialize the DuckDB bootstrap tables first, then build:

```bash
uv run python - <<'PY'
import oddsfox.storage.duckdb.connection as connection
connection._SCHEMA_INITIALIZED = False
connection._SCHEMA_LOGGED = False
connection.init_duck_db()
PY
uv run make dbt-build
```

Additional targets are available in the [Makefile](Makefile) (`unit-core`, `unit-ingest`, `integration-dbt`, etc.).

## Pull requests

1. Branch from `main`.
2. Keep changes focused; match existing style (Ruff, sqlfluff for dbt SQL).
3. Add or update tests for behavior changes.
4. Do not commit secrets, `.env`, DuckDB files, parquet/CSV exports, or other local artifacts (see [`.gitignore`](.gitignore)).
5. Ensure the quality gate passes locally.

## Reporting issues

Use GitHub Issues for bugs and feature requests. For security vulnerabilities, see [SECURITY.md](SECURITY.md).

## Code of conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By participating, you agree to uphold it.
