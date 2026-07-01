# tests

This subtree validates the OddsFox prediction-market pipeline. Version `0.1.0`
starts with WC2026 Polymarket ingestion, marts, and orchestration.

See [OddsFox docs](../docs/index.md) for setup and runbook commands.

- `unit/`: mocked config, ingestion, storage, and orchestration tests.
- `integration/`: DuckDB/dbt/Dagster smoke tests using temp databases.
- `dbt/`: dbt project structure checks.
- top-level tests: repository policy checks such as secret scanning.

Useful commands:

```bash
make unit-core
make unit-ingest
make unit-orchestration
make integration-dbt
make integration-dagster
make test
```
