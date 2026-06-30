# OddsFox Operator Manual

OddsFox v0.1.0 is a local data pipeline for FIFA World Cup 2026 Polymarket markets and odds.

It uses Dagster for orchestration, dlt for the raw Gamma market landing, DuckDB for the local warehouse, Python for odds sync ledgers and retry logic, and dbt for analytics models.

## What v0.1.0 Includes

- WC2026 Polymarket Gamma event and market discovery.
- WC2026 market registry and metadata backfill.
- CLOB odds history sync, minutely whale odds, and repair paths.
- DuckDB raw, ops, staging, intermediate, mart, and observability schemas.
- Disabled-by-default schedules for live/minutely odds refreshes.

## What v0.1.0 Excludes

Optional soccer context sources, simulations, allocation tooling, web app integration, and generated historical docs are not part of this repo.

## Safe First Run

Start with [Quickstart](quickstart.md). Do not enable schedules until the warehouse initializes, dbt builds, and manual Dagster jobs behave as expected.

Primary operator references:

- [Operations](operations.md)
- [Warehouse](warehouse.md)
- [Configuration](configuration.md)
- [Troubleshooting](troubleshooting.md)
