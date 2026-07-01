# Configuration

Use `.env.example` as the source of local overrides.
For first-run steps, see [Quickstart](quickstart.md).

Most settings are adapter-specific. In v0.1.0, that means Polymarket and WC2026
scope controls.

## Warehouse and dbt

- `DUCKDB_NAME`: warehouse filename or path. Default: `oddsfox.duckdb`.
- `DUCKDB_PATH`: optional absolute path override.
- `DBT_PROFILES_DIR`: optional dbt profiles directory override.

Most operators should leave `DBT_PROFILES_DIR` unset and use the packaged `dbt/profiles`.

## API Pacing

- `MARKETS_REQUESTS_PER_SECOND`: Gamma market/event request pace.
- `ODDS_REQUESTS_PER_SECOND`: CLOB odds request pace.
- `HTTP_CONNECT_TIMEOUT_SECONDS`: HTTP connection timeout.
- `HTTP_READ_TIMEOUT_SECONDS`: HTTP read timeout.

Lower request rates when Polymarket APIs return transient failures or timeouts.

## Current WC2026 Scope

- `POLYMARKET_WC2026_DEFAULT_EVENT_SLUG`
- `POLYMARKET_WC2026_EVENT_SLUGS`
- `POLYMARKET_WC2026_EVENT_SLUG_PREFIXES`
- `POLYMARKET_WC2026_EVENT_TAGS`
- `POLYMARKET_WC2026_KEYSET_CLOSED`
- `POLYMARKET_WC2026_KEYSET_VOLUME_MIN`
- `POLYMARKET_WC2026_KEYSET_RELATED_TAGS`
- `POLYMARKET_WC2026_TAG_DISCOVERY`
- `POLYMARKET_WC2026_TAG_CLOSURE_ROUNDS`
- `POLYMARKET_WC2026_TAG_CRAWL_MAX`

The seed file `src/oddsfox/ingestion/polymarket/seeds/wc2026_events.yml` is the default scope source.

## Schedules

- `POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED`: enables the five-minute and hourly schedules for `polymarket_minutely_odds_ingest`.
- `POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED`: enables the one-minute live schedule for `polymarket_minutely_odds_ingest`.

Both default to `false`.

## CLOB Credentials

- `CLOB_API_KEY`
- `CLOB_API_SECRET`
- `CLOB_API_PASSPHRASE`

Leave unset for local docs, dbt, and mocked tests.
