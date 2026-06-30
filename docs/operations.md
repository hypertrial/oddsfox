# Operations

## Dagster Assets

The main asset order is:

1. `dlt_polymarket_markets`
2. `polymarket_markets_snapshot`
3. `polymarket_wc2026_registry`
4. `polymarket_market_metadata_backfill`
5. `polymarket_token_odds_history`
6. `polymarket_token_odds_history_minutely`
7. `polymarket_dbt`

`polymarket_odds_repair` is an operator repair asset, not part of the routine full pipeline.

## Jobs

- `polymarket_ingest_full_refresh_events`: full WC2026 event/market discovery, registry refresh, metadata backfill, and odds sync.
- `polymarket_ingest_incremental`: metadata backfill and routine token odds sync.
- `polymarket_minutely_odds_ingest`: minutely odds refresh for whale markets.
- `dbt_full_refresh`: dbt analytics build.
- `wc2026_polymarket_full_pipeline`: full ingest plus dbt build.

## Schedules

Schedules are stopped by default.

All minutely schedules target `polymarket_minutely_odds_ingest`:

- `polymarket_minutely_odds_schedule`: every 5 minutes.
- `polymarket_minutely_odds_cold_schedule`: hourly conservative refresh with cold run config.
- `polymarket_minutely_odds_live_schedule`: every minute when explicitly enabled.

Enable only after manual jobs are healthy:

```dotenv
POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED=true
POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED=false
```

Enable `POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED` only during intentional live operation.

## Recovery

- Re-run `polymarket_ingest_incremental` for routine gaps.
- Run `polymarket_odds_repair` if the token sync ledger is inconsistent.
- Run `dbt_full_refresh` after raw or ops table repairs.
- Use `scripts/profile_warehouse.py` to inspect relation counts and freshness without opening the database read-write.
