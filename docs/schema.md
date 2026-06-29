# Schema reference

## Purpose

This page describes what each lake table means, how tables join, and how Polymarket vs Kalshi rows are distinguished. For exhaustive column lists, use the CLI or contract JSON — do not treat this page as a full column catalog.

Source of truth: [`src/schema/`](../src/schema/), [`tests/contract.golden.json`](../tests/contract.golden.json).

## Inspecting schemas

```bash
oddsfox schema markets
oddsfox schema user_fills
oddsfox contract --out ~/.oddsfox
```

## Source and ID conventions

Every bronze row includes ingest metadata: `source`, `raw_url`, `raw_sha256`, `ingested_at`, `run_id`.

| Origin | `source` column | ID shape |
|--------|---------------|----------|
| Polymarket (Gamma/CLOB) | `gamma` | Native Polymarket ids (no prefix) |
| Kalshi | `kalshi` (in ingest meta) | Prefixed ids: `kalshi:{ticker}`, tokens `kalshi:{ticker}:yes` / `:no` |

Filter by source in SQL:

```sql
SELECT market_id, question FROM bronze_markets WHERE source = 'gamma' LIMIT 5;
SELECT market_id, question FROM bronze_markets WHERE market_id LIKE 'kalshi:%' LIMIT 5;
```

## Join graph

```text
bronze_events.event_id
    └── bronze_markets.event_id
            └── bronze_outcomes.market_id
                    └── bronze_outcomes.token_id
                            └── bronze_prices.token_id
                            └── bronze_orderbooks.token_id
                            └── bronze_trades.market_id (Kalshi)

bronze_resolutions.market_id  → winning outcome
bronze_user_fills.market_id   → user PnL inputs
bronze_user_positions.market_id
```

Typical price query:

```sql
SELECT m.question, o.outcome_name, p.ts, p.price
FROM bronze_prices p
JOIN bronze_outcomes o ON p.token_id = o.token_id
JOIN bronze_markets m ON o.market_id = m.market_id
LIMIT 10;
```

## Bronze tables

### `events`

Event groupings (title, slug, tags, active/closed flags, timestamps). Join key: `event_id`.

Domain columns plus ingest metadata and optional `raw_json`.

### `markets`

Individual prediction markets. Join keys: `market_id`, `event_id`.

Notable fields: `question`, `active`, `closed`, `resolved`, `liquidity`, `volume`, `volume_24h`, `close_time`, `resolution_time`.

### `outcomes`

Outcome legs within a market. Join keys: `market_id`, `token_id`, `outcome_index`.

`token_id` links to prices and order books. `is_winner` set after resolution.

### `prices`

Token-level probability time series. Partitioned by `token_id` (not run). Join keys: `token_id`, `market_id`.

Fields: `ts`, `price`, `fidelity_minutes`, plus ingest metadata.

### `orderbooks`

Point-in-time book summary per token. Join keys: `snapshot_id`, `token_id`, `market_id`.

Fields: `best_bid`, `best_ask`, `spread`, `midpoint`, depth at 1% and 5%.

### `book_levels`

Individual price levels for each `snapshot_id`. Join to `orderbooks` on `snapshot_id`.

Fields: `side`, `price`, `size`, `level_index`.

### `trades`

Kalshi trade prints. Join key: `market_id`.

Fields: trade id, side, price, size, timestamps (see `oddsfox schema trades`).

### `resolutions`

Resolution outcome per market. Join key: `market_id`.

Fields: `resolved_at`, `winning_token_id`, `winning_outcome`, `resolution_status`.

### `user_fills`

Read-only user trade history. Join keys: `user_id`, `market_id`, `token_id`, `fill_id`.

Fields: `side`, `price`, `size`, `fee`, `realized_pnl`, `ts`.

### `user_positions`

Read-only position snapshots. Join keys: `user_id`, `market_id`, `token_id`.

Fields: size, average price, mark value, unrealized/realized PnL.

## Gold tables

Gold tables use run partitioning. DuckDB exposes them as `gold_*` views when Parquet exists.

### `metric_points` (`gold_metric_points`)

Liquidity metrics from order book snapshots. Join keys: `market_id`, `token_id`.

Metric names include `spread`, `relative_spread`, `bid_depth_1pct`, `ask_depth_1pct`. Columns: `metric_name`, `ts`, `value`, `window_seconds`.

### `calibration` (`gold_calibration`)

Calibration buckets: `bucket_start`, `bucket_end`, `mean_prediction`, `observed_rate`, `sample_count`.

### `accuracy` (`gold_accuracy`)

Per-market forecast scores on resolved outcomes: `brier_score`, `log_loss`, `price`, `outcome`.

### `user_pnl` (`gold_user_pnl`)

Rolled-up PnL per source, user, and market: `realized_pnl`, `unrealized_pnl`, `fees`, `mark_value`, `total_pnl`.

```sql
SELECT source, user_id, market_id, total_pnl
FROM gold_user_pnl
ORDER BY total_pnl DESC;
```

## Contract versioning

- **Schema version:** `prediction-market-v3` ([`src/schema/mod.rs`](../src/schema/mod.rs))
- **Layout version:** `medallion-v2` ([`src/paths.rs`](../src/paths.rs))
- **Contract version:** `1.0.0` ([`src/contract/mod.rs`](../src/contract/mod.rs))

Breaking column changes require bumping the contract version and updating the golden file (`UPDATE_GOLDEN=1 cargo test contract_matches_golden_file` — contributor workflow in [AGENTS.md](../AGENTS.md)).

## Related docs

- [storage.md](storage.md) — partition layout
- [interfaces.md](interfaces.md) — DuckDB view names
- [metadata.md](metadata.md) — contract and schema registry
