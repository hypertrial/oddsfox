# Query interfaces

oddsfox exposes the same local lake through shell SQL, interactive DuckDB, a localhost HTTP API, and direct Parquet scans. Most analysts should start with `oddsfox sql`.

Implementation: [`src/duckdb_engine.rs`](../src/duckdb_engine.rs), [`src/server/mod.rs`](../src/server/mod.rs).

## SQL-First Querying

Print a result table from the shell:

```bash
oddsfox sql "SELECT COUNT(*) AS markets FROM bronze_markets"
oddsfox sql "SELECT market_id, question, volume_24h FROM bronze_markets ORDER BY volume_24h DESC NULLS LAST" --limit 10
```

`oddsfox sql` creates DuckDB views when needed and prints tab-separated output with a header row. Null cells print empty. The default print cap is 100 rows; `--limit 0` removes it.

Useful first queries:

```sql
SELECT market_id, question, volume_24h
FROM bronze_markets
ORDER BY volume_24h DESC NULLS LAST
LIMIT 20;

SELECT m.question, o.outcome_name, p.ts, p.price
FROM bronze_prices p
JOIN bronze_outcomes o ON p.token_id = o.token_id
JOIN bronze_markets m ON o.market_id = m.market_id
ORDER BY p.ts DESC
LIMIT 20;

SELECT source, user_id, market_id, total_pnl
FROM gold_user_pnl
ORDER BY total_pnl DESC
LIMIT 20;
```

More recipes: [examples/starter_queries.sql](../examples/starter_queries.sql).

## Interactive DuckDB

Open a DuckDB shell with oddsfox views registered:

```bash
oddsfox duckdb --out ~/.oddsfox
```

Use `--db` when you want a persistent catalog file at a custom path:

```bash
oddsfox duckdb --out ~/.oddsfox --db ~/.oddsfox/catalog.duckdb
```

### Bronze Views

Created from `bronze/{table}/**/*.parquet`. Run-partitioned tables filter to completed runs only.

| View | Bronze table |
|------|--------------|
| `bronze_events` | events |
| `bronze_markets` | markets |
| `bronze_outcomes` | outcomes |
| `bronze_prices` | prices |
| `bronze_orderbooks` | orderbooks |
| `bronze_book_levels` | book_levels |
| `bronze_trades` | trades |
| `bronze_resolutions` | resolutions |
| `bronze_user_fills` | user_fills |
| `bronze_user_positions` | user_positions |

### Gold Views

Created from `gold/{name}/**/*.parquet` when present.

| View | Gold table |
|------|------------|
| `gold_metric_points` | metric_points |
| `gold_calibration` | calibration |
| `gold_liquidity_rollup` | liquidity_rollup |
| `gold_accuracy` | accuracy |
| `gold_user_pnl` | user_pnl |

## Local HTTP API

Run a read-only localhost server:

```bash
oddsfox serve --port 8787 --out ~/.oddsfox
curl http://127.0.0.1:8787/health
```

`serve` reads Parquet directly and does not require `catalog.duckdb`.

| Method | Route | Purpose |
|--------|-------|---------|
| GET | `/health` | Liveness |
| GET | `/markets` | List markets (`?active=`, `?tag=`, `?order=volume\|spread\|liquidity`) |
| GET | `/markets/{market_id}` | Market detail |
| GET | `/events` | List events |
| GET | `/events/{event_id}` | Event detail |
| GET | `/tokens/{token_id}/prices` | Price series for a token |
| GET | `/markets/{market_id}/orderbook/latest` | Latest order book snapshot |
| GET | `/markets/{market_id}/metrics` | Per-market gold metric points |
| GET | `/metrics/calibration` | Calibration buckets |
| GET | `/metrics/liquidity` | Aggregate liquidity metrics |
| GET | `/pnl` | User PnL summary |
| GET | `/users/{user_id}/pnl` | PnL for one user |
| GET | `/usage` | Local command usage, run status, lake issues, and suggested next commands |
| GET | `/resolved` | Resolved markets (`?since=` date filter) |
| GET | `/search?q=` | Full-text search over local markets/events |
| GET | `/` | Static analyst UI with Usage, health, suggestions, and top markets |

### Usage View

The Usage view is local-only. It reads the run manifest and `check` output from the selected lake root; it does not send telemetry or write read events. It is meant to answer whether the toolkit has run recently, whether any runs failed or remain incomplete, how many rows completed runs wrote, and which maintenance command is useful next.

## Direct Parquet

Bronze and gold files are standard Parquet:

```python
import duckdb
duckdb.sql("SELECT * FROM read_parquet('~/.oddsfox/bronze/markets/**/*.parquet') LIMIT 5")
```

For run-partitioned bronze tables, prefer oddsfox-managed DuckDB views because they filter to completed runs. If scanning Parquet directly, replicate the completed-run filter from [`src/duckdb_engine.rs`](../src/duckdb_engine.rs).

## Related Docs

- [schema.md](schema.md) - table and join reference
- [cli.md](cli.md) - collection workflows
- [operations.md](operations.md) - config and lake operations
