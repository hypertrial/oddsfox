# Query interfaces

## Purpose

v0.2 exposes the lake through DuckDB SQL, a local read-only HTTP API, and direct Parquet scans. This page catalogs the supported query surfaces so analysts can choose the right entry point.

Implementation: [`src/duckdb_engine.rs`](../src/duckdb_engine.rs), [`src/server/mod.rs`](../src/server/mod.rs).

## DuckDB

### Commands

```bash
oddsfox duckdb --out ~/.oddsfox --db ~/.oddsfox/catalog.duckdb
oddsfox sql "SELECT COUNT(*) FROM bronze_markets" --out ~/.oddsfox
```

`duckdb` creates or refreshes views over existing Parquet. Views are omitted when no files exist for that table.

### Bronze views

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

### Gold views

Created from `gold/{name}/**/*.parquet` when present.

| View | Gold table |
|------|------------|
| `gold_metric_points` | metric_points |
| `gold_calibration` | calibration |
| `gold_liquidity_rollup` | liquidity_rollup |
| `gold_accuracy` | accuracy |
| `gold_user_pnl` | user_pnl |

Example:

```sql
SELECT metric_name, AVG(value) AS avg_value
FROM gold_metric_points
GROUP BY metric_name;
```

More queries: [examples/starter_queries.sql](../examples/starter_queries.sql).

### Serve vs DuckDB catalog

`oddsfox serve` reads Parquet directly and does **not** require `catalog.duckdb`. Use `duckdb` or `sql --db` when you want persistent views in a catalog file.

## Local HTTP API

Read-only axum server bound to **localhost** by default. No authentication.

```bash
oddsfox serve --port 8787 --out ~/.oddsfox
curl http://127.0.0.1:8787/health
```

### Routes

| Method | Route | Purpose |
|--------|-------|---------|
| GET | `/health` | Liveness (`{"status":"ok"}`) |
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
| GET | `/resolved` | Resolved markets (`?since=` date filter) |
| GET | `/search?q=` | Full-text search over local markets/events |
| GET | `/` | Static minimal web UI |

Responses are JSON arrays or objects. The bundled UI at `/` links to common endpoints.

## External engines

Bronze and gold paths are standard Parquet. Any columnar engine can scan them directly:

```python
import duckdb
duckdb.sql("SELECT * FROM read_parquet('~/.oddsfox/bronze/markets/**/*.parquet') LIMIT 5")
```

For run-partitioned bronze tables, prefer oddsfox-managed DuckDB views (which apply completed-run filtering) or replicate the `run_id IN (...)` filter from [`src/duckdb_engine.rs`](../src/duckdb_engine.rs).

## Related docs

- [schema.md](schema.md) — table and join reference
- [cli.md](cli.md) — sync, compute, and serve workflows
- [operations.md](operations.md) — config and lake root
