# Kalshi market sync

Sync one Kalshi market end-to-end: metadata, prices, trades, and an order book snapshot.

Prerequisites:

- A Kalshi market ticker and series ticker.
- Read-only Kalshi API credentials in `oddsfox.toml` if the endpoint requires auth. See [docs/operations.md](../docs/operations.md).

```bash
oddsfox init --out ./lake

# Replace with a real open market ticker
export MARKET=KXEXAMPLE-26
export SERIES=KXEXAMPLE

oddsfox sync markets --source kalshi --status open --limit 100 --out ./lake
oddsfox sync prices --source kalshi --market $MARKET --series $SERIES --period 60 --out ./lake
oddsfox sync trades --source kalshi --market $MARKET --since 2026-01-01 --out ./lake
oddsfox snapshot books --source kalshi --market $MARKET --depth 20 --out ./lake

oddsfox duckdb --out ./lake --db ./lake/catalog.duckdb
oddsfox sql "SELECT market_id, question, volume_24h FROM bronze_markets WHERE market_id LIKE 'kalshi:%' ORDER BY volume_24h DESC NULLS LAST" --limit 10 --out ./lake
```

Verify the market has prices and trades:

```bash
oddsfox sql "SELECT token_id, COUNT(*) AS rows, MAX(ts) AS latest_ts FROM bronze_prices WHERE token_id LIKE 'kalshi:KXEXAMPLE-26:%' GROUP BY token_id" --out ./lake
oddsfox sql "SELECT market_id, ts, side, price, size FROM bronze_trades ORDER BY ts DESC" --limit 10 --out ./lake
```

Query in DuckDB:

```sql
SELECT market_id, question, volume_24h
FROM bronze_markets
WHERE market_id LIKE 'kalshi:%'
ORDER BY volume_24h DESC NULLS LAST
LIMIT 10;

SELECT ts, price FROM bronze_prices
WHERE token_id LIKE 'kalshi:KXEXAMPLE-26:%'
ORDER BY ts DESC LIMIT 20;
```
