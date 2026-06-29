# Kalshi market sync

Sync one Kalshi market end-to-end: metadata, prices, trades, and an order book snapshot.

Configure read-only API credentials in `oddsfox.toml` if needed (see [docs/operations.md](../docs/operations.md)).

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
