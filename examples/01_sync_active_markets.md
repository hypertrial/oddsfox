# Active refresh walkthrough

Use this for a small local lake under `./lake` and quick SQL checks.

## Active 24-hour refresh

```bash
oddsfox init --out ./lake
oddsfox backfill --source all --active --out ./lake
```

Verify markets and recent prices:

```bash
oddsfox sql "SELECT market_id, question, volume_24h FROM bronze_markets ORDER BY volume_24h DESC NULLS LAST" --limit 10 --out ./lake
oddsfox sql "SELECT token_id, MAX(ts) AS latest_ts FROM bronze_prices GROUP BY token_id ORDER BY latest_ts DESC" --limit 10 --out ./lake
```

## One hourly collector pass

Use `--once` when you want a bounded catch-up run instead of a forever process:

```bash
oddsfox collect hourly --source all --since 2024-01-01 --once --out ./lake
```

After the first run, the cursor seed is stored, so later one-pass runs do not need `--since`:

```bash
oddsfox collect hourly --source all --once --out ./lake
```

Query in DuckDB:

```sql
SELECT market_id, question, volume_24h
FROM bronze_markets
ORDER BY volume_24h DESC NULLS LAST
LIMIT 10;

SELECT token_id, MAX(ts) AS latest_ts
FROM bronze_prices
GROUP BY token_id
ORDER BY latest_ts DESC
LIMIT 10;
```
