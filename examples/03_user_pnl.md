# User PnL sync

Sync read-only fills and positions for a user-supplied Polymarket wallet and/or configured Kalshi credentials, then summarize PnL.

Polymarket requires a public wallet or proxy address. Kalshi uses `[kalshi]` credentials from config.

```bash
oddsfox init --out ./lake

# Polymarket: pass your wallet/proxy address
oddsfox sync user --source polymarket --user 0xabc... --limit 100 --out ./lake

# Kalshi: uses key_id from oddsfox.toml as local user id
oddsfox sync user --source kalshi --limit 100 --out ./lake

# Combined summary
oddsfox pnl --source all --format json --out ./lake

oddsfox duckdb --out ./lake --db ./lake/catalog.duckdb
```

Query rolled-up PnL:

```sql
SELECT source, user_id, market_id, total_pnl, realized_pnl, unrealized_pnl
FROM gold_user_pnl
ORDER BY total_pnl DESC;

SELECT fill_id, market_id, side, price, size, ts
FROM bronze_user_fills
WHERE user_id = '0xabc...'
ORDER BY ts DESC
LIMIT 20;
```

Reruns are safe: fills dedupe by id, positions keep the latest snapshot, and watermarks avoid refetching old activity unless `--since` is passed.

See [docs/cli.md](../docs/cli.md#user-pnl) for watermark and limit behavior.
