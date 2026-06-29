# CLI reference

See `oddsfox --help` for full flags.

## Active minute refresh (last 24 hours)

Sync only active markets/events at 1-minute fidelity for the rolling last 24 hours:

```bash
oddsfox sync markets --active
oddsfox sync prices --active --source polymarket
oddsfox sync markets --source kalshi --status open
oddsfox sync prices --active --source kalshi
```

`--active` defaults to `--fidelity 1 --recent-hours 24`. Existing price files are merged inside the 24-hour window instead of skipped.

For both sources in one backfill:

```bash
oddsfox backfill --source all --active
```

## Analyst workflow (recommended)

One command builds a DuckDB catalog of all markets with full CLOB price history:

```bash
oddsfox backfill --fidelity 60
oddsfox backfill --since 2023-01-01 --fidelity 1 --rps 5 --concurrency 4
oddsfox backfill --source kalshi --fidelity 60 --limit 25
```

Defaults (overridable via flags or `[backfill]` in `oddsfox.toml`):

- `--all` markets (active + closed + resolved)
- `--interval max` for full history (ignored when `--since`/`--until` are set)
- `--fidelity 60` (minutes between price points)
- `--rps 5` and `--concurrency 4`

Resume a partial backfill by re-running the same command (skips tokens already written). Use `--overwrite` to refetch.

Query the catalog:

```bash
oddsfox sql "SELECT m.question, p.ts, p.price FROM bronze_prices p JOIN bronze_outcomes o ON p.token_id = o.token_id JOIN bronze_markets m ON o.market_id = m.market_id LIMIT 10"
oddsfox serve --port 8787
```

## User PnL

Polymarket PnL starts from a public wallet/proxy address. Kalshi PnL uses the configured read-only API key.

```bash
oddsfox sync user --source polymarket --user 0xabc... --since 2026-01-01 --limit 100
oddsfox sync user --source kalshi --since 2026-01-01 --limit 100
oddsfox pnl --source all --format json
oddsfox sql "SELECT source, user_id, market_id, total_pnl FROM gold_user_pnl ORDER BY total_pnl DESC"
```

`sync user --source all` syncs both sources; pass `--user` for the Polymarket identity. Kalshi uses `[kalshi] key_id` as the local user id.

## Core workflow

```bash
oddsfox init --out ~/.oddsfox
oddsfox sync markets --all
oddsfox sync prices --all --interval max --fidelity 60 --rps 5 --concurrency 4
oddsfox snapshot books --active --top-volume 100
oddsfox compute all --since 2024-01-01
oddsfox duckdb --out ~/.oddsfox --db ~/.oddsfox/catalog.duckdb
oddsfox serve --port 8787
```

## Kalshi workflow

Configure read-only API credentials in `oddsfox.toml` (or the lake copy at `{out}/oddsfox.toml`):

```toml
[kalshi]
key_id = "your-key-id"
private_key_path = "/path/to/kalshi-private-key.pem"
```

Some public endpoints work without keys; authenticated requests use RSA-PSS signing for market-data reads only.
User PnL sync also uses the same credentials for read-only portfolio fills and positions.

```bash
oddsfox sync markets --source kalshi --status open --limit 100
oddsfox sync prices --source kalshi --market KXEXAMPLE-26 --series KXEXAMPLE --period 60
oddsfox sync trades --source kalshi --market KXEXAMPLE-26 --since 2026-01-01
oddsfox snapshot books --source kalshi --market KXEXAMPLE-26 --depth 20
oddsfox sql "SELECT market_id, question FROM bronze_markets WHERE source LIKE 'kalshi%' LIMIT 10"
```

## Explore

```bash
oddsfox search "election"
oddsfox market <market_id>
oddsfox event <event_id>
oddsfox resolved --since 2024-01-01
oddsfox top --by volume_24h
```

## Maintenance

```bash
oddsfox stats
oddsfox head
oddsfox head --limit 30 --export-dir ./heads
```

`head` prints the first 30 rows of every registered bronze and gold table to stdout and writes one CSV per table. Empty tables get a header-only CSV. Default export directory: `{lake}/_exports/heads/`.
