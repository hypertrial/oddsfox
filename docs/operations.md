# Operations and configuration

## Purpose

oddsfox reads optional settings from `oddsfox.toml`. Most commands also accept CLI flags that override config values for that invocation.

Config types: [`src/settings.rs`](../src/settings.rs). Default lake root: `~/.oddsfox`.

## Config file location

Resolution order:

1. `--config /path/to/oddsfox.toml` (global flag on any command)
2. `{lake_root}/oddsfox.toml` when `--out` points at a lake with a config file
3. Built-in defaults

`oddsfox init` writes a starter config into the lake root.

## Example `oddsfox.toml`

```toml
[data]
home = "/Users/you/.oddsfox"
store = "duckdb"
raw_retention_days = 30

[polymarket]
gamma_base_url = "https://gamma-api.polymarket.com"
clob_base_url = "https://clob.polymarket.com"
data_base_url = "https://data-api.polymarket.com"
ws_market_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

[kalshi]
rest_base_url = "https://external-api.kalshi.com/trade-api/v2"
ws_url = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
key_id = "your-key-id"
private_key_path = "/path/to/kalshi-private-key.pem"

[sync]
requests_per_second = 2.0
max_retries = 5
user_agent = "oddsfox/0.2.0"

[backfill]
fidelity_minutes = 60
interval = "max"
requests_per_second = 5.0
concurrency = 4

[duckdb]
database = "catalog.duckdb"
```

## Sections

### `[data]`

| Key | Default | Description |
|-----|---------|-------------|
| `home` | `~/.oddsfox` | Default lake root when `--out` is omitted |
| `store` | `duckdb` | Storage backend label (DuckDB catalog) |
| `raw_retention_days` | 30 | Days to retain `_raw/` JSON captures |

### `[polymarket]`

Override Polymarket API endpoints. Useful for testing or proxy setups. Defaults match public Polymarket Gamma, CLOB, Data API, and WebSocket URLs.

### `[kalshi]`

| Key | Description |
|-----|-------------|
| `rest_base_url` | Kalshi REST API base |
| `ws_url` | Kalshi WebSocket URL |
| `key_id` | Read-only API key id for authenticated requests |
| `private_key_path` | PEM private key for RSA-PSS signing |

Some public Kalshi endpoints work without keys. User PnL and portfolio reads require credentials. Keys stay on the local filesystem — see [compliance.md](compliance.md) and [SECURITY.md](../SECURITY.md).

### `[sync]`

Rate limiting and retry behavior for network sync commands:

| Key | Default | Description |
|-----|---------|-------------|
| `requests_per_second` | 2.0 | Global request throttle |
| `max_retries` | 5 | Retries on transient failures |
| `user_agent` | `oddsfox/0.2.0` | HTTP User-Agent header |

### `[backfill]`

Defaults for `oddsfox backfill` when flags are omitted (see [cli.md](cli.md)):

| Key | Default | Description |
|-----|---------|-------------|
| `fidelity_minutes` | 60 | Minutes between price points |
| `interval` | `max` | Price history window |
| `requests_per_second` | 5.0 | Backfill throttle |
| `concurrency` | 4 | Parallel price fetches |

### `[duckdb]`

| Key | Default | Description |
|-----|---------|-------------|
| `database` | `catalog.duckdb` | Catalog filename relative to lake root |

Use `oddsfox duckdb --db /custom/path.duckdb` to override per invocation.

## Global CLI flag

```bash
oddsfox --config /path/to/oddsfox.toml sync markets --active
```

## Maintenance commands

| Command | Purpose |
|---------|---------|
| `oddsfox check` | Incomplete runs, orphan partitions, temp files |
| `oddsfox repair` | Quarantine orphan run partitions, remove temp files |
| `oddsfox stats` | Row counts per bronze table |
| `oddsfox clean` | Inspect quarantine directory (`--dry-run` to preview) |

See [metadata.md](metadata.md) for manifest details.

## Hourly Collector Operations

Run forever:

```bash
oddsfox collect hourly --source all --since 2024-01-01
```

Run one catch-up pass for cron or CI:

```bash
oddsfox collect hourly --source all --once
```

The first run for a source requires `--since`. After that, the seed date is stored and `--since` is optional. `--lag-minutes` defaults to `5`, so the collector waits for an hour to be safely closed before fetching it.

During collection, oddsfox prints a start line per source (token count, since, horizon) and progress every 25 tokens until the pass completes.

`oddsfox collect hourly` stores resume state in `{lake}/_metadata/sync_state.parquet` using cursor keys shaped like `collect:hourly:{source}:{token_id}`. Each cursor records the next UTC hour to collect, the market id, whether the token is done, and the last window row count.

To inspect cursors:

```bash
oddsfox sql "SELECT source, cursor_key, cursor_value FROM read_json_auto('~/.oddsfox/_metadata/sync_state.parquet') WHERE cursor_key LIKE 'collect:hourly:%'" --limit 20
```

To check whether collection is writing data:

```bash
oddsfox stats --out ~/.oddsfox
oddsfox sql "SELECT token_id, MAX(ts) AS latest_ts FROM bronze_prices GROUP BY token_id ORDER BY latest_ts DESC" --limit 20
```

To reset one token, remove only that token's sync-state row. The next collector run reinitializes that token from the stored source seed date or from `--since`. Do not delete broad cursor ranges unless you intend to refetch that history.

## Related docs

- [cli.md](cli.md) — workflows and Kalshi setup
- [storage.md](storage.md) — lake directory layout
