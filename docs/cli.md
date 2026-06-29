# CLI workflows

See `oddsfox --help` and `oddsfox <command> --help` for every flag. This page is the analyst path through the CLI.

## Which command should I use?

| Goal | Command |
|------|---------|
| First demo with local UI | `oddsfox quickstart` |
| Continuous hourly all-market collection | `oddsfox collect hourly --source all --since YYYY-MM-DD` |
| One collector catch-up pass | `oddsfox collect hourly --source all --once` |
| Active-market hourly collection (faster) | `oddsfox collect hourly --source all --since YYYY-MM-DD --active` |
| Rolling active-market refresh | `oddsfox backfill --source all --active` |
| Custom source/range price fetch | `oddsfox sync prices ...` |
| User PnL | `oddsfox sync user ...`, then `oddsfox pnl` |
| Shell SQL | `oddsfox sql "SELECT ..."` |
| Interactive SQL | `oddsfox duckdb` |
| Local API and UI | `oddsfox serve` |

## Demo

```bash
oddsfox quickstart
```

Expected result: oddsfox initializes the lake, syncs a small active-market sample, creates DuckDB views, computes starter outputs, and prints a local URL.

Open <http://127.0.0.1:8787>. `quickstart` keeps serving until you stop it.

## Durable Hourly Collection

Collect hourly price history across every discovered Polymarket and Kalshi market:

```bash
oddsfox collect hourly --source all --since 2024-01-01
```

First run requires `--since` so historical collection starts from an explicit UTC date. Passing `--since` again overrides the stored seed and clears per-token cursors for that source. The collector refreshes market metadata, fetches price history in **7-day chunks** (one API call per chunk per token), splits points into UTC hourly parquet files locally, and advances a per-token cursor once per chunk.

Use `--active` to collect only tokens from markets where `active = true` (open markets). Default is all discovered tokens. For open-market monitoring, `--active` is much faster than the full historical corpus.

Interrupt and resume:

Ctrl+C, a crash, or killing the process is safe. Re-run the same `collect hourly` command; tokens already caught up are skipped. After the first run, `--since` is optional — omit it, or pass the **same** date. Only a **different** `--since` clears per-token cursors and restarts from scratch.

Restart behavior (per token, 7-day chunks):

- Crash mid-chunk before cursor save: the in-flight chunk is re-fetched; hourly files for those hours are replaced deterministically (no duplicate rows in `bronze_prices`).
- Crash after cursor save: the next run starts at the next chunk boundary (`next_start_ts`).
- Token already at horizon: skipped on restart.
- Closed or resolved tokens: cursor marked `done` after their final window.

See [operations.md](operations.md#hourly-collector-operations) and [metadata.md](metadata.md) for cursor inspection and reset.

Useful bounded run for cron, CI, or manual catch-up:

```bash
oddsfox collect hourly --source all --once
oddsfox collect hourly --source all --since 2026-06-01 --active --once
```

`--lag-minutes` defaults to `5`, so only hours that ended at least five minutes ago are collected.

Performance: each token needs roughly one API call per 7-day chunk (not one call per hour). A June–today window is ~4 calls per token instead of ~700. With `--active`, token count drops to open markets only.

## Active Market Refresh

For a rolling 24-hour active-market refresh at 1-minute fidelity:

```bash
oddsfox backfill --source all --active
```

Equivalent explicit commands:

```bash
oddsfox sync markets --active
oddsfox sync prices --active --source polymarket
oddsfox sync markets --source kalshi --status open
oddsfox sync prices --active --source kalshi
```

`--active` defaults to `--fidelity 1 --recent-hours 24`. Existing price files are merged inside the rolling window instead of skipped.

## Longer Historical Backfills

Use these when you want bounded historical data rather than a forever collector:

```bash
oddsfox backfill --fidelity 60
oddsfox backfill --since 2023-01-01 --fidelity 1 --rps 5 --concurrency 4
oddsfox backfill --source kalshi --fidelity 60 --limit 25
```

Defaults are `--all` markets, `--interval max`, `--fidelity 60`, `--rps 5`, and `--concurrency 4`.

Resume by re-running the same command. Snapshot-style bronze runs become visible only after the manifest run is complete. Price sync resumes per token using stored range/fidelity checkpoints, or skips existing token parquet when a backfill was interrupted before sync state flushed; use `--overwrite` to refetch.

## Query The Results

`oddsfox sql` prints tab-separated rows with a header and a default 100-row print cap:

```bash
oddsfox sql "SELECT market_id, question, volume_24h FROM bronze_markets ORDER BY volume_24h DESC NULLS LAST" --limit 10
```

Use `--limit 0` to remove the print cap.

Open interactive DuckDB:

```bash
oddsfox duckdb --out ~/.oddsfox
```

Serve the local API and UI:

```bash
oddsfox serve --port 8787
```

`serve` reads Parquet directly and does not require `catalog.duckdb`. Use `duckdb` or `sql --db` when you want persistent views in a catalog file.

## User PnL

Polymarket PnL starts from a public wallet/proxy address. Kalshi PnL uses the configured read-only API key.

```bash
oddsfox sync user --source polymarket --user 0xabc... --since 2026-01-01 --limit 100
oddsfox sync user --source kalshi --since 2026-01-01 --limit 100
oddsfox pnl --source all --format json
oddsfox sql "SELECT source, user_id, market_id, total_pnl FROM gold_user_pnl ORDER BY total_pnl DESC" --limit 20
```

Reruns are safe: fills dedupe by source/fill id, latest positions replace older snapshots, and watermarks avoid refetching old fills. Passing `--since` overrides the stored watermark. Use `--limit` only for smoke tests or bounded debugging.

## Kalshi Setup

Configure read-only API credentials in `oddsfox.toml` or `{lake}/oddsfox.toml`:

```toml
[kalshi]
key_id = "your-key-id"
private_key_path = "/path/to/kalshi-private-key.pem"
```

Some public endpoints work without keys. User PnL and portfolio reads require credentials.

Single-market Kalshi workflow:

```bash
oddsfox sync markets --source kalshi --status open --limit 100
oddsfox sync prices --source kalshi --market KXEXAMPLE-26 --series KXEXAMPLE --period 60
oddsfox sync trades --source kalshi --market KXEXAMPLE-26 --since 2026-01-01
oddsfox snapshot books --source kalshi --market KXEXAMPLE-26 --depth 20
oddsfox sql "SELECT market_id, question FROM bronze_markets WHERE market_id LIKE 'kalshi:%' LIMIT 10"
```

## Explore And Maintain

Explore local data:

```bash
oddsfox search "election"
oddsfox market <market_id>
oddsfox event <event_id>
oddsfox resolved --since 2024-01-01
oddsfox top --by volume_24h
```

Inspect and maintain the lake:

```bash
oddsfox stats
oddsfox head
oddsfox check
oddsfox repair
```

`check` reports incomplete runs, orphan run partitions, and leftover temp files. `repair` removes temp files and moves orphan run partitions under `_quarantine/orphan_runs/` without deleting their data.

## WebSocket Watch

Use `watch` for raw Polymarket CLOB WebSocket captures. Use `sync prices` or `collect hourly` for durable bronze price history.

```bash
oddsfox watch --active --top-volume 50 --out ~/.oddsfox
oddsfox watch --market <market_id> --out ~/.oddsfox
```

## Schemas And Metrics

Inspect table schemas:

```bash
oddsfox schema markets
oddsfox schema prices
oddsfox contract --out ~/.oddsfox
```

Compute and inspect metrics:

```bash
oddsfox compute liquidity --active
oddsfox metrics market <market_id> --out ~/.oddsfox
```

Related: [interfaces.md](interfaces.md), [schema.md](schema.md), [operations.md](operations.md).
