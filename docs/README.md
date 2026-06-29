# oddsfox analyst docs

oddsfox builds a local Polymarket and Kalshi analytics lake. Start with the job you need, then use the reference docs when you need table or storage detail.

## Start Here

| Need | Start with |
|------|------------|
| Run a first demo | [README quickstart](../README.md#1-try-it-now) |
| Keep all-market hourly data collecting | [CLI: hourly collector](cli.md#durable-hourly-collection) |
| Run one active-market refresh | [CLI: active refresh](cli.md#active-market-refresh) |
| Query data with SQL | [Interfaces: SQL and DuckDB](interfaces.md#sql-first-querying) |
| Learn the core joins | [Schema: minimum joins](schema.md#minimum-joins-analysts-need) |
| Operate or reset cursors | [Operations: hourly collector](operations.md#hourly-collector-operations) |

## Run Data Collection

| Document | Use it for |
|----------|------------|
| [cli.md](cli.md) | Analyst workflows, copy-paste commands, restart behavior |
| [operations.md](operations.md) | Config, Kalshi keys, rate limits, forever collector cursors |
| [metadata.md](metadata.md) | Run logs, sync state, cursor inspection |

## Query Data

| Document | Use it for |
|----------|------------|
| [interfaces.md](interfaces.md) | `oddsfox sql`, DuckDB, local HTTP API, direct Parquet scans |
| [schema.md](schema.md) | Table meanings, join keys, source conventions |
| [../examples/starter_queries.sql](../examples/starter_queries.sql) | Copy-paste SQL recipes |

## Understand The Lake

| Document | Use it for |
|----------|------------|
| [storage.md](storage.md) | Parquet layout, hourly price files, raw and quarantine directories |
| [metadata.md](metadata.md) | Completed-run visibility, checkpoints, contract files |
| [overview.md](overview.md) | Product goals and non-goals |

## Examples

- [Active refresh walkthrough](../examples/01_sync_active_markets.md)
- [Kalshi market workflow](../examples/02_kalshi_market_sync.md)
- [User PnL workflow](../examples/03_user_pnl.md)
- [Starter SQL queries](../examples/starter_queries.sql)

## Internal And Contributor Reference

| Document | Use it for |
|----------|------------|
| [architecture.md](architecture.md) | Data flow and implementation shape |
| [roadmap.md](roadmap.md) | Milestones and deferred work |
| [compliance.md](compliance.md) | Safety and data policy |

Current release: v0.2.0.
