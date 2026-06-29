# oddsfox v0.2 documentation

Self-hosted, MIT-licensed FOSS data lake creator for prediction-market research.
v0.2 builds a local Polymarket and Kalshi lake end-to-end.

**Current release:** v0.2.0 (medallion Parquet lake, Polymarket + Kalshi sync, user PnL, DuckDB views, local HTTP API + UI).

## Status legend

- **Done** — shipped in v0.2
- **Partial** — some capability exists; gaps documented
- **Deferred** — planned; not in v0.2 binary

See [roadmap.md](roadmap.md) for milestones and deferred features.

## Documentation index

### Core

| Document | Description |
|----------|-------------|
| [overview.md](overview.md) | Product definition and success criteria |
| [architecture.md](architecture.md) | Lake layout and data flow |
| [roadmap.md](roadmap.md) | Milestones and deferred features |
| [cli.md](cli.md) | CLI workflows and recipes |
| [compliance.md](compliance.md) | Safety and data policy |

### Layers and query surfaces

| Document | v0.2 area |
|----------|-----------|
| [storage.md](storage.md) | Bronze / gold layout, partitioning, quarantine |
| [metadata.md](metadata.md) | Runs, sync state, contract, quality manifests |
| [schema.md](schema.md) | Table purposes, join keys, source conventions |
| [interfaces.md](interfaces.md) | DuckDB views, HTTP API, external engines |
| [operations.md](operations.md) | `oddsfox.toml` configuration |

### Examples

Walkthroughs live in [`../examples/`](../examples/):

- [01_sync_active_markets.md](../examples/01_sync_active_markets.md)
- [02_kalshi_market_sync.md](../examples/02_kalshi_market_sync.md)
- [03_user_pnl.md](../examples/03_user_pnl.md)
- [starter_queries.sql](../examples/starter_queries.sql)

## Quick links

- User-facing quick start: [README.md](../README.md)
- Agent/CI instructions: [AGENTS.md](../AGENTS.md)
