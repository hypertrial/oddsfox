# CLI reference

See `oddsfox --help` for full flags.

## Core workflow

```bash
oddsfox init --out ~/.oddsfox
oddsfox sync markets --active
oddsfox sync prices --active --interval 1d --fidelity 60
oddsfox snapshot books --active --top-volume 100
oddsfox compute all --since 2024-01-01
oddsfox duckdb --out ~/.oddsfox --db ~/.oddsfox/catalog.duckdb
oddsfox serve --port 8787
```

## Explore

```bash
oddsfox search "election"
oddsfox market <market_id>
oddsfox event <event_id>
oddsfox resolved --since 2024-01-01
oddsfox top --by volume_24h
```
