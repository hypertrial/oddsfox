# Architecture

oddsfox is an end-to-end local data lake creator: public source APIs land in `_raw`, become bronze/silver/gold Parquet, are registered in DuckDB, and are exposed through CLI, SQL, HTTP API, and UI.
The current source implementations are Polymarket and Kalshi. Both write the same bronze tables using `source` and prefixed IDs.
Read-only user PnL sync adds `bronze_user_fills`, `bronze_user_positions`, and `gold_user_pnl` for user-supplied Polymarket wallets/proxy addresses and authenticated Kalshi portfolio reads.

```mermaid
flowchart TB
    gamma[GammaREST]
    clob[CLOB_REST_WS]
    dataapi[DataAPI]
    kalshi[KalshiREST]
    raw[_raw]
    bronze[bronze]
    silver[silver]
    gold[gold]
    duckdb[catalog.duckdb]
    iface[CLI_API_UI]

    gamma --> raw
    clob --> raw
    dataapi --> raw
    kalshi --> raw
    raw --> bronze
    bronze --> silver
    silver --> gold
    bronze --> duckdb
    gold --> duckdb
    duckdb --> iface
```

## Module map

See [AGENTS.md](../AGENTS.md) for file-level responsibilities.

## Lake contract

Published at `_metadata/contract.json`. Bump `lake_contract_version()` on breaking schema changes.
