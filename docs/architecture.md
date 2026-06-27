# Architecture

```mermaid
flowchart TB
    gamma[GammaREST]
    clob[CLOB_REST_WS]
    dataapi[DataAPI]
    raw[_raw]
    bronze[bronze]
    silver[silver]
    gold[gold]
    duckdb[catalog.duckdb]
    iface[CLI_API_UI]

    gamma --> raw
    clob --> raw
    dataapi --> raw
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
