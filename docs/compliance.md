# Compliance

oddsfox is **code-only** FOSS. It does not ship Polymarket or Kalshi data.

Users fetch data directly from Polymarket and Kalshi APIs under their own access rights. Local caches are created on the user's machine.
Kalshi API keys, when configured, are used only for read-only market-data and portfolio endpoints.
User PnL sync stores only data the user requests locally, under their own API access or public wallet/proxy address.

## Out of scope

- Historical dumps or hosted mirrors
- Trade execution or auto-betting
- Order submission, balance transfer, wallet custody, or hosted account data
- Geo-bypass tooling

## Research caveats

Public order-book feeds may disagree with on-chain trade direction. Quote-lifecycle attribution is structurally unavailable from public chain data alone. Quality flags in `_metadata/data_quality.parquet` document these limits.
