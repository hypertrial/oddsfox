# Compliance

oddsfox is **code-only**. It does not ship Polymarket data.

Users fetch data directly from Polymarket public APIs under their own access rights. Local caches are created on the user's machine.

## Out of scope

- Historical dumps or hosted mirrors
- Trade execution or auto-betting
- Geo-bypass tooling

## Research caveats

Public order-book feeds may disagree with on-chain trade direction. Quote-lifecycle attribution is structurally unavailable from public chain data alone. Quality flags in `_metadata/data_quality.parquet` document these limits.
