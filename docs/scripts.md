# Scripts

Operator scripts live under `scripts/`.

## Warehouse

- `profile_warehouse.py`: inspect schemas, relations, row counts, and stats.
- `compact_warehouse.py`: rewrite the DuckDB file into a compact copy and swap it into place.
- `repair_polymarket_token_sync_ledger.py`: rebuild a corrupted token sync ledger.
- `audit_legacy_warehouse_layout.py`: detect old schema layouts in a warehouse file.

## Polymarket Scope

- `audit_polymarket_wc2026_scope.py`: compare registry, allowlist, and strict WC2026 scope.
- `audit_wc2026_tag_coverage.py`: crawl Gamma tag/search discovery and report registry gaps.
- `count_gamma_tag_events.py`: count Gamma events for WC2026 scope tags.

Run scripts through the project environment:

```bash
uv run python scripts/profile_warehouse.py --snapshot-copy
```

Scripts that call Polymarket APIs need network access and should use conservative request-rate settings.
