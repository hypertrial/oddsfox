# Scripts

Operator scripts live under `scripts/`.
Run them through `uv run python` so they use the repo environment.

## Warehouse

- `profile_warehouse.py`: inspect schemas, relations, row counts, and stats.
- `compact_warehouse.py`: rewrite the DuckDB file into a compact copy and swap it into place.
- `prune_odds_history.py`: delete `polymarket_raw.odds_history` rows older than a retention window (default 365 days).
- `repair_polymarket_token_sync_ledger.py`: rebuild a corrupted token sync ledger.
- `audit_legacy_warehouse_layout.py`: detect old schema layouts in a warehouse file.

Makefile shortcuts (stop Dagster and other writers first):

```bash
make prune-odds-history          # default 365-day retention; add --dry-run via script directly
make compact-warehouse           # reclaim dead space after rebuilds or pruning
```

## Current Polymarket Scope

- `audit_polymarket_wc2026_scope.py`: compare registry, allowlist, and strict WC2026 scope.
- `audit_wc2026_tag_coverage.py`: crawl Gamma tag/search discovery and report registry gaps.
- `count_gamma_tag_events.py`: count Gamma events for WC2026 scope tags.

Run scripts through the project environment:

```bash
uv run python scripts/profile_warehouse.py --snapshot-copy
```

Scripts that call Polymarket APIs need network access and should use conservative request-rate settings.
