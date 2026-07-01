# Data Contracts

This page summarizes the public analytics surface that downstream notebooks,
scripts, and operators should rely on. OddsFox is a prediction-market pipeline;
the current public marts are Polymarket/WC2026 outputs. Model-level column docs
and tests live in the dbt project.

## Public Marts

Schema: `polymarket_marts`

| Relation | Grain | Contract |
| --- | --- | --- |
| `wc2026_markets` | One row per WC2026 market | Canonical scoped WC2026 market universe. |
| `token_coverage` | One row per `clob_token_id` | Token health, daily coverage, sync ledger state, skip state, gap health, and market fully checked rollups. |
| `market_coverage` | One row per market | Market-level coverage rolled up from `token_coverage`. |
| `token_latest_odds` | One row per WC2026 token | Latest daily OHLC and latest point-in-time odds. |
| `wc2026_whale_minutely_odds` | One row per token timestamp | Minutely odds for WC2026 markets above the configured volume threshold. |

## Health And Observability

- Use `polymarket_marts.token_coverage` for token health and market fully
  checked status.
- Use `polymarket_marts.market_coverage` for market-level coverage summaries.
- Use `polymarket_observability.sync_run_observability` for run-level ingestion
  telemetry, request counts, and sync metrics.

## Current Scope Rules

- `token_coverage` covers all staged tokens.
- `token_latest_odds` is intentionally WC2026-scoped.
- `wc2026_whale_minutely_odds` is WC2026-scoped and filtered by
  `polymarket_whale_min_volume_usd`.
- After `make prune-odds-history`, `polymarket_raw.odds_history` (and therefore
  `wc2026_whale_minutely_odds`) only guarantees the trailing ~365 days of minutely
  points unless you change the retention window.
- `int_polymarket_wc2026_markets` is the canonical market-level WC2026 scope.

## dbt Checks

`uv run make dbt-build` runs model builds and singular tests for:

- Source and staging grain.
- Price sanity and OHLC bounds.
- Token and market coverage consistency.
- Latest odds parity with intermediate time series.
- WC2026 scope and whale volume threshold filtering.
