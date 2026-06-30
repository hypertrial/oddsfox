from __future__ import annotations

import logging
import time
from typing import Any, Dict

from oddsfox.storage.duckdb import (
    ensure_duck_db,
    reconcile_token_sync_ledger_from_history,
    save_sync_run_metrics,
    snapshot_raw_layer,
)

logger = logging.getLogger(__name__)


def init_db():
    ensure_duck_db()


def reconcile_odds_ledger(*, persist_run_metrics: bool = True) -> Dict[str, Any]:
    ensure_duck_db()
    raw_pre = snapshot_raw_layer()
    started = time.monotonic()
    summary = reconcile_token_sync_ledger_from_history()
    duration = max(0.001, time.monotonic() - started)
    raw_post = snapshot_raw_layer()
    logger.info(
        "Odds ledger reconciliation complete: scanned=%s repaired=%s duration=%.2fs",
        summary.get("scanned_tokens", 0),
        summary.get("repaired_tokens", 0),
        duration,
    )
    if persist_run_metrics:
        save_sync_run_metrics(
            "reconcile_odds_ledger",
            {
                "scanned_tokens": summary.get("scanned_tokens", 0),
                "repaired_tokens": summary.get("repaired_tokens", 0),
                "duration_seconds": round(duration, 3),
                "duckdb_raw_pre": raw_pre,
                "duckdb_raw_post": raw_post,
            },
        )
    out: Dict[str, Any] = {
        "task": "reconcile_odds_ledger",
        "duration_seconds": round(duration, 3),
        "duckdb_raw_pre": raw_pre,
        "duckdb_raw_post": raw_post,
    }
    out.update(summary)
    return out
