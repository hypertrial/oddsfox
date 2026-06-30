import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from oddsfox.storage.duckdb.connection import (
    ensure_duck_db,
    get_connection,
    polymarket_ops_tbl,
)

_BACKFILL_KEY_PREFIX = "backfill:"

logger = logging.getLogger(__name__)


def _metadata_get(key: str) -> Optional[str]:
    ensure_duck_db()
    with get_connection() as conn:
        row = conn.execute(
            f"SELECT value FROM {polymarket_ops_tbl('scrape_metadata')} WHERE key = ?",
            [key],
        ).fetchone()
        return row[0] if row else None


def _metadata_set(key: str, value: str):
    ensure_duck_db()
    with get_connection() as conn:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {polymarket_ops_tbl("scrape_metadata")} (key, value)
            VALUES (?, ?)
            """,
            [key, value],
        )


def get_backfill_fully_checked(task: str) -> Optional[bool]:
    """Return ledger status for a backfill task (True/False) or None if unset."""
    key = f"{_BACKFILL_KEY_PREFIX}{task}:fully_checked"
    raw = _metadata_get(key)
    if raw is None:
        return None
    return raw.lower() in ("1", "true", "yes")


def set_backfill_fully_checked(task: str, fully_checked: bool):
    """Persist ledger status for a backfill task and update timestamp."""
    now_iso = datetime.now(timezone.utc).isoformat()
    _metadata_set(
        f"{_BACKFILL_KEY_PREFIX}{task}:fully_checked",
        "1" if fully_checked else "0",
    )
    _metadata_set(f"{_BACKFILL_KEY_PREFIX}{task}:timestamp", now_iso)


def get_backfill_progress(task: str) -> int:
    """Return the last processed count for a backfill task (0 if unset)."""
    raw = _metadata_get(f"{_BACKFILL_KEY_PREFIX}{task}:progress")
    try:
        return int(raw) if raw is not None else 0
    except ValueError:
        return 0


def set_backfill_progress(task: str, processed: int):
    """Persist the last processed count for a backfill task."""
    _metadata_set(f"{_BACKFILL_KEY_PREFIX}{task}:progress", str(int(processed)))


def append_pipeline_run_event(
    task_name: str, metrics: dict[str, Any], *, recorded_at: Optional[datetime] = None
) -> str:
    """
    Append one row to the append-only pipeline_run_events table for queryable audit history.

    Returns:
        run_id: UUID string primary key for this event.
    """
    ensure_duck_db()
    run_id = str(uuid.uuid4())
    ts = recorded_at if recorded_at is not None else datetime.now(timezone.utc)
    payload = dict(metrics)
    payload["timestamp"] = ts.isoformat()
    with get_connection() as conn:
        conn.execute(
            f"""
            INSERT INTO {polymarket_ops_tbl("pipeline_run_events")} (
                run_id, task_name, recorded_at, metrics_json
            )
            VALUES (?, ?, ?, ?)
            """,
            [run_id, task_name, ts, json.dumps(payload, sort_keys=True)],
        )
    return run_id


def save_sync_run_metrics(task: str, metrics: dict[str, Any], history_limit: int = 20):
    """
    Persist latest sync metrics and a short rolling history in scrape_metadata.
    """
    recorded = datetime.now(timezone.utc)
    now_iso = recorded.isoformat()
    base_key = f"sync_metrics:{task}"
    payload = dict(metrics)
    payload["timestamp"] = now_iso

    try:
        append_pipeline_run_event(task, payload, recorded_at=recorded)
    except Exception as exc:
        logger.warning(
            "pipeline_run_events append failed (continuing with scrape_metadata): %s",
            exc,
        )

    _metadata_set(f"{base_key}:last", json.dumps(payload, sort_keys=True))

    history_key = f"{base_key}:history"
    history_raw = _metadata_get(history_key)
    history: list[dict[str, Any]] = []
    if history_raw:
        try:
            parsed = json.loads(history_raw)
            if isinstance(parsed, list):
                history = [item for item in parsed if isinstance(item, dict)]
        except json.JSONDecodeError:
            history = []

    history.append(payload)
    if history_limit > 0:
        history = history[-int(history_limit) :]
    _metadata_set(history_key, json.dumps(history, sort_keys=True))
    with get_connection() as conn:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {polymarket_ops_tbl("sync_run_metrics")} (
                task_name, recorded_at, metrics_json, history_json
            )
            VALUES (?, ?, ?, ?)
            """,
            [
                task,
                recorded,
                json.dumps(payload, sort_keys=True),
                json.dumps(history, sort_keys=True),
            ],
        )

    logger.info(
        "Persisted sync_metrics task=%s timestamp=%s keys=%s",
        task,
        now_iso,
        sorted(k for k in payload.keys() if k != "timestamp"),
    )


def get_sync_run_metrics(task: str) -> Optional[dict[str, Any]]:
    """Return the most recent sync metrics payload for task, if present."""
    ensure_duck_db()
    with get_connection() as conn:
        try:
            row = conn.execute(
                f"""
                SELECT metrics_json
                FROM {polymarket_ops_tbl("sync_run_metrics")}
                WHERE task_name = ?
                """,
                [task],
            ).fetchone()
        except Exception:
            row = None
    if row and row[0] is not None:
        try:
            parsed_table = json.loads(str(row[0]))
        except json.JSONDecodeError:
            parsed_table = None
        if isinstance(parsed_table, dict):
            return parsed_table

    raw = _metadata_get(f"sync_metrics:{task}:last")
    if raw is None:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


_WC2026_DISCOVERY_PREFIX = "wc2026_markets_discovery:"


def get_wc2026_discovery_fully_checked() -> Optional[bool]:
    """Return whether a full keyset WC2026 markets discovery completed without truncation."""
    raw = _metadata_get(f"{_WC2026_DISCOVERY_PREFIX}fully_checked")
    if raw is None:
        return None
    return raw.lower() in ("1", "true", "yes")


def get_wc2026_discovery_scope_config_hash() -> Optional[str]:
    raw = _metadata_get(f"{_WC2026_DISCOVERY_PREFIX}scope_config_hash")
    return raw if raw else None


def set_wc2026_discovery_fully_checked(
    fully_checked: bool, *, scope_config_hash: str
) -> None:
    """Persist full keyset discovery completion and scope config hash."""
    now_iso = datetime.now(timezone.utc).isoformat()
    _metadata_set(
        f"{_WC2026_DISCOVERY_PREFIX}fully_checked",
        "1" if fully_checked else "0",
    )
    _metadata_set(f"{_WC2026_DISCOVERY_PREFIX}scope_config_hash", scope_config_hash)
    _metadata_set(f"{_WC2026_DISCOVERY_PREFIX}last_run_at", now_iso)


__all__ = [
    "_metadata_get",
    "_metadata_set",
    "append_pipeline_run_event",
    "get_backfill_fully_checked",
    "set_backfill_fully_checked",
    "get_backfill_progress",
    "set_backfill_progress",
    "get_sync_run_metrics",
    "get_wc2026_discovery_fully_checked",
    "get_wc2026_discovery_scope_config_hash",
    "save_sync_run_metrics",
    "set_wc2026_discovery_fully_checked",
]
