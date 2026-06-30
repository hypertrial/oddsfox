from __future__ import annotations

import logging
from datetime import datetime, timezone
from queue import Empty, Queue
from typing import Dict, List

from oddsfox.ingestion.polymarket.odds.support import (
    MAX_FLUSH_ROWS_CAP,
    WriterBuffers,
)
from oddsfox.storage.duckdb import (
    get_connection,
    refresh_token_odds_daily,
    save_odds_bulk_upsert,
    upsert_skipped_tokens_batch,
    upsert_token_sync_state_batch,
)

logger = logging.getLogger(__name__)


def dynamic_writer_flush_rows(base_rows: int, write_queue: Queue) -> int:
    base_rows = max(1_000, int(base_rows))
    maxsize = int(getattr(write_queue, "maxsize", 0) or 0)
    if maxsize <= 0:
        return base_rows
    try:
        qsize = int(write_queue.qsize())
    except Exception:
        return base_rows
    utilization = qsize / max(1, maxsize)
    if utilization >= 0.8:
        return max(1_000, base_rows // 4)
    if utilization >= 0.5:
        return max(1_000, base_rows // 2)
    if utilization <= 0.1:
        return min(MAX_FLUSH_ROWS_CAP, base_rows * 2)
    return base_rows


def flush_writer_buffers(
    conn,
    buffers: WriterBuffers,
    writer_stats: Dict[str, int],
    writer_flush_rows: int,
    *,
    force: bool = False,
    save_odds_bulk_upsert_fn=save_odds_bulk_upsert,
    upsert_token_sync_state_batch_fn=upsert_token_sync_state_batch,
    upsert_skipped_tokens_batch_fn=upsert_skipped_tokens_batch,
):
    pending_rows = (
        len(buffers.odds_map) + len(buffers.state_buffer) + len(buffers.skip_buffer)
    )
    if not force and pending_rows < writer_flush_rows:
        return
    if not buffers.odds_map and not buffers.state_buffer and not buffers.skip_buffer:
        return
    state_map = {}
    for token_state in buffers.state_buffer:
        token_id = token_state[0]
        current = state_map.get(token_id)
        if current is None:
            state_map[token_id] = token_state
            continue
        current_cursor = current[1]
        next_cursor = token_state[1]
        merged_cursor = current_cursor
        if next_cursor is not None and (
            merged_cursor is None or int(next_cursor) > int(merged_cursor)
        ):
            merged_cursor = next_cursor
        state_map[token_id] = (
            token_id,
            merged_cursor,
            token_state[2] or current[2],
            token_state[3] or current[3],
            token_state[4],
            bool(current[5] or token_state[5]),
        )
    skip_map = {token_id: reason for token_id, reason in buffers.skip_buffer}
    odds_records = [
        (token_id, ts, price) for (token_id, ts), price in buffers.odds_map.items()
    ]
    daily_keys = sorted(
        {
            (token_id, datetime.fromtimestamp(int(ts), tz=timezone.utc).date())
            for token_id, ts, _ in odds_records
        }
    )
    buffers.dirty_daily_keys.update(daily_keys)
    conn.execute("BEGIN")
    try:
        if odds_records:
            save_odds_bulk_upsert_fn(odds_records, conn, assume_deduped=True)
            writer_stats["saved"] += len(odds_records)
        if state_map:
            state_rows = list(state_map.values())
            upsert_token_sync_state_batch_fn(state_rows, conn)
            writer_stats["sync_rows"] += len(state_rows)
            writer_stats["full_rows"] += sum(
                1 for _, _, _, _, _, fully_checked in state_rows if fully_checked
            )
        if skip_map:
            upsert_skipped_tokens_batch_fn(list(skip_map.items()), conn)
            writer_stats["skip_rows"] += len(skip_map)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    buffers.odds_map.clear()
    buffers.state_buffer.clear()
    buffers.skip_buffer.clear()


def refresh_dirty_daily_keys(
    conn,
    buffers: WriterBuffers,
    writer_stats: Dict[str, int],
    *,
    refresh_token_odds_daily_fn=refresh_token_odds_daily,
    chunk_size: int = 50_000,
) -> None:
    if not buffers.dirty_daily_keys:
        return
    keys = sorted(buffers.dirty_daily_keys)
    for i in range(0, len(keys), max(1, int(chunk_size))):
        chunk = keys[i : i + max(1, int(chunk_size))]
        refresh_token_odds_daily_fn(chunk, conn)
    writer_stats["saved_daily_rows"] += len(keys)
    buffers.dirty_daily_keys.clear()


def apply_writer_item(item, buffers: WriterBuffers, writer_stats: Dict[str, int]):
    op_type, data = item
    if op_type == "odds":
        for token_id, ts, price in data:
            ts = int(ts)
            price = float(price)
            if ts <= 0:
                writer_stats["invalid_ts_dropped"] += 1
                continue
            if price < 0.0 or price > 1.0:
                writer_stats["invalid_price_dropped"] += 1
                continue
            key = (token_id, int(ts))
            if key in buffers.odds_map:
                writer_stats["deduped"] += 1
            buffers.odds_map[key] = price
    elif op_type == "token_state":
        buffers.state_buffer.extend(data)
    elif op_type == "skipped_tokens":
        buffers.skip_buffer.extend(data)


def writer_loop(
    write_queue: Queue,
    writer_flush_rows: int,
    writer_stats: Dict[str, int],
    writer_failures: List[Exception],
    *,
    get_connection_fn=get_connection,
    dynamic_writer_flush_rows_fn=dynamic_writer_flush_rows,
    flush_writer_buffers_fn=flush_writer_buffers,
    apply_writer_item_fn=apply_writer_item,
    refresh_dirty_daily_keys_fn=refresh_dirty_daily_keys,
):
    buffers = WriterBuffers(odds_map={}, state_buffer=[], skip_buffer=[])
    fatal_error = False
    with get_connection_fn() as conn:
        while True:
            try:
                item = write_queue.get(timeout=0.5)
            except Empty:
                if not fatal_error:
                    try:
                        flush_rows = dynamic_writer_flush_rows_fn(
                            writer_flush_rows, write_queue
                        )
                        flush_writer_buffers_fn(
                            conn, buffers, writer_stats, flush_rows, force=False
                        )
                    except Exception as exc:
                        logger.error("Writer flush failed: %s", exc)
                        writer_failures.append(exc)
                        fatal_error = True
                continue
            if item is None:
                write_queue.task_done()
                break
            try:
                if not fatal_error:
                    apply_writer_item_fn(item, buffers, writer_stats)
            finally:
                write_queue.task_done()
            if not fatal_error:
                try:
                    flush_rows = dynamic_writer_flush_rows_fn(
                        writer_flush_rows, write_queue
                    )
                    flush_writer_buffers_fn(
                        conn, buffers, writer_stats, flush_rows, force=False
                    )
                except Exception as exc:
                    logger.error("Writer flush failed: %s", exc)
                    writer_failures.append(exc)
                    fatal_error = True
        if not fatal_error:
            try:
                flush_writer_buffers_fn(
                    conn, buffers, writer_stats, writer_flush_rows, force=True
                )
                refresh_dirty_daily_keys_fn(conn, buffers, writer_stats)
            except Exception as exc:
                logger.error("Final writer flush failed: %s", exc)
                writer_failures.append(exc)


def maybe_auto_tune_rps(
    *,
    limiter,
    runtime_status: Dict[str, int],
    tune_state: Dict[str, float],
    window_requests: int,
    threshold_429: float,
    threshold_error: float,
    min_rps: int,
    max_rps: int,
):
    if limiter is None:
        return
    total = int(runtime_status.get("total", 0))
    if total <= 0:
        return
    last_total = int(tune_state.get("last_total", 0))
    if (total - last_total) < max(1, int(window_requests)):
        return
    last_429 = int(tune_state.get("last_429", 0))
    last_error = int(tune_state.get("last_error", 0))
    current_429 = int(runtime_status.get("429", 0))
    current_error = int(runtime_status.get("error", 0))
    delta_total = total - last_total
    delta_429 = max(0, current_429 - last_429)
    delta_error = max(0, current_error - last_error)
    ratio_429 = delta_429 / max(1, delta_total)
    ratio_error = delta_error / max(1, delta_total)
    try:
        current_rate = (
            float(limiter.get_rate())
            if hasattr(limiter, "get_rate")
            else float(getattr(limiter, "rate", 0) or 0)
        )
    except Exception:
        current_rate = 0
    if current_rate <= 0:
        return
    target_rate = current_rate
    if ratio_429 > max(0.0, float(threshold_429)):
        target_rate = max(float(min_rps), current_rate * 0.8)
    elif ratio_error > max(0.0, float(threshold_error)):
        target_rate = max(float(min_rps), current_rate * 0.9)
    elif delta_429 == 0:
        target_rate = min(float(max_rps), current_rate * 1.1)
    if abs(target_rate - current_rate) >= 0.5 and hasattr(limiter, "set_rate"):
        limiter.set_rate(target_rate)
        logger.info(
            "Auto-tuned odds RPS: %.1f -> %.1f (window_requests=%s 429_ratio=%.3f error_ratio=%.3f)",
            current_rate,
            target_rate,
            delta_total,
            ratio_429,
            ratio_error,
        )
    tune_state["last_total"] = total
    tune_state["last_429"] = current_429
    tune_state["last_error"] = current_error


__all__ = [
    "apply_writer_item",
    "dynamic_writer_flush_rows",
    "flush_writer_buffers",
    "refresh_dirty_daily_keys",
    "maybe_auto_tune_rps",
    "writer_loop",
]
