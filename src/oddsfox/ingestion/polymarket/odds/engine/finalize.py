from __future__ import annotations

from queue import Queue
from threading import Lock
from typing import Any, Callable, Dict, List

from oddsfox.ingestion.polymarket.odds.deps import OddsSyncRuntime
from oddsfox.ingestion.polymarket.odds.support import (
    PlanningState,
    build_planning_context,
)
from oddsfox.resources.progress_guardrails import NoProgressTimeoutError


def drain_writer(
    *,
    write_queue: Queue,
    writer_thread: Any,
    writer_failures: List[Exception],
    no_progress_error: NoProgressTimeoutError | None,
    progress_poll_seconds: int,
) -> None:
    if no_progress_error is None:
        write_queue.put(None)
        write_queue.join()
        writer_thread.join(timeout=60)
    else:
        try:
            write_queue.put_nowait(None)
        except Exception:
            pass
        writer_thread.join(timeout=max(1, progress_poll_seconds))


def raise_writer_failures(
    writer_failures: List[Exception],
    no_progress_error: NoProgressTimeoutError | None,
) -> None:
    if writer_failures and no_progress_error is None:
        raise RuntimeError(f"Writer thread failed: {writer_failures[0]}")


def build_run_summary(
    *,
    run_started: float,
    runtime: OddsSyncRuntime,
    guardrail: Any,
    raw_pre: Dict[str, Any],
    planning_state: PlanningState,
    invalid_tokens: Dict[str, str],
    totals: Dict[str, int],
    writer_stats: Dict[str, int],
    status_snapshot: Dict[str, int],
    shared_rate_limiter: Any,
    aborted: bool,
    abort_reason: str | None,
) -> Dict[str, Any]:
    run_seconds = max(0.001, runtime.time_mod.monotonic() - run_started)
    token_rate = totals["processed_tokens"] / run_seconds
    row_rate = totals["rows"] / run_seconds
    final_rate = (
        float(shared_rate_limiter.rate)
        if shared_rate_limiter is not None and hasattr(shared_rate_limiter, "rate")
        else None
    )
    planning_context = build_planning_context(
        raw_pre, planning_state, invalid_tokens=len(invalid_tokens)
    )
    raw_post = runtime.snapshot_raw_layer()
    guardrail_snapshot = guardrail.snapshot()
    return {
        "task": "sync_odds",
        "noop": False,
        "aborted": aborted,
        "abort_reason": abort_reason,
        "duration_seconds": round(run_seconds, 3),
        "soft_warning_count": guardrail_snapshot["soft_warning_count"],
        "max_idle_seconds": guardrail_snapshot["max_idle_seconds"],
        "planning": planning_state,
        "planning_context": planning_context,
        "invalid_tokens": len(invalid_tokens),
        "totals": dict(totals),
        "writer": dict(writer_stats),
        "http": dict(status_snapshot),
        "final_rps": final_rate,
        "token_rate": round(token_rate, 4),
        "row_rate": round(row_rate, 4),
        "duckdb_raw_pre": raw_pre,
        "duckdb_raw_post": raw_post,
        "run_seconds": run_seconds,
        "token_rate_value": token_rate,
        "row_rate_value": row_rate,
        "guardrail_snapshot": guardrail_snapshot,
    }


def persist_sync_run_metrics(
    runtime: OddsSyncRuntime,
    *,
    run_summary: Dict[str, Any],
    planning_context: Dict[str, Any],
    planning_state_dict: Dict[str, Any],
    totals: Dict[str, int],
    writer_stats: Dict[str, int],
    status_snapshot: Dict[str, int],
    run_seconds: float,
    token_rate: float,
    row_rate: float,
    final_rate: float | None,
    raw_pre: Dict[str, Any],
    raw_post: Dict[str, Any],
    invalid_token_count: int,
    aborted: bool,
    abort_reason: str | None,
    guardrail_snapshot: Dict[str, Any],
) -> None:
    runtime.save_sync_run_metrics(
        "sync_odds",
        {
            "tokens": totals["processed_tokens"],
            "windows": totals["windows"],
            "rows": totals["rows"],
            "errors": totals["error"],
            "permanent_errors": totals["permanent_error"],
            "empty": totals["empty"],
            "distinct_markets": totals.get("distinct_markets", 0),
            "duration_seconds": round(run_seconds, 3),
            "token_rate": round(token_rate, 4),
            "row_rate": round(row_rate, 4),
            "saved_rows": writer_stats["saved"],
            "saved_daily_rows": writer_stats["saved_daily_rows"],
            "sync_updates": writer_stats["sync_rows"],
            "queue_high_watermark": writer_stats["queue_high_watermark"],
            "http_requests": status_snapshot.get("total", 0),
            "http_429": status_snapshot.get("429", 0),
            "http_errors": status_snapshot.get("error", 0),
            "final_rps": final_rate,
            "noop": False,
            "planning": planning_state_dict,
            "planning_context": planning_context,
            "invalid_tokens": invalid_token_count,
            "totals": run_summary["totals"],
            "writer": run_summary["writer"],
            "http": run_summary["http"],
            "duckdb_raw_pre": raw_pre,
            "duckdb_raw_post": raw_post,
            "aborted": aborted,
            "abort_reason": abort_reason,
            "soft_warning_count": guardrail_snapshot["soft_warning_count"],
            "max_idle_seconds": guardrail_snapshot["max_idle_seconds"],
        },
    )


def finalize_sync_odds_run(
    *,
    runtime: OddsSyncRuntime,
    run_started: float,
    guardrail: Any,
    raw_pre: Dict[str, Any],
    planning_state: PlanningState,
    invalid_tokens: Dict[str, str],
    persist_invalid_tokens_batch: Callable[[List[tuple[str, str]]], None],
    totals: Dict[str, int],
    writer_stats: Dict[str, int],
    runtime_status_lock: Lock,
    runtime_status: Dict[str, int],
    shared_rate_limiter: Any,
    aborted: bool,
    abort_reason: str | None,
    no_progress_error: NoProgressTimeoutError | None,
    write_queue: Queue,
    writer_thread: Any,
    writer_failures: List[Exception],
    progress_poll_seconds: int,
    persist_run_metrics: bool,
    planning_state_to_dict: Callable[[PlanningState], Dict[str, Any]],
) -> Dict[str, Any]:
    drain_writer(
        write_queue=write_queue,
        writer_thread=writer_thread,
        writer_failures=writer_failures,
        no_progress_error=no_progress_error,
        progress_poll_seconds=progress_poll_seconds,
    )
    raise_writer_failures(writer_failures, no_progress_error)
    if invalid_tokens:
        persist_invalid_tokens_batch(list(invalid_tokens.items()))
    with runtime_status_lock:
        status_snapshot = dict(runtime_status)
    partial = build_run_summary(
        run_started=run_started,
        runtime=runtime,
        guardrail=guardrail,
        raw_pre=raw_pre,
        planning_state=planning_state,
        invalid_tokens=invalid_tokens,
        totals=totals,
        writer_stats=writer_stats,
        status_snapshot=status_snapshot,
        shared_rate_limiter=shared_rate_limiter,
        aborted=aborted,
        abort_reason=abort_reason,
    )
    planning_state_dict = planning_state_to_dict(planning_state)
    run_summary = {
        "task": partial["task"],
        "noop": partial["noop"],
        "aborted": partial["aborted"],
        "abort_reason": partial["abort_reason"],
        "duration_seconds": partial["duration_seconds"],
        "soft_warning_count": partial["soft_warning_count"],
        "max_idle_seconds": partial["max_idle_seconds"],
        "planning": planning_state_dict,
        "planning_context": partial["planning_context"],
        "invalid_tokens": partial["invalid_tokens"],
        "totals": partial["totals"],
        "writer": partial["writer"],
        "http": partial["http"],
        "final_rps": partial["final_rps"],
        "token_rate": partial["token_rate"],
        "row_rate": partial["row_rate"],
        "duckdb_raw_pre": partial["duckdb_raw_pre"],
        "duckdb_raw_post": partial["duckdb_raw_post"],
    }
    if persist_run_metrics:
        persist_sync_run_metrics(
            runtime,
            run_summary=run_summary,
            planning_context=partial["planning_context"],
            planning_state_dict=planning_state_dict,
            totals=totals,
            writer_stats=writer_stats,
            status_snapshot=status_snapshot,
            run_seconds=partial["run_seconds"],
            token_rate=partial["token_rate_value"],
            row_rate=partial["row_rate_value"],
            final_rate=partial["final_rps"],
            raw_pre=raw_pre,
            raw_post=partial["duckdb_raw_post"],
            invalid_token_count=len(invalid_tokens),
            aborted=aborted,
            abort_reason=abort_reason,
            guardrail_snapshot=partial["guardrail_snapshot"],
        )
    if no_progress_error is not None:
        raise no_progress_error
    return run_summary
