from __future__ import annotations

import logging
from concurrent.futures import FIRST_COMPLETED, CancelledError
from dataclasses import dataclass
from queue import Queue
from threading import Lock, local
from typing import Any, Callable, Dict, List

from oddsfox.config.settings import CLOB_API_URL, ODDS_REQUESTS_PER_SECOND
from oddsfox.ingestion.polymarket.errors import ClobRequestError
from oddsfox.ingestion.polymarket.odds.deps import OddsSyncRuntime
from oddsfox.ingestion.polymarket.odds.execution import InflightTokenFuture
from oddsfox.ingestion.polymarket.odds.fetch import build_client
from oddsfox.ingestion.polymarket.odds.support import (
    MAX_INFLIGHT_CAP,
    MAX_WORKERS_CAP,
    build_inflight_future_diagnostics,
    build_planning_context,
    log_planning_context,
    log_planning_state,
)
from oddsfox.ingestion.polymarket.odds.writer import maybe_auto_tune_rps
from oddsfox.resources.progress_guardrails import NoProgressTimeoutError

from .bootstrap import NormalizedSyncParams, PlanningBootstrap

logger = logging.getLogger(__name__)


@dataclass
class PoolResources:
    effective_workers: int
    shared_rate_limiter: Any
    runtime_status: Dict[str, int]
    runtime_status_lock: Lock
    tune_state: Dict[str, int]
    get_worker_client: Callable[[], Any]
    write_queue: Queue
    writer_stats: Dict[str, int]
    writer_failures: List[Exception]
    writer_thread: Any
    totals: Dict[str, int]


def setup_rate_limiting(
    *,
    max_workers: int,
    requests_per_second: int | None,
    rate_limiter_factory: Callable[[int | None], Any] | None,
    client_factory: Callable[[], Any] | None,
    runtime: OddsSyncRuntime,
) -> tuple[
    int,
    Any,
    Dict[str, int],
    Lock,
    Dict[str, int],
    Callable[[], Any],
    Callable[[int], None],
]:
    requested_workers = int(max_workers)
    effective_workers = min(MAX_WORKERS_CAP, max(1, requested_workers))
    rate_limiter_factory = rate_limiter_factory or runtime.default_rate_limiter_factory
    configured_rps = (
        requests_per_second
        if requests_per_second is not None
        else (ODDS_REQUESTS_PER_SECOND or max_workers)
    )
    configured_rps = max(1, int(configured_rps)) if configured_rps else None
    shared_rate_limiter = rate_limiter_factory(configured_rps)
    runtime_status = {"total": 0, "429": 0, "error": 0}
    runtime_status_lock = Lock()

    def on_http_status(status_code: int):
        with runtime_status_lock:
            runtime_status["total"] += 1
            status_code = int(status_code)
            if status_code == 429:
                runtime_status["429"] += 1
            if status_code < 200 or status_code >= 400:
                runtime_status["error"] += 1

    tune_state = {"last_total": 0, "last_429": 0, "last_error": 0}
    factory = client_factory or (
        lambda: build_client(CLOB_API_URL, rate_limiter=shared_rate_limiter)
    )
    thread_local = local()

    def get_worker_client():
        client = getattr(thread_local, "client", None)
        if client is None:
            client = factory()
            thread_local.client = client
        return client

    return (
        effective_workers,
        shared_rate_limiter,
        runtime_status,
        runtime_status_lock,
        tune_state,
        get_worker_client,
        on_http_status,
    )


def setup_writer(
    runtime: OddsSyncRuntime,
    *,
    effective_workers: int,
    writer_flush_rows: int,
) -> tuple[Queue, Dict[str, int], List[Exception], Any]:
    write_queue: Queue = Queue(
        maxsize=min(max(100, effective_workers * 8), MAX_INFLIGHT_CAP)
    )
    writer_stats = {
        "saved": 0,
        "saved_daily_rows": 0,
        "deduped": 0,
        "sync_rows": 0,
        "skip_rows": 0,
        "full_rows": 0,
        "invalid_ts_dropped": 0,
        "invalid_price_dropped": 0,
        "queue_high_watermark": 0,
    }
    writer_failures: List[Exception] = []
    writer_thread = runtime.thread_cls(
        target=runtime.writer_loop,
        args=(write_queue, writer_flush_rows, writer_stats, writer_failures),
        daemon=True,
    )
    writer_thread.start()
    return write_queue, writer_stats, writer_failures, writer_thread


@dataclass
class PoolRunResult:
    planning_state: Any
    invalid_tokens: Dict[str, str]
    totals: Dict[str, int]
    aborted: bool
    abort_reason: str | None
    no_progress_error: NoProgressTimeoutError | None
    executor_shutdown: bool


def run_sync_pool(
    runtime: OddsSyncRuntime,
    boot: PlanningBootstrap,
    params: NormalizedSyncParams,
    guardrail: Any,
    pool: PoolResources,
    *,
    first_plan: Any,
    effective_max_rps: int,
    auto_tune_rps: bool,
    auto_tune_window_requests: int,
    auto_tune_429_threshold: float,
    auto_tune_error_threshold: float,
    auto_tune_min_rps: int,
    transient_retries: int,
    transient_backoff_seconds: float,
    progress_callback: Callable[[str, dict[str, Any]], None] | None,
    progress_poll_seconds: int,
    on_http_status: Callable[[int], None],
) -> PoolRunResult:
    totals = pool.totals
    write_queue = pool.write_queue
    writer_stats = pool.writer_stats
    shared_rate_limiter = pool.shared_rate_limiter
    runtime_status = pool.runtime_status
    runtime_status_lock = pool.runtime_status_lock
    tune_state = pool.tune_state
    get_worker_client = pool.get_worker_client

    max_inflight = min(max(pool.effective_workers * 8, 64), MAX_INFLIGHT_CAP)
    futures: Dict[object, InflightTokenFuture] = {}
    exhausted_plans = False
    aborted = False
    abort_reason: str | None = None
    no_progress_error: NoProgressTimeoutError | None = None
    planning_state = boot.planning_state
    invalid_tokens = boot.invalid_tokens
    plan_iter = boot.plan_iter
    raw_pre = boot.raw_pre
    completed_markets: set[str] = set()
    progress_total = boot.candidate_tokens or None
    candidate_markets = boot.candidate_markets

    executor = runtime.thread_pool_executor(max_workers=pool.effective_workers)
    executor_shutdown = False
    try:

        def submit_plan(plan):
            future = executor.submit(
                runtime.sync_token_plan,
                plan,
                get_worker_client,
                write_queue,
                params.window_seconds,
                params.writer_chunk_rows,
                params.min_split_window_seconds,
                params.routine_interval_seconds,
                params.empty_retry_base_seconds,
                params.empty_retry_max_seconds,
                params.error_retry_seconds,
                transient_retries,
                transient_backoff_seconds,
                on_http_status,
            )
            futures[future] = InflightTokenFuture(
                plan=plan, submitted_at=runtime.time_mod.monotonic()
            )

        submit_plan(first_plan)
        with runtime.tqdm_mod(
            desc="Syncing odds (price history)",
            unit="token",
            ncols=110,
            total=progress_total,
        ) as pbar:
            while True:
                while not exhausted_plans and len(futures) < max_inflight:
                    try:
                        plan = next(plan_iter)
                    except StopIteration as done:
                        exhausted_plans = True
                        if done.value:
                            planning_state, invalid_tokens = done.value
                        log_planning_state(planning_state)
                        log_planning_context(
                            build_planning_context(
                                raw_pre,
                                planning_state,
                                invalid_tokens=len(invalid_tokens),
                            )
                        )
                        break
                    submit_plan(plan)
                if exhausted_plans and not futures:
                    break
                try:
                    done_futures, _ = runtime.wait_fn(
                        set(futures.keys()),
                        timeout=float(progress_poll_seconds),
                        return_when=FIRST_COMPLETED,
                    )
                except TypeError:
                    done_futures, _ = runtime.wait_fn(
                        set(futures.keys()), return_when=FIRST_COMPLETED
                    )
                if not done_futures:
                    diagnostics = {
                        "inflight_futures": len(futures),
                        "queue_size": write_queue.qsize(),
                        "queue_high_watermark": writer_stats["queue_high_watermark"],
                        "http": dict(runtime_status),
                        "final_rps": float(shared_rate_limiter.get_rate())
                        if shared_rate_limiter is not None
                        and hasattr(shared_rate_limiter, "get_rate")
                        else (
                            float(getattr(shared_rate_limiter, "rate", 0.0))
                            if shared_rate_limiter is not None
                            else None
                        ),
                    }
                    diagnostics.update(build_inflight_future_diagnostics(futures))
                    guardrail.check(
                        phase="waiting_for_token_futures", diagnostics=diagnostics
                    )
                    continue
                for future in done_futures:
                    inflight = futures.pop(future)
                    plan = inflight.plan
                    totals["processed_tokens"] += 1
                    completed_markets.add(plan.market_id)
                    totals["distinct_markets"] = len(completed_markets)
                    guardrail.record_progress(
                        work_increment=1,
                        phase="token_future_completed",
                        diagnostics={
                            "processed_tokens": totals["processed_tokens"],
                            "token_id_prefix": plan.token_id[:24],
                            "inflight_futures": len(futures),
                        },
                    )
                    try:
                        result = future.result()
                    except (
                        CancelledError,
                        ClobRequestError,
                        OSError,
                        RuntimeError,
                    ) as exc:
                        logger.error("Token %s failed: %s", plan.token_id[:24], exc)
                        totals["error"] += 1
                        pbar.update(1)
                        continue
                    totals["rows"] += int(result["rows"])
                    totals["windows"] += int(result["windows"])
                    totals["empty"] += 1 if bool(result["empty"]) else 0
                    totals["error"] += int(result["error"])
                    totals["permanent_error"] += int(result["permanent_error"])
                    totals["fully_checked"] += 1 if bool(result["fully_checked"]) else 0
                    writer_stats["queue_high_watermark"] = max(
                        writer_stats["queue_high_watermark"], write_queue.qsize()
                    )
                    if auto_tune_rps:
                        with runtime_status_lock:
                            snapshot = dict(runtime_status)
                        maybe_auto_tune_rps(
                            limiter=shared_rate_limiter,
                            runtime_status=snapshot,
                            tune_state=tune_state,
                            window_requests=auto_tune_window_requests,
                            threshold_429=auto_tune_429_threshold,
                            threshold_error=auto_tune_error_threshold,
                            min_rps=max(1, int(auto_tune_min_rps)),
                            max_rps=max(1, int(effective_max_rps)),
                        )
                    pbar.update(1)
                    markets_postfix = (
                        f"{len(completed_markets)}/{candidate_markets}"
                        if candidate_markets
                        else str(len(completed_markets))
                    )
                    pbar.set_postfix(
                        {
                            "rows": f"{totals['rows']:,}",
                            "empty": totals["empty"],
                            "err": totals["error"],
                            "closed_done": totals["fully_checked"],
                            "markets": markets_postfix,
                        },
                        refresh=True,
                    )
                    if progress_callback is not None:
                        progress_callback(
                            "token_completed",
                            {
                                "processed_tokens": totals["processed_tokens"],
                                "rows": totals["rows"],
                                "windows": totals["windows"],
                            },
                        )
    except runtime.no_progress_timeout_error as exc:
        aborted = True
        abort_reason = str(exc)
        no_progress_error = exc
        for future in list(futures.keys()):
            future.cancel()
        shutdown_fn = getattr(executor, "shutdown", None)
        if callable(shutdown_fn):
            shutdown_fn(wait=False, cancel_futures=True)
        executor_shutdown = True
    finally:
        if not executor_shutdown:
            shutdown_fn = getattr(executor, "shutdown", None)
            if callable(shutdown_fn):
                shutdown_fn(wait=True, cancel_futures=False)

    return PoolRunResult(
        planning_state=planning_state,
        invalid_tokens=invalid_tokens,
        totals=totals,
        aborted=aborted,
        abort_reason=abort_reason,
        no_progress_error=no_progress_error,
        executor_shutdown=executor_shutdown,
    )
