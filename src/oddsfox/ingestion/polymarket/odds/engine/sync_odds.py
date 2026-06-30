from __future__ import annotations

from typing import Any, Callable, Dict

from oddsfox.config.settings import (
    DEFAULT_ODDS_FIDELITY_MINUTES,
    ODDS_REQUESTS_PER_SECOND,
)
from oddsfox.ingestion.polymarket.odds.deps import OddsSyncRuntime
from oddsfox.ingestion.polymarket.odds.planning import iter_token_plans_paged
from oddsfox.ingestion.polymarket.odds.support import (
    DEFAULT_AUTOTUNE_429_THRESHOLD,
    DEFAULT_AUTOTUNE_ERROR_THRESHOLD,
    DEFAULT_AUTOTUNE_WINDOW_REQUESTS,
    DEFAULT_EMPTY_RETRY_BASE_HOURS,
    DEFAULT_EMPTY_RETRY_MAX_HOURS,
    DEFAULT_EMPTY_TOKEN_SKIP_RUNS,
    DEFAULT_ERROR_RETRY_MINUTES,
    DEFAULT_MARKET_PAGE_SIZE,
    DEFAULT_MIN_SPLIT_WINDOW_MINUTES,
    DEFAULT_OVERLAP_MINUTES,
    DEFAULT_ROUTINE_INTERVAL_HOURS,
    DEFAULT_SKIP_RECENT_MINUTES,
    DEFAULT_TRANSIENT_BACKOFF_SECONDS,
    DEFAULT_TRANSIENT_RETRIES,
    DEFAULT_WINDOW_HOURS,
    DEFAULT_WRITER_CHUNK_ROWS,
    DEFAULT_WRITER_FLUSH_ROWS,
    planning_state_to_dict,
)

from .bootstrap import (
    bootstrap_planning,
    build_noop_sync_result,
    normalize_sync_params,
    setup_guardrail,
)
from .finalize import finalize_sync_odds_run
from .pool import PoolResources, run_sync_pool, setup_rate_limiting, setup_writer


def sync_odds(
    *,
    max_workers: int = 10,
    batch_size: int = 50_000,
    fidelity: int = DEFAULT_ODDS_FIDELITY_MINUTES,
    requests_per_second: int | None = None,
    auto_tune_rps: bool = True,
    auto_tune_window_requests: int = DEFAULT_AUTOTUNE_WINDOW_REQUESTS,
    auto_tune_429_threshold: float = DEFAULT_AUTOTUNE_429_THRESHOLD,
    auto_tune_error_threshold: float = DEFAULT_AUTOTUNE_ERROR_THRESHOLD,
    auto_tune_min_rps: int = 1,
    auto_tune_max_rps: int | None = None,
    force: bool = False,
    clob_cutoff_date: str = "2023-01-01",
    skip_recent_minutes: int = DEFAULT_SKIP_RECENT_MINUTES,
    overlap_minutes: int = DEFAULT_OVERLAP_MINUTES,
    window_hours: int = DEFAULT_WINDOW_HOURS,
    rebuild_minutely: bool = False,
    reconcile_ledger: bool = False,
    short_range_first: bool = True,
    market_scope: str = "all",
    ended_market_grace_days: int | None = None,
    min_volume: float | None = None,
    max_volume: float | None = None,
    minutely_backfill_days: int = 0,
    empty_token_skip_runs: int = DEFAULT_EMPTY_TOKEN_SKIP_RUNS,
    empty_token_skip_budgets: Dict[str, int] | None = None,
    routine_interval_hours: int = DEFAULT_ROUTINE_INTERVAL_HOURS,
    empty_retry_base_hours: int = DEFAULT_EMPTY_RETRY_BASE_HOURS,
    empty_retry_max_hours: int = DEFAULT_EMPTY_RETRY_MAX_HOURS,
    error_retry_minutes: int = DEFAULT_ERROR_RETRY_MINUTES,
    transient_retries: int = DEFAULT_TRANSIENT_RETRIES,
    transient_backoff_seconds: float = DEFAULT_TRANSIENT_BACKOFF_SECONDS,
    market_page_size: int = DEFAULT_MARKET_PAGE_SIZE,
    rate_limiter_factory: Callable[[int | None], Any] | None = None,
    client_factory: Callable[[], Any] | None = None,
    plan_iterator_factory: Callable[..., Any] = iter_token_plans_paged,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
    progress_log_interval_tokens: int = 100,
    progress_log_interval_seconds: int = 60,
    no_progress_soft_timeout_seconds: int | None = 900,
    no_progress_hard_timeout_seconds: int | None = 2700,
    progress_poll_seconds: int = 5,
    writer_chunk_rows: int = DEFAULT_WRITER_CHUNK_ROWS,
    writer_flush_rows: int = DEFAULT_WRITER_FLUSH_ROWS,
    min_split_window_minutes: int = DEFAULT_MIN_SPLIT_WINDOW_MINUTES,
    persist_run_metrics: bool = True,
    runtime: OddsSyncRuntime,
) -> Dict[str, Any]:
    run_started = runtime.time_mod.monotonic()
    params = normalize_sync_params(
        batch_size=batch_size,
        window_hours=window_hours,
        writer_chunk_rows=writer_chunk_rows,
        writer_flush_rows=writer_flush_rows,
        min_split_window_minutes=min_split_window_minutes,
        routine_interval_hours=routine_interval_hours,
        empty_retry_base_hours=empty_retry_base_hours,
        empty_retry_max_hours=empty_retry_max_hours,
        error_retry_minutes=error_retry_minutes,
    )
    guardrail = setup_guardrail(
        runtime,
        progress_log_interval_seconds=progress_log_interval_seconds,
        no_progress_soft_timeout_seconds=no_progress_soft_timeout_seconds,
        no_progress_hard_timeout_seconds=no_progress_hard_timeout_seconds,
        progress_log_interval_tokens=progress_log_interval_tokens,
        progress_callback=progress_callback,
    )
    boot = bootstrap_planning(
        runtime,
        clob_cutoff_date=clob_cutoff_date,
        fidelity=fidelity,
        force=force,
        rebuild_minutely=rebuild_minutely,
        overlap_minutes=overlap_minutes,
        skip_recent_minutes=skip_recent_minutes,
        market_page_size=market_page_size,
        reconcile_ledger=reconcile_ledger,
        short_range_first=short_range_first,
        market_scope=market_scope,
        ended_market_grace_days=ended_market_grace_days,
        min_volume=min_volume,
        max_volume=max_volume,
        minutely_backfill_days=minutely_backfill_days,
        empty_token_skip_budgets=empty_token_skip_budgets,
        empty_token_skip_runs=empty_token_skip_runs,
        plan_iterator_factory=plan_iterator_factory,
    )
    if boot.first_plan is None:
        return build_noop_sync_result(
            runtime=runtime,
            guardrail=guardrail,
            run_started=run_started,
            raw_pre=boot.raw_pre,
            planning_state=boot.planning_state,
            invalid_tokens=boot.invalid_tokens,
            persist_invalid_tokens_batch=boot.persist_invalid_tokens_batch,
        )

    (
        effective_workers,
        shared_rate_limiter,
        runtime_status,
        runtime_status_lock,
        tune_state,
        get_worker_client,
        on_http_status,
    ) = setup_rate_limiting(
        max_workers=max_workers,
        requests_per_second=requests_per_second,
        rate_limiter_factory=rate_limiter_factory,
        client_factory=client_factory,
        runtime=runtime,
    )
    configured_rps = (
        requests_per_second
        if requests_per_second is not None
        else (ODDS_REQUESTS_PER_SECOND or max_workers)
    )
    configured_rps = max(1, int(configured_rps)) if configured_rps else None
    effective_max_rps = (
        max(2, effective_workers, configured_rps or 0)
        if auto_tune_max_rps is None
        else max(int(auto_tune_max_rps), max(1, configured_rps or 1))
    )
    write_queue, writer_stats, writer_failures, writer_thread = setup_writer(
        runtime,
        effective_workers=effective_workers,
        writer_flush_rows=params.writer_flush_rows,
    )
    pool = PoolResources(
        effective_workers=effective_workers,
        shared_rate_limiter=shared_rate_limiter,
        runtime_status=runtime_status,
        runtime_status_lock=runtime_status_lock,
        tune_state=tune_state,
        get_worker_client=get_worker_client,
        write_queue=write_queue,
        writer_stats=writer_stats,
        writer_failures=writer_failures,
        writer_thread=writer_thread,
        totals={
            "processed_tokens": 0,
            "rows": 0,
            "windows": 0,
            "empty": 0,
            "error": 0,
            "permanent_error": 0,
            "fully_checked": 0,
            "distinct_markets": 0,
        },
    )
    pool_result = run_sync_pool(
        runtime,
        boot,
        params,
        guardrail,
        pool,
        first_plan=boot.first_plan,
        effective_max_rps=effective_max_rps,
        auto_tune_rps=auto_tune_rps,
        auto_tune_window_requests=auto_tune_window_requests,
        auto_tune_429_threshold=auto_tune_429_threshold,
        auto_tune_error_threshold=auto_tune_error_threshold,
        auto_tune_min_rps=auto_tune_min_rps,
        transient_retries=transient_retries,
        transient_backoff_seconds=transient_backoff_seconds,
        progress_callback=progress_callback,
        progress_poll_seconds=progress_poll_seconds,
        on_http_status=on_http_status,
    )
    return finalize_sync_odds_run(
        runtime=runtime,
        run_started=run_started,
        guardrail=guardrail,
        raw_pre=boot.raw_pre,
        planning_state=pool_result.planning_state,
        invalid_tokens=pool_result.invalid_tokens,
        persist_invalid_tokens_batch=boot.persist_invalid_tokens_batch,
        totals=pool_result.totals,
        writer_stats=writer_stats,
        runtime_status_lock=runtime_status_lock,
        runtime_status=runtime_status,
        shared_rate_limiter=shared_rate_limiter,
        aborted=pool_result.aborted,
        abort_reason=pool_result.abort_reason,
        no_progress_error=pool_result.no_progress_error,
        write_queue=write_queue,
        writer_thread=writer_thread,
        writer_failures=writer_failures,
        progress_poll_seconds=progress_poll_seconds,
        persist_run_metrics=persist_run_metrics,
        planning_state_to_dict=planning_state_to_dict,
    )
