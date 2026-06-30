from __future__ import annotations

import contextlib
from queue import Empty, Queue
from threading import Thread
from typing import Any

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource

from oddsfox.orchestration.config import DbtBuildConfig
from oddsfox.resources.progress_guardrails import (
    NoProgressTimeoutError,
    ProgressGuardrail,
)


def stream_dbt_build(
    *,
    asset_name: str,
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    config: DbtBuildConfig,
    heartbeat_diagnostics_fn=None,
):
    guardrail = ProgressGuardrail(
        asset=asset_name,
        logger=context.log,
        progress_log_interval_seconds=config.progress_log_interval_seconds,
        no_progress_soft_timeout_seconds=config.no_progress_soft_timeout_seconds,
        no_progress_hard_timeout_seconds=config.no_progress_hard_timeout_seconds,
        work_log_interval=config.progress_log_interval_events,
    )
    guardrail.record_progress(
        work_increment=0,
        phase="start",
        diagnostics={},
        force_log=True,
    )

    build_args = ["build"]
    if config.full_refresh:
        build_args.append("--full-refresh")
    invocation = dbt.cli(build_args, context=context)
    sentinel = object()
    event_queue: Queue[Any] = Queue()
    producer_error: list[Exception] = []

    def _producer() -> None:
        try:
            for event in invocation.stream():
                event_queue.put(event)
        except Exception as exc:  # pragma: no cover
            producer_error.append(exc)
        finally:
            event_queue.put(sentinel)

    producer = Thread(target=_producer, daemon=True)
    producer.start()

    events_emitted = 0
    while True:
        try:
            item = event_queue.get(timeout=max(1, config.progress_poll_seconds))
        except Empty:
            diagnostics = {
                "events_emitted": events_emitted,
                "queue_size": event_queue.qsize(),
                "dbt_return_code": getattr(invocation.process, "returncode", None),
            }
            if callable(heartbeat_diagnostics_fn):
                extra = heartbeat_diagnostics_fn()
                if isinstance(extra, dict):
                    diagnostics.update(extra)
            try:
                guardrail.check(
                    phase="dbt_build_stream_wait",
                    diagnostics=diagnostics,
                )
            except NoProgressTimeoutError:
                context.log.error(
                    "%s dbt build no-progress hard timeout; terminating dbt process",
                    asset_name,
                )
                with contextlib.suppress(Exception):
                    invocation.process.terminate()
                raise
            continue

        if item is sentinel:
            break

        events_emitted += 1
        guardrail.record_progress(
            work_increment=1,
            phase="dbt_build_event",
            diagnostics={"events_emitted": events_emitted},
        )
        yield item

    producer.join(timeout=max(1, config.progress_poll_seconds) * 2)
    if producer_error:
        raise producer_error[0]

    guardrail.record_progress(
        work_increment=0,
        phase="dbt_build_complete",
        diagnostics={"events_emitted": events_emitted},
        force_log=True,
    )


__all__ = ["stream_dbt_build"]
