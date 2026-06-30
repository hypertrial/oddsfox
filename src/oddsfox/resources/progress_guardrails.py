from __future__ import annotations

import time
from threading import Lock
from typing import Any, Callable, Optional, Protocol


class LoggerLike(Protocol):
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...  # pragma: no cover

    def warning(
        self, msg: str, *args: Any, **kwargs: Any
    ) -> None: ...  # pragma: no cover

    def error(
        self, msg: str, *args: Any, **kwargs: Any
    ) -> None: ...  # pragma: no cover


class NoProgressTimeoutError(RuntimeError):
    """Raised when no-progress hard timeout is exceeded."""

    def __init__(
        self,
        message: str,
        *,
        details: dict[str, Any],
    ) -> None:
        super().__init__(message)
        self.details = details


class ProgressGuardrail:
    """Tracks progress heartbeat and enforces soft/hard no-progress thresholds."""

    def __init__(
        self,
        *,
        asset: str,
        logger: LoggerLike,
        progress_log_interval_seconds: int = 60,
        no_progress_soft_timeout_seconds: int | None = 900,
        no_progress_hard_timeout_seconds: int | None = 2700,
        work_log_interval: int | None = None,
        progress_callback: Optional[Callable[[str, dict[str, Any]], None]] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if progress_log_interval_seconds <= 0:
            raise ValueError("progress_log_interval_seconds must be positive")
        if work_log_interval is not None and work_log_interval <= 0:
            raise ValueError("work_log_interval must be positive when set")
        if (
            no_progress_soft_timeout_seconds is not None
            and no_progress_soft_timeout_seconds <= 0
        ):
            raise ValueError(
                "no_progress_soft_timeout_seconds must be positive when set"
            )
        if (
            no_progress_hard_timeout_seconds is not None
            and no_progress_hard_timeout_seconds <= 0
        ):
            raise ValueError(
                "no_progress_hard_timeout_seconds must be positive when set"
            )
        if (
            no_progress_soft_timeout_seconds is not None
            and no_progress_hard_timeout_seconds is not None
            and no_progress_hard_timeout_seconds <= no_progress_soft_timeout_seconds
        ):
            raise ValueError(
                "no_progress_hard_timeout_seconds must be greater than "
                "no_progress_soft_timeout_seconds when both are set"
            )

        now = clock()
        self._asset = asset
        self._logger = logger
        self._clock = clock
        self._progress_log_interval_seconds = int(progress_log_interval_seconds)
        self._soft_timeout_seconds = no_progress_soft_timeout_seconds
        self._hard_timeout_seconds = no_progress_hard_timeout_seconds
        self._work_log_interval = work_log_interval
        self._progress_callback = progress_callback

        self._started_at = now
        self._last_progress_at = now
        self._last_heartbeat_at = now
        self._last_logged_work = 0
        self._work_completed = 0
        self._soft_warning_count = 0
        self._max_idle_seconds = 0.0
        self._next_soft_warning_idle_seconds = (
            float(no_progress_soft_timeout_seconds)
            if no_progress_soft_timeout_seconds is not None
            else None
        )
        self._lock = Lock()

    def _elapsed(self, now: float) -> float:
        return max(0.0, now - self._started_at)

    def _idle(self, now: float) -> float:
        return max(0.0, now - self._last_progress_at)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            now = self._clock()
            elapsed = self._elapsed(now)
            idle = self._idle(now)
            self._max_idle_seconds = max(self._max_idle_seconds, idle)
            rate = (self._work_completed / elapsed) if elapsed > 0 else 0.0
            return {
                "asset": self._asset,
                "elapsed_seconds": round(elapsed, 3),
                "idle_seconds": round(idle, 3),
                "max_idle_seconds": round(self._max_idle_seconds, 3),
                "work_completed": int(self._work_completed),
                "work_rate_per_second": round(rate, 4),
                "soft_warning_count": int(self._soft_warning_count),
                "progress_log_interval_seconds": self._progress_log_interval_seconds,
                "no_progress_soft_timeout_seconds": self._soft_timeout_seconds,
                "no_progress_hard_timeout_seconds": self._hard_timeout_seconds,
            }

    def _emit(
        self,
        *,
        level: str,
        phase: str,
        reason: str,
        diagnostics: dict[str, Any] | None,
    ) -> None:
        payload = self.snapshot()
        payload["phase"] = phase
        payload["reason"] = reason
        if diagnostics:
            payload["diagnostics"] = diagnostics
        log_fn = getattr(self._logger, level)
        log_fn("progress_guardrail %s", payload)
        if self._progress_callback:
            try:
                self._progress_callback("guardrail", payload)
            except Exception:
                debug_fn = getattr(self._logger, "debug", None)
                if callable(debug_fn):
                    debug_fn("Ignoring progress callback failure", exc_info=True)

    def record_progress(  # pragma: no branch
        self,
        *,
        work_increment: int = 1,
        phase: str = "progress",
        diagnostics: dict[str, Any] | None = None,
        force_log: bool = False,
    ) -> None:
        with self._lock:
            now = self._clock()
            prev_idle = self._idle(now)
            self._max_idle_seconds = max(self._max_idle_seconds, prev_idle)
            self._last_progress_at = now
            self._work_completed += max(0, int(work_increment))
            self._next_soft_warning_idle_seconds = (
                float(self._soft_timeout_seconds)
                if self._soft_timeout_seconds is not None
                else None
            )

            should_log = force_log
            if (
                not should_log
                and self._work_log_interval is not None
                and (self._work_completed - self._last_logged_work)
                >= self._work_log_interval
            ):
                should_log = True
            if (
                not should_log
                and (now - self._last_heartbeat_at)
                >= self._progress_log_interval_seconds
            ):
                should_log = True

            if should_log:
                self._last_heartbeat_at = now
                self._last_logged_work = self._work_completed

        if should_log:
            self._emit(
                level="info",
                phase=phase,
                reason="progress",
                diagnostics=diagnostics,
            )

    def check(
        self,
        *,
        phase: str = "heartbeat",
        diagnostics: dict[str, Any] | None = None,
    ) -> None:
        should_heartbeat = False
        should_soft_warn = False
        should_hard_fail = False
        hard_fail_details: dict[str, Any] | None = None
        with self._lock:
            now = self._clock()
            idle = self._idle(now)
            self._max_idle_seconds = max(self._max_idle_seconds, idle)
            elapsed = self._elapsed(now)
            rate = (self._work_completed / elapsed) if elapsed > 0 else 0.0

            if (
                self._next_soft_warning_idle_seconds is not None
                and idle >= self._next_soft_warning_idle_seconds
            ):
                should_soft_warn = True
                self._soft_warning_count += 1
                self._next_soft_warning_idle_seconds += float(
                    self._soft_timeout_seconds
                )

            if self._hard_timeout_seconds is not None and idle >= float(
                self._hard_timeout_seconds
            ):
                should_hard_fail = True
                self._last_heartbeat_at = now
                hard_fail_details = {
                    "asset": self._asset,
                    "elapsed_seconds": round(elapsed, 3),
                    "idle_seconds": round(idle, 3),
                    "max_idle_seconds": round(self._max_idle_seconds, 3),
                    "work_completed": int(self._work_completed),
                    "work_rate_per_second": round(rate, 4),
                    "soft_warning_count": int(self._soft_warning_count),
                    "progress_log_interval_seconds": self._progress_log_interval_seconds,
                    "no_progress_soft_timeout_seconds": self._soft_timeout_seconds,
                    "no_progress_hard_timeout_seconds": self._hard_timeout_seconds,
                    "phase": phase,
                    "diagnostics": diagnostics or {},
                }

            if (now - self._last_heartbeat_at) >= self._progress_log_interval_seconds:
                should_heartbeat = True
                self._last_heartbeat_at = now

        if should_hard_fail:
            self._emit(
                level="error",
                phase=phase,
                reason="no_progress_hard_timeout",
                diagnostics=diagnostics,
            )
            assert hard_fail_details is not None
            raise NoProgressTimeoutError(
                (
                    f"No progress timeout exceeded for asset={self._asset} "
                    f"(idle_seconds={hard_fail_details['idle_seconds']}, "
                    f"hard_timeout_seconds={self._hard_timeout_seconds})"
                ),
                details=hard_fail_details,
            )
        if should_soft_warn:
            self._emit(
                level="warning",
                phase=phase,
                reason="no_progress_soft_timeout",
                diagnostics=diagnostics,
            )
        elif should_heartbeat:
            self._emit(
                level="info",
                phase=phase,
                reason="heartbeat",
                diagnostics=diagnostics,
            )


__all__ = ["NoProgressTimeoutError", "ProgressGuardrail"]
