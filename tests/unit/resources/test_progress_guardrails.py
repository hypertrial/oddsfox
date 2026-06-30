from __future__ import annotations

import pytest

from oddsfox.resources.progress_guardrails import (
    NoProgressTimeoutError,
    ProgressGuardrail,
)


class _Logger:
    def __init__(self):
        self.records: list[tuple[str, str, dict]] = []
        self.debug_called = 0

    def info(self, msg: str, payload: dict):
        self.records.append(("info", msg, payload))

    def warning(self, msg: str, payload: dict):
        self.records.append(("warning", msg, payload))

    def error(self, msg: str, payload: dict):
        self.records.append(("error", msg, payload))

    def debug(self, msg: str, **kwargs):
        del msg, kwargs
        self.debug_called += 1


class _LoggerNoDebug:
    def __init__(self):
        self.records: list[tuple[str, str, dict]] = []

    def info(self, msg: str, payload: dict):
        self.records.append(("info", msg, payload))

    def warning(self, msg: str, payload: dict):
        self.records.append(("warning", msg, payload))

    def error(self, msg: str, payload: dict):
        self.records.append(("error", msg, payload))


class _Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_progress_guardrail_validation_errors():
    logger = _Logger()
    with pytest.raises(ValueError, match="progress_log_interval_seconds"):
        ProgressGuardrail(asset="a", logger=logger, progress_log_interval_seconds=0)
    with pytest.raises(ValueError, match="work_log_interval"):
        ProgressGuardrail(asset="a", logger=logger, work_log_interval=0)
    with pytest.raises(ValueError, match="soft_timeout"):
        ProgressGuardrail(asset="a", logger=logger, no_progress_soft_timeout_seconds=0)
    with pytest.raises(ValueError, match="hard_timeout"):
        ProgressGuardrail(asset="a", logger=logger, no_progress_hard_timeout_seconds=0)
    with pytest.raises(ValueError, match="greater than"):
        ProgressGuardrail(
            asset="a",
            logger=logger,
            no_progress_soft_timeout_seconds=10,
            no_progress_hard_timeout_seconds=10,
        )


def test_progress_guardrail_callback_failure_is_ignored_with_debug_log():
    logger = _Logger()

    def bad_callback(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("callback broke")

    guardrail = ProgressGuardrail(
        asset="asset",
        logger=logger,
        progress_callback=bad_callback,
    )
    guardrail.record_progress(work_increment=0, phase="forced", force_log=True)

    assert logger.records
    assert logger.records[-1][0] == "info"
    assert logger.debug_called == 1


def test_progress_guardrail_callback_failure_without_debug_logger():
    logger = _LoggerNoDebug()

    def bad_callback(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("callback broke")

    guardrail = ProgressGuardrail(
        asset="asset",
        logger=logger,
        progress_callback=bad_callback,
    )
    guardrail.record_progress(work_increment=0, phase="forced", force_log=True)
    assert logger.records


def test_progress_guardrail_time_heartbeat_soft_and_hard_paths():
    logger = _Logger()
    clock = _Clock()
    guardrail = ProgressGuardrail(
        asset="asset",
        logger=logger,
        progress_log_interval_seconds=1,
        no_progress_soft_timeout_seconds=2,
        no_progress_hard_timeout_seconds=5,
        work_log_interval=100,
        clock=clock,
    )

    clock.advance(1.1)
    guardrail.record_progress(work_increment=0, phase="tick")

    clock.advance(1.1)
    guardrail.check(phase="heartbeat")

    clock.advance(1.0)
    guardrail.check(phase="soft")

    clock.advance(3.0)
    with pytest.raises(NoProgressTimeoutError) as excinfo:
        guardrail.check(phase="hard", diagnostics={"probe": 1})

    assert excinfo.value.details["phase"] == "hard"
    assert excinfo.value.details["diagnostics"]["probe"] == 1
    reasons = [payload["reason"] for _, _, payload in logger.records]
    assert "progress" in reasons
    assert "heartbeat" in reasons
    assert "no_progress_soft_timeout" in reasons
    assert "no_progress_hard_timeout" in reasons


def test_progress_guardrail_record_progress_can_skip_log_emit():
    logger = _Logger()
    clock = _Clock()
    guardrail = ProgressGuardrail(
        asset="asset",
        logger=logger,
        progress_log_interval_seconds=60,
        no_progress_soft_timeout_seconds=None,
        no_progress_hard_timeout_seconds=None,
        work_log_interval=100,
        clock=clock,
    )
    guardrail.record_progress(work_increment=1, phase="quiet")
    assert logger.records == []
