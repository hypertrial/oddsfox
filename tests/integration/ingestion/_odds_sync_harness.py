"""Shared fakes and patch helpers for Polymarket odds sync unit tests."""

from __future__ import annotations

from concurrent.futures import Future
from contextlib import contextmanager
from typing import Any, Callable, Iterator
from unittest.mock import patch

from oddsfox.ingestion.polymarket.odds import sync as odds_sync
from oddsfox.ingestion.polymarket.odds.deps import replace_odds_sync_runtime
from oddsfox.resources.progress_guardrails import ProgressGuardrail

_RAW_SNAPSHOT_DEFAULTS: dict[str, Any] = {
    "market_tokens_distinct_tokens": 0,
    "odds_history_distinct_tokens": 0,
    "token_odds_daily_distinct_tokens": 0,
    "ledger_distinct_tokens": 0,
    "ledger_fully_checked_tokens": 0,
    "token_sync_skips_distinct_tokens": 0,
    "market_tokens_without_history": 0,
    "history_tokens_without_market_tokens": 0,
    "token_sync_skips_by_reason": {},
}


class ImmediateFuture(Future):
    def __init__(self, fn, *args, **kwargs):
        super().__init__()
        self.set_result(fn(*args, **kwargs))


class DoneFuture(Future):
    """Like ImmediateFuture but catches exceptions into the Future (gap-closure tests)."""

    def __init__(self, fn, *args, **kwargs):
        super().__init__()
        try:
            self.set_result(fn(*args, **kwargs))
        except Exception as exc:  # pragma: no cover - Future surface only
            self.set_exception(exc)


class ImmediatePool:
    def __init__(self, *args, **kwargs):
        del args, kwargs

    def __enter__(self):
        return self

    def __exit__(self, *args):
        del args
        return False

    def submit(self, fn, *args, **kwargs):
        return DoneFuture(fn, *args, **kwargs)

    def shutdown(self, wait=True, cancel_futures=False):
        del wait, cancel_futures
        return None


DonePool = ImmediatePool


class ImmediatePoolNoShutdown(ImmediatePool):
    """Completes work synchronously but lacks a shutdown hook (pool finally branch)."""

    shutdown = None  # type: ignore[assignment]


class NeverDoneFuture(Future):
    def __init__(self):
        super().__init__()


class NeverDonePool:
    def __init__(self, *args, **kwargs):
        del args, kwargs
        self._future = NeverDoneFuture()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        del args
        return False

    def submit(self, fn, *args, **kwargs):
        del fn, args, kwargs
        return self._future

    def shutdown(self, wait=True, cancel_futures=False):
        del wait, cancel_futures
        return None


class NoShutdownNeverDonePool:
    """Never completes; no context manager or shutdown (timeout edge-case tests)."""

    def __init__(self, *args, **kwargs):
        del args, kwargs
        self._future = NeverDoneFuture()

    def submit(self, fn, *args, **kwargs):
        del fn, args, kwargs
        return self._future


class NoThread:
    def __init__(self, *args, **kwargs):
        del args, kwargs

    def start(self):
        return None

    def join(self, timeout=None):
        del timeout
        return None


class ImmediateThread:
    def __init__(self, target=None, args=(), daemon=None, **kwargs):
        del daemon, kwargs
        self.target = target
        self.args = args

    def start(self):
        if self.target is not None:
            self.target(*self.args)

    def join(self, timeout=None):
        del timeout
        return None


class FakePbar:
    last_instance: FakePbar | None = None

    def __init__(self, *args, **kwargs):
        del args
        self.total = kwargs.get("total")
        self.updated = 0
        self.postfix = None
        FakePbar.last_instance = self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        del args
        return False

    def update(self, amount):
        self.updated += amount

    def set_postfix(self, payload, refresh=True):
        del refresh
        self.postfix = payload


class FakeClock:
    def __init__(self, start: float = 0.0):
        self.now = float(start)

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> float:
        self.now += float(seconds)
        return self.now


def raw_snapshot(**overrides: Any) -> dict[str, Any]:
    out = dict(_RAW_SNAPSHOT_DEFAULTS)
    out.update(overrides)
    return out


def valid_token_id(prefix: str = "1") -> str:
    return prefix * 33 + "12"


def make_token_plan(
    token_id: str | None = None,
    *,
    closed: bool = False,
    short_window: bool = False,
) -> odds_sync.TokenPlan:
    tid = token_id or valid_token_id()
    if short_window:
        return odds_sync.TokenPlan(
            token_id=tid,
            market_id="m",
            is_closed=closed,
            created_at_ts=1_700_000_000,
            start_ts=10,
            end_ts=31,
            fidelity=1440,
        )
    return odds_sync.TokenPlan(
        token_id=tid,
        market_id="m",
        is_closed=closed,
        created_at_ts=1_600_000_000,
        start_ts=1_600_000_100,
        end_ts=1_700_000_000,
        fidelity=1440,
    )


def noop_wait(futures, timeout=None, return_when=None):
    del timeout, return_when
    return set(futures), set()


def patch_queue_join(monkeypatch) -> None:
    monkeypatch.setattr(odds_sync.Queue, "join", lambda self: None)


def patch_guardrail_clock(monkeypatch, clock: FakeClock) -> None:
    def _factory(*args, **kwargs):
        kwargs.setdefault("clock", clock)
        return ProgressGuardrail(*args, **kwargs)

    monkeypatch.setattr(odds_sync, "ProgressGuardrail", _factory)


def make_runtime(**overrides: Any):
    return replace_odds_sync_runtime(odds_sync.default_odds_sync_runtime(), **overrides)


def patch_sync_odds_idle(
    monkeypatch,
    *,
    writer_loop: Callable[..., Any] | None = None,
    thread_cls: type = NoThread,
    snapshot: dict[str, Any] | None = None,
) -> None:
    monkeypatch.setattr(odds_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        odds_sync,
        "snapshot_raw_layer",
        lambda: raw_snapshot() if snapshot is None else snapshot,
    )
    monkeypatch.setattr(
        odds_sync,
        "save_sync_run_metrics",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(odds_sync, "Thread", thread_cls)
    if writer_loop is not None:
        monkeypatch.setattr(odds_sync, "_writer_loop", writer_loop)
    else:
        monkeypatch.setattr(odds_sync, "_writer_loop", lambda *args, **kwargs: None)


def patch_sync_odds_executor(monkeypatch, pool_cls: type = ImmediatePool) -> None:
    monkeypatch.setattr(odds_sync, "ThreadPoolExecutor", pool_cls)


def make_plan(
    token_id: str,
    *,
    closed: bool = False,
) -> odds_sync.TokenPlan:
    """Short-window token plan helper."""
    return make_token_plan(token_id, closed=closed, short_window=True)


def patch_sync_odds_standard_idle(
    monkeypatch,
    *,
    plan_iter=None,
    sync_plan_fn=None,
    saved_skips: list | None = None,
    thread_cls=None,
    writer_loop=None,
    budgets: dict | None = None,
) -> None:
    """Standard idle patches for sync_odds integration tests."""
    patch_sync_odds_idle(
        monkeypatch,
        writer_loop=writer_loop if writer_loop is not None else (lambda *a, **k: None),
        thread_cls=thread_cls or NoThread,
    )
    if plan_iter is not None:
        monkeypatch.setattr(odds_sync, "iter_token_plans_paged", plan_iter)
    if sync_plan_fn is not None:
        monkeypatch.setattr(odds_sync, "_sync_token_plan", sync_plan_fn)
    if saved_skips is not None:
        monkeypatch.setattr(
            odds_sync, "save_skipped_tokens", lambda rows: saved_skips.extend(rows)
        )
    else:
        monkeypatch.setattr(odds_sync, "save_skipped_tokens", lambda *a, **k: None)
    patch_sync_odds_executor(monkeypatch, ImmediatePool)
    monkeypatch.setattr(
        odds_sync, "wait", lambda futures, return_when=None: (set(futures), set())
    )
    monkeypatch.setattr(odds_sync, "tqdm", FakePbar)
    patch_queue_join(monkeypatch)
    if budgets is not None:
        pass  # caller passes budgets dict to sync_odds directly


@contextmanager
def immediate_executor() -> Iterator[type[ImmediatePool]]:
    with patch.object(odds_sync, "ThreadPoolExecutor", ImmediatePool):
        yield ImmediatePool
