"""Complete branch coverage for odds fetch.py and process.py."""

from __future__ import annotations

from threading import Lock
from unittest.mock import MagicMock, patch

import pytest
import requests

from oddsfox.ingestion.polymarket.odds import fetch as odds_fetch
from oddsfox.ingestion.polymarket.odds import process as odds_process


def test_emit_status_hook_failure():
    odds_fetch.set_status_hook(lambda s: (_ for _ in ()).throw(RuntimeError("hook")))
    odds_fetch._emit_status(200)
    odds_fetch.set_status_hook(None)


def test_emit_status_via_explicit_hook_failure():
    odds_fetch._emit_status_via(lambda s: (_ for _ in ()).throw(ValueError("x")), 500)


def test_fetch_token_history_points_missing_t_or_p():
    c = MagicMock()
    c.get.return_value = {"history": [{"t": 1}, {"p": 0.5}]}
    out = odds_fetch.fetch_token_history(c, "t" * 40, interval="1d")
    assert out == []


def test_fetch_token_history_http_generic_5xx_logs():
    c = MagicMock()
    err = requests.HTTPError()
    err.response = MagicMock(status_code=503, text="x")
    c.get.side_effect = err
    assert odds_fetch.fetch_token_history(c, "t" * 40) is None


def test_fetch_token_history_exception_branch_429_via_response():
    c = MagicMock()
    err = Exception("x")
    err.response = MagicMock(status_code=429, text="")
    c.get.side_effect = err
    assert odds_fetch.fetch_token_history(c, "t" * 40) is None


def test_fetch_token_history_exception_400_raises():
    c = MagicMock()
    err = ValueError("x")
    err.response = MagicMock(status_code=400, text="bad")
    c.get.side_effect = err
    with pytest.raises(odds_fetch.BadRequestError):
        odds_fetch.fetch_token_history(c, "t" * 40)


def test_fetch_token_history_exception_404_permanent():
    c = MagicMock()
    err = RuntimeError("x")
    err.response = MagicMock(status_code=404, text="n")
    c.get.side_effect = err
    with pytest.raises(odds_fetch.PermanentAPIError):
        odds_fetch.fetch_token_history(c, "t" * 40)


def test_fetch_token_history_fidelity_param():
    c = MagicMock()
    c.get.return_value = {"history": []}
    odds_fetch.fetch_token_history(c, "t" * 40, interval="1d", fidelity=5)
    assert "fidelity" in c.get.call_args[1]["params"]


def test_fetch_with_retry_inner_loop_break_without_sleep():
    c = MagicMock()
    c.get.return_value = {"history": []}
    with patch(
        "oddsfox.ingestion.polymarket.odds.fetch.fetch_token_history",
        return_value=None,
    ) as ft:
        odds_fetch.fetch_token_history_with_retry(
            c,
            "t" * 40,
            interval="1d",
            transient_retries=1,
            transient_backoff_base_seconds=0.0,
        )
    assert ft.call_count >= 1


def test_fetch_with_retry_retries_exhaust_to_none():
    c = MagicMock()
    with patch("oddsfox.ingestion.polymarket.odds.fetch.time.sleep") as sl:
        with patch(
            "oddsfox.ingestion.polymarket.odds.fetch.random.uniform",
            return_value=1.0,
        ):
            with patch(
                "oddsfox.ingestion.polymarket.odds.fetch.fetch_token_history",
                return_value=None,
            ) as ft:
                out = odds_fetch.fetch_token_history_with_retry(
                    c,
                    "t" * 40,
                    interval="1d",
                    transient_retries=1,
                    transient_backoff_base_seconds=0.5,
                )
    assert out is None
    assert ft.call_count == 2
    sl.assert_called_once()


def _http_err(status: int, text: str = ""):
    err = requests.HTTPError()
    err.response = MagicMock(status_code=status, text=text)
    return err


def test_fetch_with_retry_range_adjusted_end_le_start():
    c = MagicMock()
    c.get.side_effect = _http_err(400, "interval is too long")
    with pytest.raises(odds_fetch.BadRequestError):
        odds_fetch.fetch_token_history_with_retry(
            c, "t" * 40, start_ts=100, end_ts=200, now_ts=50, transient_retries=0
        )


def test_fetch_with_retry_range_retry_chain():
    c = MagicMock()
    calls = {"n": 0}

    def side(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise _http_err(400, "other")
        return {"history": [{"t": 1, "p": 0.1}]}

    c.get.side_effect = side
    out = odds_fetch.fetch_token_history_with_retry(
        c, "t" * 40, start_ts=10, end_ts=1000, now_ts=500, transient_retries=0
    )
    assert out


def test_fetch_with_retry_interval_then_max():
    """Interval path: first fetch fails 400 (not interval-long), second succeeds without fidelity."""
    c = MagicMock()
    calls = {"n": 0}

    def ft_mock(*a, **kw):
        calls["n"] += 1
        if kw.get("interval") == "1d" and kw.get("fidelity") is not None:
            raise odds_fetch.BadRequestError("b", body="other", status=400)
        if kw.get("interval") == "1d" and kw.get("fidelity") is None:
            return {"history": [{"t": 1, "p": 0.2}]}
        return {"history": []}

    with patch.object(odds_fetch, "fetch_token_history", side_effect=ft_mock):
        out = odds_fetch.fetch_token_history_with_retry(
            c, "t" * 40, interval="1d", fidelity=5, transient_retries=0
        )
    assert out and calls["n"] >= 2


def test_fetch_with_retry_interval_max_raises_chained():
    c = MagicMock()

    def ft_mock(*a, **kw):
        raise odds_fetch.BadRequestError("b", body="x", status=400)

    with patch.object(odds_fetch, "fetch_token_history", side_effect=ft_mock):
        with pytest.raises(odds_fetch.BadRequestError):
            odds_fetch.fetch_token_history_with_retry(
                c, "t" * 40, interval="max", transient_retries=0
            )


def test_fetch_with_retry_interval_too_long_propagates():
    c = MagicMock()
    with patch.object(
        odds_fetch,
        "fetch_token_history",
        side_effect=odds_fetch.BadRequestError(
            "b", body="interval is too long for this token", status=400
        ),
    ):
        with pytest.raises(odds_fetch.BadRequestError):
            odds_fetch.fetch_token_history_with_retry(
                c, "t" * 40, start_ts=1, end_ts=100, now_ts=200, transient_retries=0
            )


def test_is_interval_too_long_helper():
    assert odds_fetch._is_interval_too_long(
        odds_fetch.BadRequestError("x", body="INTERVAL IS TOO LONG")
    )


TOKEN = "t" * 33 + "12"


def _stats():
    return {
        "success": 0,
        "empty": 0,
        "error": 0,
        "skipped": 0,
        "fully_checked": 0,
        "permanent_error": 0,
    }


def test_process_token_skip_registry_with_lock():
    lock = Lock()
    reg = {}
    client = MagicMock()
    client.get.return_value = {"history": []}

    odds_process.process_token(
        TOKEN,
        {TOKEN: 1},
        set(),
        client,
        lock,
        _stats(),
        skip_recent_hours=0,
        force=True,
        skip_registry=reg,
        skip_lock=Lock(),
    )


def test_process_token_start_ge_now():
    lock = Lock()
    client = MagicMock()
    future = 2_000_000_000
    odds_process.process_token(
        TOKEN,
        {TOKEN: future},
        set(),
        client,
        lock,
        _stats(),
        skip_recent_hours=0,
        force=True,
        max_range_seconds=3600,
    )


def test_process_token_multi_window_and_none_chunk():
    lock = Lock()
    client = MagicMock()
    client.get.return_value = {"history": [{"t": 100, "p": 0.5}]}

    with patch(
        "oddsfox.ingestion.polymarket.odds.process.fetch_token_history_with_retry",
        side_effect=[None],
    ):
        out = odds_process.process_token(
            TOKEN,
            {TOKEN: 1},
            set(),
            client,
            lock,
            _stats(),
            skip_recent_hours=0,
            force=True,
            max_range_seconds=100,
        )
    assert out == (None, None)


def test_process_token_post_fetch_exception_counts_as_error():
    client = MagicMock()
    client.get.return_value = {"history": [{"t": 200, "p": 0.5}]}
    lock = Lock()
    stats = _stats()
    with patch("builtins.max", side_effect=RuntimeError("outer")):
        out = odds_process.process_token(
            TOKEN,
            {TOKEN: 100},
            set(),
            client,
            lock,
            stats,
            skip_recent_hours=0,
            force=True,
        )
    assert out == (None, None)
    assert stats["error"] == 1


def test_process_token_bad_request_skip_with_lock():
    lock = Lock()
    reg = {}
    client = MagicMock()
    client.get.side_effect = _http_err(400, "bad")
    odds_process.process_token(
        TOKEN,
        {TOKEN: 1},
        set(),
        client,
        lock,
        _stats(),
        skip_recent_hours=0,
        force=True,
        skip_registry=reg,
        skip_lock=Lock(),
    )
    assert TOKEN in reg


def test_process_token_bad_request_without_registry():
    lock = Lock()
    client = MagicMock()
    client.get.side_effect = _http_err(400, "bad")
    stats = _stats()
    out = odds_process.process_token(
        TOKEN,
        {TOKEN: 1},
        set(),
        client,
        lock,
        stats,
        skip_recent_hours=0,
        force=True,
    )
    assert out[0] == []
    assert stats["permanent_error"] == 1


def test_process_token_permanent_inner():
    lock = Lock()
    reg = {}
    client = MagicMock()
    client.get.side_effect = _http_err(404, "nope")
    odds_process.process_token(
        TOKEN,
        {TOKEN: 1},
        set(),
        client,
        lock,
        _stats(),
        skip_recent_hours=0,
        force=True,
        skip_registry=reg,
    )
    assert TOKEN in reg


def test_process_token_incremental_fetch_path_returns_filtered_records():
    lock = Lock()
    client = MagicMock()
    fake_dt = MagicMock()
    fake_dt.timestamp.return_value = 200.0
    with (
        patch(
            "oddsfox.ingestion.polymarket.odds.process.fetch_token_history_with_retry",
            return_value=[(TOKEN, 50, 0.1), (TOKEN, 150, 0.2)],
        ),
        patch("oddsfox.ingestion.polymarket.odds.process.datetime") as dt_mod,
    ):
        dt_mod.now.return_value = fake_dt
        records, cursor = odds_process.process_token(
            TOKEN,
            {TOKEN: 100},
            set(),
            client,
            lock,
            _stats(),
            skip_recent_hours=0,
            force=True,
            max_range_seconds=1000,
        )
    assert records == [(TOKEN, 150, 0.2)]
    assert cursor == 150


def test_fetch_retry_transient_backoff_sleeps():
    c = MagicMock()
    e429 = requests.HTTPError()
    e429.response = MagicMock(status_code=429, text="")
    c.get.side_effect = [e429, {"history": []}]
    with patch("oddsfox.ingestion.polymarket.odds.fetch.time.sleep") as sl:
        with patch(
            "oddsfox.ingestion.polymarket.odds.fetch.random.uniform",
            return_value=1.0,
        ):
            odds_fetch.fetch_token_history_with_retry(
                c,
                "t" * 40,
                interval="1d",
                transient_retries=1,
                transient_backoff_base_seconds=0.5,
            )
    assert sl.called


def test_fetch_retry_range_second_raises_interval_long():
    c = MagicMock()
    n = {"v": 0}

    def ft_side(*a, **k):
        n["v"] += 1
        if n["v"] == 1:
            raise odds_fetch.BadRequestError("a", body="other", status=400)
        raise odds_fetch.BadRequestError("b", body="interval is too long", status=400)

    with patch.object(odds_fetch, "fetch_token_history", side_effect=ft_side):
        with pytest.raises(odds_fetch.BadRequestError):
            odds_fetch.fetch_token_history_with_retry(
                c, "t" * 40, start_ts=10, end_ts=1000, now_ts=500, transient_retries=0
            )


def test_process_token_skip_recent_window():
    """Lines 90-95: recently synced token skipped when not force."""
    lock = Lock()
    client = MagicMock()
    now_ts = 2_000_000
    fake_dt = MagicMock()
    fake_dt.timestamp.return_value = float(now_ts)
    with patch("oddsfox.ingestion.polymarket.odds.process.datetime") as dt_mod:
        dt_mod.now.return_value = fake_dt
        out = odds_process.process_token(
            TOKEN,
            {TOKEN: now_ts - 60},
            set(),
            client,
            lock,
            _stats(),
            skip_recent_hours=24,
            force=False,
        )
    assert out == ([], now_ts - 60)


def test_process_token_unexpected_error():
    lock = Lock()
    client = MagicMock()
    client.get.side_effect = RuntimeError("boom")
    odds_process.process_token(
        TOKEN,
        {TOKEN: 1},
        set(),
        client,
        lock,
        _stats(),
        skip_recent_hours=0,
        force=True,
    )


def test_process_token_records_none_after_fetch():
    lock = Lock()
    stats = _stats()
    client = MagicMock()

    with patch(
        "oddsfox.ingestion.polymarket.odds.process.fetch_token_history_with_retry",
        return_value=None,
    ):
        odds_process.process_token(
            TOKEN,
            {},
            set(),
            client,
            lock,
            stats,
            skip_recent_hours=0,
            force=True,
        )
    assert stats.get("error", 0) >= 1


def test_fetch_retry_range_now_ts_none_uses_time():
    c = MagicMock()
    calls = {"n": 0}

    def ft_side(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise odds_fetch.BadRequestError("a", body="other", status=400)
        return {"history": [{"t": 1, "p": 0.1}]}

    with patch.object(odds_fetch, "fetch_token_history", side_effect=ft_side):
        with patch(
            "oddsfox.ingestion.polymarket.odds.fetch.time.time",
            return_value=500,
        ):
            out = odds_fetch.fetch_token_history_with_retry(
                c, "t" * 40, start_ts=100, end_ts=400, now_ts=None, transient_retries=0
            )
    assert out


def test_fetch_retry_range_adjusted_end_le_start_raises():
    c = MagicMock()
    c.get.side_effect = _http_err(400, "bad range")
    with pytest.raises(odds_fetch.BadRequestError):
        odds_fetch.fetch_token_history_with_retry(
            c, "t" * 40, start_ts=500, end_ts=600, now_ts=400, transient_retries=0
        )


def test_fetch_retry_interval_fails_then_max_raises():
    """284-296: interval!='max', last fetch with interval=max still raises."""
    c = MagicMock()

    def ft_side(*a, **k):
        raise odds_fetch.BadRequestError("b", body="nope", status=400)

    with patch.object(odds_fetch, "fetch_token_history", side_effect=ft_side):
        with pytest.raises(odds_fetch.BadRequestError):
            odds_fetch.fetch_token_history_with_retry(
                c, "t" * 40, interval="1d", transient_retries=0
            )


def test_fetch_retry_interval_second_bad_request_interval_too_long():
    c = MagicMock()
    calls = {"n": 0}

    def ft_side(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise odds_fetch.BadRequestError("first", body="other", status=400)
        raise odds_fetch.BadRequestError(
            "second", body="interval is too long", status=400
        )

    with patch.object(odds_fetch, "fetch_token_history", side_effect=ft_side):
        with pytest.raises(odds_fetch.BadRequestError):
            odds_fetch.fetch_token_history_with_retry(
                c, "t" * 40, interval="1d", transient_retries=0
            )


def test_process_token_post_fetch_exception_does_not_touch_skip_registry():
    lock = Lock()
    reg = {}
    client = MagicMock()
    client.get.return_value = {"history": [{"t": 200, "p": 0.5}]}
    with patch("builtins.max", side_effect=RuntimeError("outer")):
        odds_process.process_token(
            TOKEN,
            {TOKEN: 100},
            set(),
            client,
            lock,
            _stats(),
            skip_recent_hours=0,
            force=True,
            skip_registry=reg,
            skip_lock=Lock(),
        )
    assert reg == {}
