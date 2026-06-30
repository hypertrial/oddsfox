import time
from threading import Lock
from unittest.mock import MagicMock, patch

import pytest
import requests

from oddsfox.ingestion.polymarket.odds import fetch as odds_fetch
from oddsfox.ingestion.polymarket.odds import process as odds_process


def test_status_hooks():
    seen = []

    odds_fetch.set_status_hook(lambda s: seen.append(s))
    odds_fetch._emit_status(200)
    odds_fetch._emit_status_via(lambda s: (_ for _ in ()).throw(RuntimeError("x")), 400)
    odds_fetch.set_status_hook(None)


def test_build_client():
    c = odds_fetch.build_client("https://clob.example", rate_limiter=None)
    assert c.base_url == "https://clob.example"
    assert c.request_timeout == odds_fetch.HTTP_REQUEST_TIMEOUT


def test_fetch_token_history_interval_success():
    c = MagicMock()
    c.get.return_value = {"history": [{"t": 1, "p": 0.5}]}
    out = odds_fetch.fetch_token_history(c, "t" * 40, interval="1d")
    assert out and out[0][2] == 0.5


def test_fetch_token_history_range():
    c = MagicMock()
    c.get.return_value = {"history": []}
    odds_fetch.fetch_token_history(c, "t" * 40, start_ts=1, end_ts=2)


def test_fetch_token_history_http_429():
    c = MagicMock()
    err = requests.HTTPError()
    err.response = MagicMock(status_code=429, text="")
    c.get.side_effect = err
    assert odds_fetch.fetch_token_history(c, "t" * 40) is None


def test_fetch_token_history_http_400_raises():
    c = MagicMock()
    err = requests.HTTPError()
    err.response = MagicMock(status_code=400, text="bad")
    c.get.side_effect = err
    with pytest.raises(odds_fetch.BadRequestError):
        odds_fetch.fetch_token_history(c, "t" * 40)


def test_fetch_token_history_http_404_permanent():
    c = MagicMock()
    err = requests.HTTPError()
    err.response = MagicMock(status_code=404, text="nope")
    c.get.side_effect = err
    with pytest.raises(odds_fetch.PermanentAPIError):
        odds_fetch.fetch_token_history(c, "t" * 40)


def test_fetch_token_history_generic_exc():
    c = MagicMock()
    c.get.side_effect = ValueError("nope")
    assert odds_fetch.fetch_token_history(c, "t" * 40) is None


def test_fetch_token_history_oserror_returns_none():
    c = MagicMock()
    c.get.side_effect = OSError("network down")
    assert odds_fetch.fetch_token_history(c, "t" * 40) is None


def test_fetch_token_history_clob_connection_error_returns_none():
    c = MagicMock()
    c.get.side_effect = requests.ConnectionError("clob down")
    assert odds_fetch.fetch_token_history(c, "t" * 40) is None


def test_fetch_token_history_exception_with_response_transient():
    c = MagicMock()

    class _ErrWithResponse(Exception):
        def __init__(self):
            self.response = MagicMock(status_code=503, text="busy")

    c.get.side_effect = _ErrWithResponse()
    assert odds_fetch.fetch_token_history(c, "t" * 40) is None


def test_fetch_token_history_exception_with_response_4xx_permanent():
    c = MagicMock()

    class _ErrWithResponse(Exception):
        def __init__(self):
            self.response = MagicMock(status_code=403, text="forbidden")

    c.get.side_effect = _ErrWithResponse()
    with pytest.raises(odds_fetch.PermanentAPIError):
        odds_fetch.fetch_token_history(c, "t" * 40)


def test_fetch_token_history_timeout_returns_none_and_emits_error_status():
    c = MagicMock()
    c.get.side_effect = requests.Timeout("slow read")
    seen = []
    out = odds_fetch.fetch_token_history(
        c,
        "t" * 40,
        start_ts=1,
        end_ts=2,
        status_hook=lambda status: seen.append(status),
    )
    assert out is None
    assert seen == [-1]


def test_fetch_with_retry_transient_then_ok():
    c = MagicMock()
    with (
        patch(
            "oddsfox.ingestion.polymarket.odds.fetch.fetch_token_history",
            side_effect=[None, [("t" * 40, 1, 0.1)]],
        ),
        patch(
            "oddsfox.ingestion.polymarket.odds.fetch.time.sleep",
            lambda s: None,
        ),
    ):
        out = odds_fetch.fetch_token_history_with_retry(
            c,
            "t" * 40,
            interval="1d",
            transient_retries=1,
            transient_backoff_base_seconds=0.01,
        )
    assert out


def test_fetch_with_retry_retries_on_timeout_none():
    c = MagicMock()
    with (
        patch(
            "oddsfox.ingestion.polymarket.odds.fetch.fetch_token_history",
            side_effect=[None, [("t" * 40, 1, 0.1)]],
        ) as fetch_history,
        patch(
            "oddsfox.ingestion.polymarket.odds.fetch.time.sleep",
            lambda s: None,
        ),
    ):
        out = odds_fetch.fetch_token_history_with_retry(
            c,
            "t" * 40,
            interval="1d",
            transient_retries=1,
            transient_backoff_base_seconds=0.01,
        )
    assert out
    assert fetch_history.call_count == 2


def test_fetch_with_retry_bad_request_range(monkeypatch):
    c = MagicMock()
    err = requests.HTTPError()
    err.response = MagicMock(
        status_code=400, text="interval is too long for this token"
    )
    c.get.side_effect = err
    with pytest.raises(odds_fetch.BadRequestError):
        odds_fetch.fetch_token_history_with_retry(
            c, "t" * 40, start_ts=10, end_ts=100, now_ts=200
        )


def test_fetch_with_retry_clamps_end(monkeypatch):
    c = MagicMock()
    calls = {"n": 0}

    def side(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            e = odds_fetch.BadRequestError("x", body="other", status=400)
            raise e
        return {"history": []}

    c.get.side_effect = side
    with patch("oddsfox.ingestion.polymarket.odds.fetch.time.time", return_value=50):
        odds_fetch.fetch_token_history_with_retry(
            c, "t" * 40, start_ts=10, end_ts=100, now_ts=40, transient_retries=0
        )


def test_split_time_windows_and_token_heuristic():
    assert list(odds_process.split_time_windows(0, 100, 30))[:2]
    assert odds_process.is_probably_clob_token("") is False
    assert odds_process.is_probably_clob_token("open_token_x") is False
    assert odds_process.is_probably_clob_token("bad!") is False
    assert odds_process.is_probably_clob_token("a" * 64) is True
    assert odds_process.is_probably_clob_token("t1") is True


def test_process_token_paths():
    lock = Lock()
    stats = {
        "success": 0,
        "empty": 0,
        "error": 0,
        "skipped": 0,
        "fully_checked": 0,
        "permanent_error": 0,
    }
    client = MagicMock()
    client.get.return_value = {"history": [{"t": 200, "p": 0.5}]}

    odds_process.process_token(
        "t" * 40,
        {("t" * 40): 100},
        set(),
        client,
        lock,
        stats,
        skip_recent_hours=0,
        force=False,
    )

    odds_process.process_token(
        "t" * 40,
        {("t" * 40): int(time.time())},
        set(),
        client,
        lock,
        stats,
        skip_recent_hours=999,
        force=False,
    )

    odds_process.process_token(
        "t" * 40,
        {},
        set(),
        client,
        lock,
        stats,
        skip_recent_hours=0,
        force=False,
    )


def test_process_token_fully_checked():
    lock = Lock()
    stats = {
        k: 0
        for k in (
            "success",
            "empty",
            "error",
            "skipped",
            "fully_checked",
            "permanent_error",
        )
    }
    client = MagicMock()
    odds_process.process_token(
        "t" * 40,
        {},
        {"t" * 40},
        client,
        lock,
        stats,
        force=False,
    )


def test_process_token_bad_request():
    lock = Lock()
    stats = {
        k: 0
        for k in (
            "success",
            "empty",
            "error",
            "skipped",
            "fully_checked",
            "permanent_error",
        )
    }
    client = MagicMock()

    def boom(*a, **k):
        raise odds_fetch.BadRequestError("b", status=400, body="")

    client.get.side_effect = boom
    odds_process.process_token(
        "t" * 40,
        {("t" * 40): 1},
        set(),
        client,
        lock,
        stats,
        skip_recent_hours=0,
        force=True,
        max_range_seconds=3600,
    )


@pytest.mark.filterwarnings("ignore")
def test_process_token_records_none_branch():
    lock = Lock()
    stats = {
        k: 0
        for k in (
            "success",
            "empty",
            "error",
            "skipped",
            "fully_checked",
            "permanent_error",
        )
    }
    client = MagicMock()
    client.get.return_value = None

    with patch(
        "oddsfox.ingestion.polymarket.odds.process.fetch_token_history_with_retry",
        return_value=None,
    ):
        odds_process.process_token(
            "t" * 40,
            {},
            set(),
            client,
            lock,
            stats,
            skip_recent_hours=0,
            force=True,
        )


def test_helpers_response_status():
    exc = Exception()
    assert odds_fetch._response_status_and_body(exc) == (None, "")
    resp = MagicMock(status_code=500, text="body")
    exc.response = resp
    assert odds_fetch._response_status_and_body(exc) == (500, "body")
