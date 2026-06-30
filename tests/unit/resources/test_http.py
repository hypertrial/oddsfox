import base64
from unittest.mock import MagicMock, patch

import pytest
import requests

from oddsfox.resources.http import APIClient, ClobAuth, RateLimiter


def test_clob_auth_base64_secret():
    secret = base64.b64encode(b"hello-world").decode()
    auth = ClobAuth("k", secret, "p")
    assert auth._secret_bytes == b"hello-world"


def test_clob_auth_hex_secret():
    raw = b"\xab" * 32
    secret = raw.hex()
    with patch(
        "oddsfox.resources.http.base64.b64decode",
        side_effect=Exception("force hex branch"),
    ):
        auth = ClobAuth("k", secret, "p")
    assert auth._secret_bytes == raw


def test_clob_auth_plain_utf8_secret():
    auth = ClobAuth("k", "not-base64-not-64-hex", "p")
    assert auth._secret_bytes == b"not-base64-not-64-hex"


def test_clob_auth_hex_branch_inner_exception():
    """Cover inner except when hex decode fails despite passing the hex shape check."""
    secret = "a" * 64  # length + charset pass bytes.fromhex in normal cases

    with patch("oddsfox.resources.http.base64.b64decode", side_effect=Exception("x")):
        with patch(
            "oddsfox.resources.http._bytes_from_hex",
            side_effect=ValueError("fromhex failed"),
        ):
            auth = ClobAuth("k", secret, "p")
    assert auth._secret_bytes == secret.encode("utf-8")


def test_clob_auth_sign_with_explicit_timestamp():
    auth = ClobAuth("k", "secret", "p")
    headers = auth.sign("GET", "/path", body="{}", timestamp="12345")
    assert headers["CLOB-API-TIMESTAMP"] == "12345"
    assert "CLOB-API-SIGN" in headers


def test_rate_limiter_negative_rate_raises():
    with pytest.raises(ValueError, match="rate must be positive"):
        RateLimiter(0)


def test_rate_limiter_wait_consumes_token():
    rl = RateLimiter(1000.0)
    rl.wait()
    assert rl.tokens < rl.capacity


def test_rate_limiter_wait_sleeps_when_depleted(monkeypatch):
    rl = RateLimiter(10.0)
    rl.tokens = 0.0
    # Align with patched monotonic so elapsed is 0; otherwise last_check from __init__
    # makes elapsed huge and the bucket refills without ever calling sleep.
    rl.last_check = 1000.0
    sleeps = []

    def capture_sleep(s):
        sleeps.append(s)
        rl.tokens = rl.capacity  # unblock after one sleep

    monkeypatch.setattr("oddsfox.resources.http.time.sleep", capture_sleep)
    monkeypatch.setattr("oddsfox.resources.http.time.monotonic", lambda: 1000.0)
    rl.wait()
    assert sleeps


def test_rate_limiter_set_rate_negative_raises():
    rl = RateLimiter(10.0)
    with pytest.raises(ValueError, match="new_rate must be positive"):
        rl.set_rate(0)


def test_rate_limiter_set_rate_preserves_ratio():
    rl = RateLimiter(10.0)
    rl.tokens = 5.0
    rl.set_rate(20.0)
    assert rl.get_rate() == 20.0


def test_api_client_get_with_relative_endpoint_and_params():
    session = MagicMock()
    session.get.return_value.json.return_value = {"ok": True}
    session.get.return_value.raise_for_status = MagicMock()
    auth = ClobAuth("k", "secret", "p")
    client = APIClient(
        base_url="https://api.example.com",
        api_key="k",
        api_secret="secret",
        api_passphrase="p",
    )
    client.session = session
    client.auth = auth
    out = client.get("/x", params={"b": "2", "a": "1"}, use_auth=True)
    assert out == {"ok": True}
    session.get.assert_called_once()
    url = session.get.call_args[0][0]
    assert "http" in url


def test_api_client_get_absolute_endpoint():
    session = MagicMock()
    session.get.return_value.json.return_value = []
    session.get.return_value.raise_for_status = MagicMock()
    client = APIClient(base_url="https://api.example.com")
    client.session = session
    client.get("https://other.example/full", params=None)
    session.get.assert_called_once_with(
        "https://other.example/full",
        params=None,
        headers={},
        timeout=client.request_timeout,
    )


def test_api_client_get_uses_default_timeout():
    session = MagicMock()
    session.get.return_value.json.return_value = {"ok": True}
    session.get.return_value.raise_for_status = MagicMock()
    client = APIClient(
        base_url="https://api.example.com",
        request_timeout=(2.0, 7.0),
    )
    client.session = session
    client.get("/timeout")
    assert session.get.call_args.kwargs["timeout"] == (2.0, 7.0)


def test_api_client_get_explicit_timeout_overrides_default():
    session = MagicMock()
    session.get.return_value.json.return_value = {"ok": True}
    session.get.return_value.raise_for_status = MagicMock()
    client = APIClient(
        base_url="https://api.example.com",
        request_timeout=(2.0, 7.0),
    )
    client.session = session
    client.get("/timeout", timeout=(1.0, 3.0))
    assert session.get.call_args.kwargs["timeout"] == (1.0, 3.0)


def test_api_client_rate_limiter_wait():
    rl = RateLimiter(10000.0)
    client = APIClient(base_url="https://x.com", rate_limiter=rl)
    with patch.object(rl, "wait", wraps=rl.wait) as w:
        client._wait_for_rate_limit()
        w.assert_called_once()


def test_api_client_delay_without_limiter():
    client = APIClient(base_url="https://x.com", requests_per_second=10.0)
    with patch("oddsfox.resources.http.time.sleep") as sl:
        client.last_request_time = 0.0
        times = iter([0.0, 0.05, 0.15, 0.2, 0.25, 0.3])

        def next_time():
            return next(times)

        with patch("oddsfox.resources.http.time.time", side_effect=next_time):
            client._wait_for_rate_limit()
            client._wait_for_rate_limit()
        assert sl.called


def test_api_client_delay_skips_sleep_when_caught_up():
    """Second wait: elapsed already exceeds delay — no sleep branch."""
    client = APIClient(base_url="https://x.com", requests_per_second=10.0)
    with patch("oddsfox.resources.http.time.sleep") as sl:
        with patch("oddsfox.resources.http.time.time", return_value=100.0):
            client.last_request_time = 99.0
            client._wait_for_rate_limit()
        sl.assert_not_called()


def test_api_client_http_error_propagates():
    client = APIClient(base_url="https://x.com")
    err = requests.HTTPError()
    err.response = MagicMock(status_code=500)
    client.session.get = MagicMock(side_effect=err)
    with pytest.raises(requests.HTTPError):
        client.get("/fail")


def test_api_client_auth_factory():
    client = APIClient(
        base_url="https://x.com",
        api_key="a",
        api_secret=base64.b64encode(b"s").decode(),
        api_passphrase="p",
    )
    assert client.auth is not None


def test_api_client_explicit_auth():
    auth = ClobAuth("k", "x" * 64, "p")
    client = APIClient(base_url="https://x.com", auth=auth)
    assert client.auth is auth


def test_api_client_get_use_auth_without_params():
    session = MagicMock()
    session.get.return_value.json.return_value = {"ok": True}
    session.get.return_value.raise_for_status = MagicMock()
    auth = ClobAuth("k", "secret", "p")
    client = APIClient(
        base_url="https://api.example.com",
        api_key="k",
        api_secret="secret",
        api_passphrase="p",
    )
    client.session = session
    client.auth = auth
    client.get("/path-only", params=None, use_auth=True)
    session.get.assert_called_once()
    call_kw = session.get.call_args[1]
    assert "headers" in call_kw


def test_rate_limiter_wait_skips_sleep_when_sleep_time_zero(monkeypatch):
    """When (1-tokens)/rate == 0, skip time.sleep and loop until tokens refill (80->69)."""
    rl = RateLimiter(1.0)
    rl.rate = float("inf")
    rl.capacity = float("inf")
    rl.tokens = 0.5
    rl.last_check = 100.0
    times = iter([100.0, 101.0])

    def mono():
        return next(times)

    monkeypatch.setattr("oddsfox.resources.http.time.monotonic", mono)
    sleeps = []
    monkeypatch.setattr("oddsfox.resources.http.time.sleep", lambda s: sleeps.append(s))
    rl.wait()
    assert all(s <= 0.0 for s in sleeps)


def test_rate_limiter_wait_skips_sleep_when_rounds_to_zero(monkeypatch):
    """Cover branch where sleep_time is 0 so the inner sleep is skipped (loop continues)."""
    rl = RateLimiter(1e300)
    rl.tokens = 1.0 - 1e-320
    rl.last_check = 0.0
    sleeps = []
    monkeypatch.setattr("oddsfox.resources.http.time.sleep", lambda s: sleeps.append(s))
    # Enough monotonic samples so wait() never exhausts the iterator mid-loop.
    base = [0.0, 0.0, 1.0, 2.0, 3.0]
    it = iter(base)

    def mono():
        try:
            return next(it)
        except StopIteration:
            return base[-1] + 1.0

    monkeypatch.setattr("oddsfox.resources.http.time.monotonic", mono)
    rl.wait()
    assert rl.tokens < rl.capacity


def test_rate_limiter_wait_zero_sleep_loops_without_sleep(monkeypatch):
    rl = RateLimiter(1.0)
    rl.rate = float("inf")
    rl.capacity = float("inf")
    rl.tokens = 0.0
    rl.last_check = 0.0
    times = iter([0.0, 1.0])
    sleeps = []
    monkeypatch.setattr("oddsfox.resources.http.time.monotonic", lambda: next(times))
    monkeypatch.setattr(
        "oddsfox.resources.http.time.sleep", lambda value: sleeps.append(value)
    )
    rl.wait()
    assert all(s <= 0.0 for s in sleeps)
