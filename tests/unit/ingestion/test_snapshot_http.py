"""Shared snapshot HTTP retry and byte-cache helpers."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from oddsfox.ingestion import snapshot_http


def test_sha256_and_cache_roundtrip(tmp_path: Path) -> None:
    data = b"rank,club\n1,Test"
    path = snapshot_http.cache_path_for_source(tmp_path, "source.csv", subdir="csv")

    assert snapshot_http.sha256_bytes(data) == snapshot_http.sha256_bytes(data)
    assert path == tmp_path / "csv" / "source.csv"
    assert snapshot_http.read_bytes_cache(path) is None

    snapshot_http.write_bytes_cache(path, data)

    assert snapshot_http.read_bytes_cache(path) == data


def test_snapshot_http_client_retries_transient_then_success() -> None:
    pytest.importorskip("curl_cffi")

    client = snapshot_http.SnapshotHttpClient(
        user_agent="test-agent",
        accept="text/plain",
        validate_url=lambda url: url,
        timeout=5.0,
        min_delay=0.0,
        max_delay=0.0,
        max_retries=3,
    )
    bad = MagicMock(status_code=503, content=b"", headers={})
    good = MagicMock(status_code=200, content=b"ok", headers={})
    client.session.get = MagicMock(side_effect=[bad, good])

    sleeps: list[float] = []
    orig_sleep = snapshot_http.time.sleep
    try:
        snapshot_http.time.sleep = lambda s: sleeps.append(float(s))
        out = client.get_bytes("https://example.com/source.tsv")
    finally:
        snapshot_http.time.sleep = orig_sleep

    assert out.status_code == 200
    assert out.attempts == 2
    assert sleeps


def test_snapshot_http_client_network_error_exhausts_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("curl_cffi")
    from curl_cffi.requests.errors import RequestsError

    client = snapshot_http.SnapshotHttpClient(
        user_agent="test-agent",
        accept="text/plain",
        validate_url=lambda url: url,
        timeout=5.0,
        min_delay=0.0,
        max_delay=0.0,
        max_retries=1,
    )
    client.session.get = MagicMock(side_effect=RequestsError("boom"))
    monkeypatch.setattr(snapshot_http.time, "sleep", lambda _s: None)

    out = client.get_bytes("https://example.com/source.tsv")

    assert out.status_code == 0
    assert out.source == "network_error"
    assert out.error == "boom"


def test_is_transient_status() -> None:
    assert snapshot_http.is_transient_status(503)
    assert snapshot_http.is_transient_status(0)
    assert not snapshot_http.is_transient_status(404)


def test_transient_error_from_requests() -> None:
    import requests

    timeout_exc = requests.exceptions.Timeout("timed out")
    out = snapshot_http.transient_error_from_requests(
        timeout_exc,
        source_file="results.csv",
    )
    assert isinstance(out, snapshot_http.TransientSnapshotHttpError)
    assert out.status_code == 0
    assert out.source_file == "results.csv"

    conn_exc = requests.exceptions.ConnectionError("refused")
    assert snapshot_http.transient_error_from_requests(conn_exc) is not None

    resp = requests.Response()
    resp.status_code = 503
    http_exc = requests.exceptions.HTTPError("503", response=resp)
    transient = snapshot_http.transient_error_from_requests(
        http_exc,
        source_file="2026-06-19",
    )
    assert transient is not None
    assert transient.status_code == 503

    resp.status_code = 404
    hard_exc = requests.exceptions.HTTPError("404", response=resp)
    assert snapshot_http.transient_error_from_requests(hard_exc) is None

    assert snapshot_http.transient_error_from_requests(RuntimeError("boom")) is None
