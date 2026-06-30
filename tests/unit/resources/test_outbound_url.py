import socket

import pytest

from oddsfox.resources.outbound_url import (
    OutboundUrlError,
    assert_same_origin,
    clear_outbound_url_host_cache,
    join_under_base,
    validate_outbound_https_url,
)


@pytest.fixture(autouse=True)
def _clear_dns_cache():
    clear_outbound_url_host_cache()
    yield
    clear_outbound_url_host_cache()


def _mock_public_dns(monkeypatch, public_host: str = "example.com") -> None:
    public_hosts = {public_host, "other.example"}

    def fake_getaddrinfo(host, *args, **kwargs):
        if host in public_hosts:
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))]
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)


def test_validate_outbound_https_url_accepts_public_https(monkeypatch):
    _mock_public_dns(monkeypatch)

    assert validate_outbound_https_url("https://example.com/path") == (
        "https://example.com/path"
    )


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com/path",
        "file:///tmp/data.csv",
        "https://127.0.0.1/data.csv",
        "https://10.0.0.1/data.csv",
        "https://",
        "  ",
    ],
)
def test_validate_outbound_https_url_rejects_unsafe_targets(monkeypatch, url):
    _mock_public_dns(monkeypatch)

    with pytest.raises(OutboundUrlError):
        validate_outbound_https_url(url)


def test_assert_same_origin_accepts_matching_origin(monkeypatch):
    _mock_public_dns(monkeypatch)

    assert (
        assert_same_origin(
            "https://example.com/a.csv",
            "https://example.com/root",
        )
        == "https://example.com/a.csv"
    )


def test_assert_same_origin_rejects_other_origin(monkeypatch):
    _mock_public_dns(monkeypatch)

    with pytest.raises(OutboundUrlError, match="does not match"):
        assert_same_origin("https://other.example/x", "https://example.com/root")


def test_join_under_base_accepts_relative_path(monkeypatch):
    _mock_public_dns(monkeypatch)

    assert join_under_base("https://example.com/root", "/data/file.csv") == (
        "https://example.com/root/data/file.csv"
    )


def test_join_under_base_rejects_protocol_relative(monkeypatch):
    _mock_public_dns(monkeypatch)

    with pytest.raises(OutboundUrlError, match="protocol-relative"):
        join_under_base("https://example.com", "//evil.example/file.csv")
