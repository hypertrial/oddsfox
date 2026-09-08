"""Pinned public HTTPS: allowlists, public DNS, IP pin, fail-closed venue redirects."""

import ipaddress
import socket
import time
from urllib.parse import urljoin, urlsplit

import httpx

DOCUMENT_HOSTS = frozenset(
    {
        "kalshi.com",
        "www.kalshi.com",
        "kalshi-public-docs.s3.amazonaws.com",
        "kalshi-public-docs.s3.us-east-1.amazonaws.com",
        "kalshi-public-docs.s3.us-east-2.amazonaws.com",
        "polymarket.com",
        "www.polymarket.com",
        "docs.polymarket.com",
    }
)
VENUE_HOSTS = frozenset(
    {
        "external-api.kalshi.com",
        "gamma-api.polymarket.com",
    }
)


def is_public_ip(address: str) -> bool:
    ip = ipaddress.ip_address(address)
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    return bool(ip.is_global) and not ip.is_multicast and not ip.is_unspecified


def validate_https_url(url: str, allowed_hosts: frozenset[str]) -> str:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname not in allowed_hosts
        or parsed.username
        or parsed.password
        or parsed.port not in {None, 443}
    ):
        raise ValueError("host is not an approved official HTTPS host")
    return parsed.hostname or ""


def resolve_public(hostname: str) -> tuple[str, str]:
    addresses = {
        str(item[4][0]) for item in socket.getaddrinfo(hostname, 443, type=socket.SOCK_STREAM)
    }
    if not addresses or any(not is_public_ip(address) for address in addresses):
        raise ValueError("host resolves to a non-public network")
    return hostname, sorted(addresses)[0]


def uses_custom_transport(client: httpx.Client) -> bool:
    transport = getattr(client, "_transport", None)
    return type(transport).__name__ == "MockTransport"


def stream_get(
    client: httpx.Client,
    url: str,
    *,
    allowed_hosts: frozenset[str],
    follow_redirects: bool,
    max_bytes: int,
    timeout: float = 20,
    params=None,
    max_redirects: int = 4,
    pin_dns: bool | None = None,
) -> bytes:
    pin = (not uses_custom_transport(client)) if pin_dns is None else pin_dns
    for _ in range(max_redirects):
        host = validate_https_url(url, allowed_hosts)
        headers: dict[str, str] = {}
        extensions: dict[str, str] = {}
        target: str | httpx.URL = url
        if pin:
            _, address = resolve_public(host)
            target = httpx.URL(url).copy_with(host=address)
            headers["Host"] = host
            extensions["sni_hostname"] = host
        with client.stream(
            "GET",
            target,
            params=params,
            headers=headers,
            extensions=extensions,
            timeout=timeout,
            follow_redirects=False,
        ) as response:
            if response.is_redirect:
                if not follow_redirects:
                    raise ValueError("venue API redirected; fail-closed")
                location = response.headers.get("location")
                if not location:
                    raise ValueError("redirect missing Location")
                url = urljoin(url, location)
                params = None
                continue
            response.raise_for_status()
            chunks, size, started = [], 0, time.monotonic()
            for chunk in response.iter_bytes():
                size += len(chunk)
                if size > max_bytes or time.monotonic() - started > 45:
                    raise ValueError("response exceeds resource limit")
                chunks.append(chunk)
            return b"".join(chunks)
    raise ValueError("too many redirects")
