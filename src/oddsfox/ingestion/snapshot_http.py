"""Shared HTTP and byte-cache helpers for snapshot-style ingestion clients."""

from __future__ import annotations

import hashlib
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

TRANSIENT_STATUSES = frozenset({408, 429, 500, 502, 503, 504})


class TransientSnapshotHttpError(RuntimeError):
    """Raised when an optional snapshot fetch fails with a retryable HTTP/network error."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        source_file: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.source_file = source_file


def is_transient_status(status: int) -> bool:
    return status in TRANSIENT_STATUSES or status == 0


def transient_error_from_requests(
    exc: BaseException,
    *,
    source_file: str | None = None,
) -> TransientSnapshotHttpError | None:
    import requests

    if isinstance(
        exc, requests.exceptions.Timeout | requests.exceptions.ConnectionError
    ):
        return TransientSnapshotHttpError(
            str(exc),
            status_code=0,
            source_file=source_file,
        )
    if isinstance(exc, requests.exceptions.HTTPError):
        response = getattr(exc, "response", None)
        status_code = int(response.status_code) if response is not None else 0
        if is_transient_status(status_code):
            return TransientSnapshotHttpError(
                str(exc),
                status_code=status_code,
                source_file=source_file,
            )
    return None


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_bytes_cache(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def read_bytes_cache(path: Path) -> bytes | None:
    if path.is_file():
        return path.read_bytes()
    return None


def cache_path_for_source(cache_dir: Path, source_file: str, *, subdir: str) -> Path:
    return cache_dir / subdir / source_file


def retry_after_seconds(resp: Any, *, cap: float = 120.0) -> float | None:
    headers: Mapping[str, str] = getattr(resp, "headers", {}) or {}
    raw = headers.get("Retry-After")
    if raw is None or not str(raw).strip():
        return None
    try:
        sec = float(str(raw).strip())
        if sec < 0:
            return None
        return min(sec, cap)
    except ValueError:
        return None


@dataclass(frozen=True)
class FetchResult:
    status_code: int
    content: bytes
    attempts: int
    source: str
    error: str | None = None
    url: str | None = None
    from_disk_cache: bool = False


class SnapshotHttpClient:
    """curl_cffi GET client with polite jitter and retries."""

    def __init__(
        self,
        *,
        user_agent: str,
        accept: str,
        validate_url: Callable[[str], str],
        timeout: float = 60.0,
        min_delay: float = 0.5,
        max_delay: float = 1.5,
        max_retries: int = 4,
        http_impersonate: str = "chrome120",
    ) -> None:
        from curl_cffi import requests as cffi_requests
        from curl_cffi.requests.errors import RequestsError

        self._RequestsError = RequestsError
        self._validate_url = validate_url
        self.timeout = timeout
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
        self.session = cffi_requests.Session(impersonate=http_impersonate)
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": accept,
                "Accept-Language": "en-US,en;q=0.9",
            }
        )

    def _sleep_jitter(self) -> None:
        time.sleep(random.uniform(self.min_delay, self.max_delay))

    def get_bytes(self, url: str) -> FetchResult:
        validated = self._validate_url(url)
        self._sleep_jitter()
        last_error: str | None = None
        for attempt in range(1, self.max_retries + 2):
            try:
                resp = self.session.get(validated, timeout=self.timeout)
            except self._RequestsError as exc:
                last_error = str(exc)
                if attempt > self.max_retries:
                    return FetchResult(
                        status_code=0,
                        content=b"",
                        attempts=attempt,
                        source="network_error",
                        error=last_error,
                        url=validated,
                    )
                time.sleep(min(2.0**attempt, 30.0))
                continue
            status = int(resp.status_code)
            body = bytes(resp.content or b"")
            if status in TRANSIENT_STATUSES and attempt <= self.max_retries:
                retry_after = retry_after_seconds(resp) or min(2.0**attempt, 30.0)
                time.sleep(retry_after)
                continue
            return FetchResult(
                status_code=status,
                content=body,
                attempts=attempt,
                source="http",
                error=None if status < 400 else f"http_{status}",
                url=validated,
            )
        return FetchResult(  # pragma: no cover - defensive fallback
            status_code=0,
            content=b"",
            attempts=self.max_retries + 1,
            source="exhausted",
            error=last_error or "max_retries",
            url=validated,
        )


__all__ = [
    "FetchResult",
    "SnapshotHttpClient",
    "TRANSIENT_STATUSES",
    "TransientSnapshotHttpError",
    "cache_path_for_source",
    "is_transient_status",
    "read_bytes_cache",
    "retry_after_seconds",
    "sha256_bytes",
    "transient_error_from_requests",
    "write_bytes_cache",
]
