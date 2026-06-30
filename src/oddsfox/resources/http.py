import base64
import hashlib
import hmac
import logging
import time
from threading import Lock
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from oddsfox.config.settings import HTTP_REQUEST_TIMEOUT

logger = logging.getLogger(__name__)


def _bytes_from_hex(hex_str: str) -> bytes:
    """Thin wrapper so hex-decode failures are testable without patching builtins."""
    return bytes.fromhex(hex_str)


class ClobAuth:
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self._secret_bytes = self._decode_secret(api_secret)

    def _decode_secret(self, secret: str) -> bytes:
        try:
            return base64.b64decode(secret)
        except Exception:
            pass
        try:
            if len(secret) == 64 and all(c in "0123456789abcdefABCDEF" for c in secret):
                return _bytes_from_hex(secret)
        except Exception:
            pass
        return secret.encode("utf-8")

    def sign(
        self, method: str, path: str, body: str = "", timestamp: str | None = None
    ) -> dict:
        if timestamp is None:
            timestamp = str(int(time.time()))
        message = timestamp + method + path + body
        signature = hmac.new(
            self._secret_bytes,
            message.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        signature_b64 = base64.b64encode(signature).decode("utf-8")
        return {
            "CLOB-API-KEY": self.api_key,
            "CLOB-API-SIGN": signature_b64,
            "CLOB-API-TIMESTAMP": timestamp,
            "CLOB-API-PASSPHRASE": self.api_passphrase,
        }


class RateLimiter:
    def __init__(self, rate: float):
        if rate <= 0:
            raise ValueError("rate must be positive")
        self.rate = rate
        self.capacity = rate
        self.tokens = rate
        self.lock = Lock()
        self.last_check = time.monotonic()

    def wait(self):
        while True:
            sleep_time = 0.0
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.last_check
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.last_check = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                sleep_time = (1 - self.tokens) / self.rate
            time.sleep(max(0.0, sleep_time))

    def set_rate(self, new_rate: float):
        if new_rate <= 0:
            raise ValueError("new_rate must be positive")
        with self.lock:
            ratio = (self.tokens / self.capacity) if self.capacity > 0 else 0.0
            self.rate = float(new_rate)
            self.capacity = float(new_rate)
            self.tokens = min(self.capacity, max(0.0, ratio * self.capacity))

    def get_rate(self) -> float:
        with self.lock:
            return float(self.rate)


class APIClient:
    def __init__(
        self,
        base_url: Optional[str] = None,
        retries: int = 3,
        backoff_factor: float = 1.0,
        requests_per_second: Optional[float] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        api_passphrase: Optional[str] = None,
        auth: Optional[ClobAuth] = None,
        rate_limiter: Optional[RateLimiter] = None,
        request_timeout: Optional[float | tuple[float, float]] = HTTP_REQUEST_TIMEOUT,
    ):
        self.base_url = base_url.rstrip("/") if base_url else ""
        self.session = requests.Session()
        self.auth = auth or (
            ClobAuth(api_key, api_secret, api_passphrase)
            if all([api_key, api_secret, api_passphrase])
            else None
        )
        self.rate_limit_lock = Lock()
        self.rate_limiter = rate_limiter
        self.request_timeout = request_timeout
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            other=retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=["GET", "POST", "PUT", "DELETE"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.delay = 1.0 / requests_per_second if requests_per_second else 0
        self.last_request_time = 0.0

    def _wait_for_rate_limit(self):
        if self.rate_limiter:
            self.rate_limiter.wait()
            return
        if self.delay > 0:
            with self.rate_limit_lock:
                elapsed = time.time() - self.last_request_time
                wait_time = self.delay - elapsed
                if wait_time > 0:
                    time.sleep(wait_time)
                self.last_request_time = time.time()

    def get(
        self,
        endpoint: str,
        params: dict | None = None,
        use_auth: bool = False,
        **kwargs,
    ):
        self._wait_for_rate_limit()
        url = (
            f"{self.base_url}{endpoint}"
            if self.base_url and not endpoint.startswith("http")
            else endpoint
        )
        headers = kwargs.pop("headers", {})
        if use_auth and self.auth:
            path = endpoint
            if params:
                query_string = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
                path = f"{endpoint}?{query_string}"
            headers.update(self.auth.sign("GET", path))
        if "timeout" not in kwargs and self.request_timeout is not None:
            kwargs["timeout"] = self.request_timeout
        response = self.session.get(url, params=params, headers=headers, **kwargs)
        response.raise_for_status()
        return response.json()
