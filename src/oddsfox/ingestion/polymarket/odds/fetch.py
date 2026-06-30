"""
HTTP helpers for odds ingestion.
"""

import logging
import random
import time
from typing import Callable, List, Optional, Tuple

import requests

from oddsfox.config.settings import HTTP_REQUEST_TIMEOUT
from oddsfox.ingestion.polymarket.errors import ClobRequestError, clob_get
from oddsfox.resources.http import APIClient

logger = logging.getLogger(__name__)
_status_hook: Optional[Callable[[int], None]] = None


class PermanentAPIError(Exception):
    """Raised when an API response indicates a non-retriable client error."""


class BadRequestError(Exception):
    """Raised when the API returns a 400 for request params."""

    def __init__(
        self, message: str, *, status: int | None = None, body: str = "", params=None
    ):
        super().__init__(message)
        self.status = status
        self.body = body
        self.params = params or {}


def _is_interval_too_long(error: BadRequestError) -> bool:
    body = (error.body or "").lower()
    return "interval is too long" in body


def _response_status_and_body(exc) -> Tuple[Optional[int], str]:
    """Safely pull status_code/text from an exception with a response attr."""
    resp = getattr(exc, "response", None)
    if resp is None:
        return None, ""
    status = getattr(resp, "status_code", None)
    body = getattr(resp, "text", "") or ""
    return status, str(body)[:500]


def _is_transient_client_status(status: Optional[int]) -> bool:
    # 429s are retryable/rate-limit responses and should not permanently skip tokens.
    return status == 429


def set_status_hook(hook: Optional[Callable[[int], None]]) -> None:
    """Set an optional status callback used for sync telemetry/autotuning."""
    global _status_hook
    _status_hook = hook


def _emit_status(status: int) -> None:
    hook = _status_hook
    if hook is None:
        return
    try:
        hook(int(status))
    except Exception:
        # Telemetry should not interfere with data flow.
        logger.debug("Ignoring status hook failure", exc_info=True)


def _emit_status_via(hook: Optional[Callable[[int], None]], status: int) -> None:
    """Emit status to an explicit hook first, then optional module-level hook."""
    if hook is not None:
        try:
            hook(int(status))
        except Exception:
            logger.debug("Ignoring explicit status hook failure", exc_info=True)
    _emit_status(status)


def build_client(base_url: str, *, rate_limiter=None) -> APIClient:
    """
    Construct an API client configured for the given base URL.

    rate_limiter is shared by the orchestrator to coordinate request pacing
    across worker threads.
    """
    return APIClient(
        base_url=base_url,
        requests_per_second=None,  # rely on shared limiter
        rate_limiter=rate_limiter,
        request_timeout=HTTP_REQUEST_TIMEOUT,
    )


def fetch_token_history(
    client: APIClient,
    token_id: str,
    interval: str = "1d",
    start_ts: int | None = None,
    end_ts: int | None = None,
    fidelity: int | None = None,
    status_hook: Optional[Callable[[int], None]] = None,
) -> Optional[List[Tuple]]:
    """
    Fetch and process history for a single token.

    According to Polymarket API docs:
    https://docs.polymarket.com/api-reference/pricing/get-price-history-for-a-traded-token

    Args:
        client: APIClient instance
        token_id: The CLOB token ID
        interval: Duration string (1m, 1h, 6h, 1d, 1w, max). Mutually exclusive with startTs/endTs
        start_ts: Start time as Unix timestamp (UTC). Requires end_ts.
        end_ts: End time as Unix timestamp (UTC). Requires start_ts.
        fidelity: Resolution of data in minutes (optional)

    Returns:
        List[Tuple]: Records if successful (token_id, timestamp, price)
        None: If an error occurred
    """
    try:
        params = {"market": token_id}

        # Use interval OR explicit time range (mutually exclusive per docs)
        if start_ts and end_ts:
            params["startTs"] = start_ts
            params["endTs"] = end_ts
        else:
            params["interval"] = interval

        # Add fidelity if specified (resolution in minutes)
        if fidelity:
            params["fidelity"] = fidelity

        data = clob_get(client, "/prices-history", params=params)
        _emit_status_via(status_hook, 200)
        history = data.get("history", [])

        if not history:
            logger.debug(f"No history found for token {token_id}")
            return []

        records: List[Tuple] = []
        for point in history:
            if "t" in point and "p" in point:
                records.append((token_id, point["t"], point["p"]))

        return records
    except (requests.Timeout, ClobRequestError, requests.HTTPError) as e:
        if isinstance(e, requests.Timeout) or (
            isinstance(e, ClobRequestError)
            and isinstance(e.__cause__, requests.Timeout)
        ):
            _emit_status_via(status_hook, -1)
            logger.warning(
                "Timeout fetching token %s with params=%s; will retry on next sync",
                token_id,
                params,
            )
            return None
        status, body = _response_status_and_body(e)
        _emit_status_via(status_hook, status if status is not None else -1)
        if _is_transient_client_status(status):
            logger.warning(
                "Transient client status %s for token %s; will retry on next sync",
                status,
                token_id,
            )
            return None
        if status == 400:
            raise BadRequestError(
                f"{status} bad request for token {token_id}: {body}",
                status=status,
                body=body,
                params=params,
            ) from e
        if status and 400 <= status < 500:
            raise PermanentAPIError(
                f"{status} client error for token {token_id}: {body}"
            ) from e
        logger.error(f"Failed to process token {token_id}: {e}")
        return None
    except OSError as e:
        logger.error(f"Failed to process token {token_id}: {e}")
        return None
    except Exception as e:
        status, body = _response_status_and_body(e)
        if getattr(e, "response", None) is None:
            logger.error(f"Failed to process token {token_id}: {e}")
            return None
        _emit_status_via(status_hook, status if status is not None else -1)
        if _is_transient_client_status(status):
            logger.warning(
                "Transient client status %s for token %s; will retry on next sync",
                status,
                token_id,
            )
            return None
        if status == 400:
            raise BadRequestError(
                f"{status} bad request for token {token_id}: {body}",
                status=status,
                body=body,
                params=params,
            ) from e
        if status and 400 <= status < 500:
            raise PermanentAPIError(
                f"{status} client error for token {token_id}: {body}"
            ) from e
        logger.error(f"Failed to process token {token_id}: {e}")
        return None


def fetch_token_history_with_retry(
    client,
    token_id: str,
    *,
    interval: str = "1d",
    start_ts: int | None = None,
    end_ts: int | None = None,
    fidelity: int | None = None,
    now_ts: int | None = None,
    transient_retries: int = 0,
    transient_backoff_base_seconds: float = 0.0,
    status_hook: Optional[Callable[[int], None]] = None,
) -> Optional[List[Tuple]]:
    """
    Fetch token history with a retry path for 400s using adjusted params.
    """
    transient_retries = max(0, int(transient_retries))
    transient_backoff_base_seconds = max(0.0, float(transient_backoff_base_seconds))

    def _call_with_transient_retry(callable_fetch) -> Optional[List[Tuple]]:
        for attempt in range(transient_retries + 1):
            result = callable_fetch()
            if result is not None:
                return result
            if attempt < transient_retries:
                # jittered exponential backoff for transient errors (e.g., 429/5xx)
                sleep_for = transient_backoff_base_seconds * (2**attempt)
                if sleep_for > 0:
                    time.sleep(random.uniform(0.5, 1.5) * sleep_for)
        return None

    try:
        return _call_with_transient_retry(
            lambda: fetch_token_history(
                client,
                token_id,
                interval=interval,
                start_ts=start_ts,
                end_ts=end_ts,
                fidelity=fidelity,
                status_hook=status_hook,
            )
        )
    except BadRequestError as exc:
        last_error = exc
        if _is_interval_too_long(last_error):
            raise last_error

    # Retry path for explicit ranges: drop fidelity, clamp end to now
    if start_ts is not None and end_ts is not None:
        if now_ts is None:
            now_ts = int(time.time())
        adjusted_end = min(end_ts, now_ts)
        if adjusted_end <= start_ts:
            raise last_error
        try:
            return _call_with_transient_retry(
                lambda: fetch_token_history(
                    client,
                    token_id,
                    start_ts=start_ts,
                    end_ts=adjusted_end,
                    fidelity=None,
                    status_hook=status_hook,
                )
            )
        except BadRequestError as exc:
            if _is_interval_too_long(exc):
                raise exc
            raise exc from last_error

    # Retry path for interval fetches: drop fidelity, then fall back to max
    try:
        return _call_with_transient_retry(
            lambda: fetch_token_history(
                client,
                token_id,
                interval=interval,
                fidelity=None,
                status_hook=status_hook,
            )
        )
    except BadRequestError as exc:
        if _is_interval_too_long(exc):
            raise exc
        last_error = exc

    if interval != "max":
        try:
            return _call_with_transient_retry(
                lambda: fetch_token_history(
                    client,
                    token_id,
                    interval="max",
                    fidelity=None,
                    status_hook=status_hook,
                )
            )
        except BadRequestError as exc:
            raise exc from last_error

    raise last_error
