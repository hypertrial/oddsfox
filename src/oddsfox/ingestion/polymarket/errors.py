"""Typed errors for Polymarket ingestion (Gamma + CLOB)."""

from __future__ import annotations

from typing import Any

import requests


class PolymarketIngestionError(Exception):
    """Base error for Polymarket ingest paths."""


class GammaRequestError(PolymarketIngestionError, requests.RequestException):
    """Gamma API request failed."""


class ClobRequestError(PolymarketIngestionError, requests.RequestException):
    """CLOB API request failed."""


def _wrap_request_error(
    exc: requests.RequestException,
    error_cls: type[GammaRequestError | ClobRequestError],
) -> GammaRequestError | ClobRequestError:
    if isinstance(exc, error_cls):
        return exc
    wrapped = error_cls(*exc.args)
    wrapped.__cause__ = exc
    if exc.response is not None:
        wrapped.response = exc.response
    if exc.request is not None:
        wrapped.request = exc.request
    return wrapped


def gamma_get(client: Any, endpoint: str, **kwargs: Any) -> Any:
    """Gamma HTTP GET with typed transport errors."""
    try:
        return client.get(endpoint, **kwargs)
    except requests.RequestException as exc:
        raise _wrap_request_error(exc, GammaRequestError) from exc


def clob_get(client: Any, endpoint: str, **kwargs: Any) -> Any:
    """CLOB HTTP GET with typed transport errors."""
    try:
        return client.get(endpoint, **kwargs)
    except requests.RequestException as exc:
        raise _wrap_request_error(exc, ClobRequestError) from exc
