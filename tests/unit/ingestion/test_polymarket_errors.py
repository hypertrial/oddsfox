"""Polymarket typed errors and narrowed exception handling."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import requests

from oddsfox.ingestion.polymarket.errors import (
    ClobRequestError,
    GammaRequestError,
    PolymarketIngestionError,
)
from oddsfox.ingestion.polymarket.markets.backfill._gamma import (
    _process_market_chunks,
)
from oddsfox.resources.http import APIClient


def test_polymarket_error_taxonomy() -> None:
    assert issubclass(GammaRequestError, PolymarketIngestionError)
    assert issubclass(ClobRequestError, requests.RequestException)


def test_gamma_get_wraps_connection_error() -> None:
    from oddsfox.ingestion.polymarket.errors import gamma_get

    client = MagicMock(spec=APIClient)
    client.get.side_effect = requests.ConnectionError("gamma down")
    try:
        gamma_get(client, "/markets")
    except GammaRequestError as exc:
        assert isinstance(exc.__cause__, requests.ConnectionError)
    else:
        raise AssertionError("expected GammaRequestError")


def test_backfill_batch_continues_after_request_error() -> None:
    client = MagicMock(spec=APIClient)
    client.get.side_effect = requests.ConnectionError("gamma down")
    saved_batches: list[list] = []

    def _save(batch: list) -> None:
        saved_batches.append(batch)

    processed, saved, api_requests = _process_market_chunks(
        client=client,
        market_ids=["m1", "m2"],
        batch_size=10,
        desc="test",
        include_events=False,
        extract_record=lambda _mid, _m: ("a", "b"),
        save_batch=_save,
        count_errors_as_processed=True,
    )
    assert processed == 2
    assert saved == 0
    assert api_requests == 1
    assert saved_batches == []


def test_gamma_get_reraises_existing_gamma_request_error() -> None:
    from oddsfox.ingestion.polymarket.errors import gamma_get

    client = MagicMock(spec=APIClient)
    original = GammaRequestError("already wrapped")
    client.get.side_effect = original
    with pytest.raises(GammaRequestError) as exc_info:
        gamma_get(client, "/markets")
    assert exc_info.value is original


def test_wrap_request_error_copies_request_attribute() -> None:
    from oddsfox.ingestion.polymarket.errors import _wrap_request_error

    req = requests.Request("GET", "http://example.com")
    exc = requests.ConnectionError("down")
    exc.request = req
    wrapped = _wrap_request_error(exc, GammaRequestError)
    assert wrapped.request is req
