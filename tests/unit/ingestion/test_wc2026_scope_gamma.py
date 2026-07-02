"""Unit tests for WC2026 scope gamma helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from tests.unit.ingestion.wc2026_scope_test_support import slug_only_cfg

from oddsfox.ingestion.polymarket.wc2026_scope import (
    collect_wc2026_markets_from_events,
    refresh_registry_from_events,
)
from oddsfox.ingestion.polymarket.wc2026_scope import (
    gamma as scope_gamma_mod,
)


def test_iter_wc2026_gamma_events_skips_non_allowlisted(monkeypatch, tmp_path):
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "event_first.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)

    client = MagicMock()
    client.get.side_effect = [
        {
            "events": [
                {
                    "id": "ev1",
                    "slug": "2026-fifa-world-cup-winner-595",
                    "markets": [{"id": "m1"}],
                },
            ],
            "next_cursor": "page2",
        },
        {
            "events": [
                {
                    "id": "ev2",
                    "slug": "other-league-2026",
                    "markets": [{"id": "m2"}],
                },
            ],
            "next_cursor": None,
        },
    ]
    markets, meta = collect_wc2026_markets_from_events(
        client,
        config=slug_only_cfg(),
        max_pages=10,
        tag_discovery=False,
    )
    assert len(markets) == 1
    assert markets[0]["id"] == "m1"
    assert meta["events_pages"] == 2


def test_gamma_events_keyset_shared_pagination_params(monkeypatch, tmp_path):
    """WC2026 scan and event-slug fallback use the same /events/keyset pagination."""
    import importlib

    import oddsfox.storage.duckdb.connection as connection
    from oddsfox.config._reload_settings import reload_all_settings_modules
    from oddsfox.ingestion.polymarket.markets.backfill._events_fallback import (
        _fill_from_events_endpoint,
    )

    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "shared_pagination.duckdb"))
    reload_all_settings_modules()
    connection.reset_duckdb_connection_state()
    importlib.reload(connection)
    connection.ensure_duck_db()

    page1 = {
        "events": [
            {
                "id": "ev1",
                "slug": "2026-fifa-world-cup-winner-595",
                "markets": [{"id": "m1"}],
            },
            {"id": "ev2", "slug": "other", "markets": [{"id": "m2"}]},
        ],
        "next_cursor": "c2",
    }
    page2 = {"events": [], "next_cursor": None}
    client = MagicMock()
    client.get.side_effect = [page1, page2, page1, page2]

    cfg = slug_only_cfg()
    refresh_registry_from_events(
        client,
        config=cfg,
        max_pages=10,
        tag_discovery=False,
        keyset_related_tags=False,
    )
    wc_calls = [
        c.kwargs.get("params") or {}
        for c in client.get.call_args_list
        if c.args and str(c.args[0]).endswith("/events/keyset")
    ][:2]

    client.get.reset_mock()
    client.get.side_effect = [page1, page2]
    saved, _meta = _fill_from_events_endpoint(client, {"m99"}, max_pages=10)
    assert saved == 0
    fb_calls = [c.kwargs.get("params") or {} for c in client.get.call_args_list]

    assert len(wc_calls) >= 1 and len(fb_calls) >= 1
    assert wc_calls[0].get("limit") == fb_calls[0].get("limit") == 500
    assert wc_calls[0].get("closed") is False
    assert wc_calls[0].get("volume_min") == 10000
    assert wc_calls[1].get("next_cursor") == "c2"
    assert fb_calls[1].get("next_cursor") == "c2"
    assert "closed" not in fb_calls[0]


def test_fetch_gamma_event_by_slug_handles_missing(monkeypatch):
    from oddsfox.ingestion.polymarket.errors import GammaRequestError
    from oddsfox.ingestion.polymarket.gamma_events import (
        fetch_gamma_event_by_slug,
    )

    client = MagicMock()
    response = MagicMock()
    response.status_code = 404
    client.get.side_effect = GammaRequestError("missing", response=response)
    assert fetch_gamma_event_by_slug(client, "missing-slug") is None
    assert fetch_gamma_event_by_slug(client, "  ") is None

    client.get.reset_mock()
    client.get.side_effect = None
    client.get.return_value = {"slug": "x"}
    assert fetch_gamma_event_by_slug(client, "empty-id") is None

    client.get.return_value = {"id": "1", "slug": "ok-slug"}
    assert fetch_gamma_event_by_slug(client, "ok-slug")["id"] == "1"


def test_fetch_gamma_event_by_slug_reraises_non_404():
    from oddsfox.ingestion.polymarket.errors import GammaRequestError
    from oddsfox.ingestion.polymarket.gamma_events import (
        fetch_gamma_event_by_slug,
    )

    client = MagicMock()
    response = MagicMock()
    response.status_code = 500
    client.get.side_effect = GammaRequestError("server error", response=response)
    with pytest.raises(GammaRequestError):
        fetch_gamma_event_by_slug(client, "some-slug")


def test_fetch_gamma_event_by_slug_reraises_transport_errors(monkeypatch):
    import requests

    from oddsfox.ingestion.polymarket.gamma_events import (
        fetch_gamma_event_by_slug,
    )

    client = MagicMock()
    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.gamma_events.gamma_get",
        lambda *a, **k: (_ for _ in ()).throw(requests.ConnectionError("down")),
    )
    with pytest.raises(requests.ConnectionError):
        fetch_gamma_event_by_slug(client, "some-slug")


def test_gamma_market_id_filter_and_resilient_fetch():
    from oddsfox.ingestion.polymarket.errors import GammaRequestError

    assert scope_gamma_mod._is_gamma_market_id("253591") is True
    assert scope_gamma_mod._is_gamma_market_id("m1") is False
    assert scope_gamma_mod._gamma_market_ids(["253591", "m1", "m2"]) == ["253591"]

    client = MagicMock()
    client.get.side_effect = [
        GammaRequestError("batch", response=MagicMock(status_code=422)),
        GammaRequestError("bad", response=MagicMock(status_code=422)),
        [{"id": "1"}],
    ]
    rows = scope_gamma_mod._fetch_markets_batch_resilient(client, ["bad", "1"])
    assert len(rows) == 1
    assert rows[0]["id"] == "1"
    assert scope_gamma_mod._fetch_markets_batch_resilient(client, []) == []

    client.get.side_effect = GammaRequestError(
        "server", response=MagicMock(status_code=500)
    )
    with pytest.raises(GammaRequestError):
        scope_gamma_mod._fetch_markets_batch_resilient(client, ["1"])
