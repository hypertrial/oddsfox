from datetime import datetime, timezone

import polars as pl

from oddsfox.ingestion.polymarket.markets import fetch, transform
from oddsfox.ingestion.polymarket.markets import sync as markets_sync
from oddsfox.ingestion.polymarket.markets.persistence import (
    prepare_batch_for_db,
)
from oddsfox.ingestion.polymarket.wc2026_scope import Wc2026ScopeConfig

_SLUG_ONLY_CFG = Wc2026ScopeConfig(
    event_slugs=("2026-fifa-world-cup-winner",),
    event_slug_prefixes=("2026-fifa-world-cup",),
    market_ids=(),
    registry_max_event_pages=None,
    event_tags=(),
)


def test_extract_event_id_variants():
    assert transform.extract_event_id(None) is None
    assert transform.extract_event_id([]) is None
    assert transform.extract_event_id([{}]) is None
    assert transform.extract_event_id([{"id": 42}]) == "42"
    assert transform.extract_event_id([{"id": None}]) is None
    assert transform.extract_event_id([["not-a-dict"]]) is None


def test_extract_event_slug_variants():
    assert transform.extract_event_slug(None) is None
    assert transform.extract_event_slug([]) is None
    assert transform.extract_event_slug([{"slug": "e"}]) == "e"
    assert transform.extract_event_slug("x") is None
    assert transform.extract_event_slug([["not", "a", "dict"]]) is None


def test_process_markets_dataframe_empty():
    assert transform.process_markets_dataframe([]).is_empty()


def test_process_markets_dataframe_shapes():
    markets = [
        {
            "id": "1",
            "question": "q",
            "category": "c",
            "description": "d",
            "outcomes": ["Y", "N"],
            "volumeNum": 1.5,
            "active": True,
            "closed": False,
            "createdAt": "2024-01-15T12:00:00.000Z",
            "endDate": "2024-06-01T00:00:00.000Z",
            "clobTokenIds": ["t1", "t2"],
            "slug": "s",
            "events": [{"slug": "ev"}],
        },
        "not-a-dict",
    ]
    df = transform.process_markets_dataframe(markets)
    assert df.height == 1
    assert df["outcomes_str"][0] == '["Y", "N"]'
    assert df["clobTokenIds_str"][0] == '["t1", "t2"]'
    assert df["event_slug"][0] == "ev"


def test_prepare_batch_for_db_empty():
    assert prepare_batch_for_db(pl.DataFrame()) == ([], [])


def test_prepare_batch_for_db_column_variants():
    df = pl.DataFrame(
        {
            "id": ["1"],
            "question": ["q"],
            "category": ["c"],
            "description": ["d"],
            "outcomes_str": ["[]"],
            "volumeNum": [2.0],
            "active": [True],
            "closed": [False],
            "created_at": [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            "end_date": [datetime(2024, 2, 1, tzinfo=timezone.utc)],
            "slug": ["sl"],
            "event_slug": ["es"],
            "clobTokenIds_str": ['["a"]'],
        }
    )
    m, t = prepare_batch_for_db(df)
    assert m and t


def test_build_client():
    c = fetch.build_client(requests_per_second=2)
    assert c.base_url.endswith("gamma-api.polymarket.com")
    assert c.rate_limiter is not None
    assert c.rate_limiter.get_rate() == 2.0


def _event_market(market_id: str = "m1") -> dict:
    return {
        "id": market_id,
        "question": "Will the World Cup 2026 event-first sync pass?",
        "category": "World Cup 2026 Testing",
        "description": "Synthetic World Cup 2026 market",
        "outcomes": ["Yes", "No"],
        "volumeNum": 1,
        "active": True,
        "closed": False,
        "createdAt": "2024-01-01T00:00:00.000Z",
        "endDate": "2026-07-19T00:00:00.000Z",
        "clobTokenIds": ["t1", "t2"],
        "slug": "world-cup-2026-event-first-sync-pass",
        "events": [{"slug": "2026-fifa-world-cup-winner-595", "id": "ev1"}],
    }


def test_sync_markets_targeted_saves_tokens(monkeypatch):
    saved: list = []
    progress = []

    monkeypatch.setattr(markets_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(markets_sync, "load_wc2026_config", lambda: _SLUG_ONLY_CFG)
    monkeypatch.setattr(markets_sync, "save_sync_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(
        markets_sync,
        "refresh_registry_and_collect_markets_targeted",
        lambda client, config, progress_callback=None: (
            {"registry_rows_upserted": 1, "registry_refreshed": True},
            [_event_market()],
            {
                "events_pages": 0,
                "markets_collected": 1,
                "registry_refreshed": True,
                "api_requests": 2,
            },
        ),
    )
    monkeypatch.setattr(
        markets_sync,
        "save_market_tokens_batch",
        lambda token_data: saved.extend(token_data),
    )

    out = markets_sync.sync_markets(
        client_factory=lambda: object(),
        discovery_mode="targeted",
        progress_callback=lambda phase, payload: progress.append((phase, payload)),
        progress_log_interval_pages=1,
    )
    assert out["mode"] == "wc2026_event_first"
    assert out["discovery_mode"] == "targeted"
    assert out["total_fetched"] == 1
    assert out["registry_refreshed"] is True
    assert saved
    assert any(phase == "discovery_complete" for phase, _ in progress)


def test_sync_markets_targeted_empty_events(monkeypatch):
    saved = []

    monkeypatch.setattr(markets_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(markets_sync, "load_wc2026_config", lambda: _SLUG_ONLY_CFG)
    monkeypatch.setattr(markets_sync, "save_sync_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(
        markets_sync,
        "refresh_registry_and_collect_markets_targeted",
        lambda client, config, progress_callback=None: (
            {"registry_rows_upserted": 0},
            [],
            {"events_pages": 0, "markets_collected": 0, "registry_refreshed": True},
        ),
    )
    monkeypatch.setattr(
        markets_sync,
        "save_market_tokens_batch",
        lambda token_data: saved.append(token_data),
    )

    out = markets_sync.sync_markets(
        client_factory=lambda: object(),
        discovery_mode="targeted",
    )

    assert out["mode"] == "wc2026_event_first"
    assert out["total_fetched"] == 0
    assert saved == []


def test_sync_markets_full_keyset_routing(monkeypatch):
    called = {}

    monkeypatch.setattr(markets_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(markets_sync, "load_wc2026_config", lambda: _SLUG_ONLY_CFG)
    monkeypatch.setattr(markets_sync, "save_sync_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(
        markets_sync,
        "refresh_registry_and_collect_markets_from_events",
        lambda client, config, progress_callback=None, **kwargs: (
            {"registry_rows_upserted": 0},
            [],
            {"events_pages": 3, "markets_collected": 0, "registry_refreshed": True},
        ),
    )
    monkeypatch.setattr(
        markets_sync,
        "refresh_registry_and_collect_markets_targeted",
        lambda *a, **k: called.setdefault("targeted", True) or ([], [], {}),
    )

    out = markets_sync.sync_markets(
        client_factory=lambda: object(),
        discovery_mode="full_keyset",
    )
    assert out["discovery_mode"] == "full_keyset"
    assert "targeted" not in called

    out_default = markets_sync.sync_markets(client_factory=lambda: object())
    assert out_default["discovery_mode"] == "full_keyset"

    out = markets_sync.sync_markets(
        client_factory=lambda: object(),
        force_full_discovery=True,
    )
    assert out["discovery_mode"] == "full_keyset"


def test_sync_markets_ignores_progress_callback_failures(monkeypatch):
    monkeypatch.setattr(markets_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(markets_sync, "load_wc2026_config", lambda: _SLUG_ONLY_CFG)
    monkeypatch.setattr(markets_sync, "save_sync_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(
        markets_sync,
        "refresh_registry_and_collect_markets_targeted",
        lambda client, config, progress_callback=None: (
            {"registry_rows_upserted": 1},
            [_event_market()],
            {"events_pages": 0, "markets_collected": 1, "registry_refreshed": True},
        ),
    )
    monkeypatch.setattr(markets_sync, "save_market_tokens_batch", lambda *a, **k: None)

    out = markets_sync.sync_markets(
        client_factory=lambda: object(),
        discovery_mode="targeted",
        progress_callback=lambda phase, payload: (_ for _ in ()).throw(
            RuntimeError("callback failed")
        ),
        progress_log_interval_pages=1,
    )

    assert out["aborted"] is False
    assert out["error"] is None


def test_sync_markets_guardrail_check_during_discovery_progress(monkeypatch):
    from oddsfox.resources.progress_guardrails import ProgressGuardrail

    checks: list[tuple[str, dict]] = []
    original_check = ProgressGuardrail.check

    def recording_check(self, *, phase, diagnostics):
        checks.append((phase, diagnostics))
        return original_check(self, phase=phase, diagnostics=diagnostics)

    monkeypatch.setattr(ProgressGuardrail, "check", recording_check)
    monkeypatch.setattr(markets_sync, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(markets_sync, "load_wc2026_config", lambda: _SLUG_ONLY_CFG)
    monkeypatch.setattr(markets_sync, "save_sync_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(markets_sync, "save_market_tokens_batch", lambda *a, **k: None)

    def fake_refresh(client, config, progress_callback=None):
        del client, config
        if progress_callback:
            progress_callback(
                "discovery_page",
                {"events_pages": 2, "api_requests": 3, "markets_collected": 1},
            )
        return (
            {"registry_rows_upserted": 1, "registry_refreshed": True},
            [_event_market()],
            {
                "events_pages": 2,
                "markets_collected": 1,
                "registry_refreshed": True,
                "api_requests": 3,
            },
        )

    monkeypatch.setattr(
        markets_sync,
        "refresh_registry_and_collect_markets_targeted",
        fake_refresh,
    )

    markets_sync.sync_markets(
        client_factory=lambda: object(),
        discovery_mode="targeted",
        progress_log_interval_pages=1,
    )

    assert any(phase == "discovery_page" for phase, _ in checks)
