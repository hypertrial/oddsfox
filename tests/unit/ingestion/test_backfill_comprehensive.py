"""Drive remaining branches in markets/backfill/."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from tests.unit.ingestion.backfill_test_support import (
    bf_end_dates,
    bf_event_slugs,
    bf_events_fallback,
    bf_gamma,
    bf_slugs,
    bf_tokens,
)

from oddsfox.ingestion.polymarket.markets import backfill as bf


@pytest.fixture
def no_sleep_tqdm(monkeypatch):
    monkeypatch.setattr(
        bf,
        "tqdm",
        lambda *a, **k: MagicMock(__enter__=lambda s: s, __exit__=lambda *x: None),
    )
    monkeypatch.setattr(bf_gamma.time, "sleep", lambda s: None)
    monkeypatch.setattr(bf_events_fallback.time, "sleep", lambda s: None)


def test_extract_tokens_json_string_success():
    raw = json.dumps(["a", "b"])
    assert bf._extract_tokens_record("1", {"clobTokenIds": raw}) == ("1", raw)


def test_extract_tokens_invalid_list_type():
    assert bf._extract_tokens_record("1", {"clobTokenIds": 123}) is None


def test_extract_tokens_invalid_json_and_empty_list():
    assert bf._extract_tokens_record("1", {"clobTokenIds": "{bad"}) is None
    assert bf._extract_tokens_record("1", {"clobTokenIds": []}) is None


def test_extract_event_slug_non_list_events():
    assert bf._extract_event_slug_record("1", {"events": "x"}) is None


def test_extract_event_slug_bad_first_event():
    assert bf._extract_event_slug_record("1", {"events": [123]}) is None


def test_extract_end_date_iso_alt():
    assert (
        bf._extract_end_date_record("1", {"endDateIso": "2020-01-01"})[0]
        == "2020-01-01"
    )


def test_extract_end_date_missing():
    assert bf._extract_end_date_record("1", {}) is None


def test_extract_slug_empty():
    assert bf._extract_slug_record("1", {"slug": ""}) is None


def test_extract_event_slug_empty_slug():
    assert bf._extract_event_slug_record("1", {"events": [{"slug": ""}]}) is None


def test_process_market_chunks_with_records_and_on_saved(no_sleep_tqdm, monkeypatch):
    monkeypatch.setattr(bf, "ensure_duck_db", lambda: None)
    saved_ids = []

    def on_saved(mid):
        saved_ids.append(mid)

    client = MagicMock()
    client.get.return_value = [{"id": "1", "clobTokenIds": ["x"]}]
    processed, saved, _api = bf._process_market_chunks(
        client=client,
        market_ids=["1"],
        batch_size=1,
        desc="t",
        include_events=False,
        extract_record=bf._extract_tokens_record,
        save_batch=lambda x: None,
        on_record_saved=on_saved,
    )
    assert processed >= 1


def test_process_market_chunks_record_none_without_callback(no_sleep_tqdm, monkeypatch):
    monkeypatch.setattr(bf, "ensure_duck_db", lambda: None)
    client = MagicMock()
    client.get.return_value = [{"id": "1", "slug": ""}]
    processed, saved, _api = bf._process_market_chunks(
        client=client,
        market_ids=["1"],
        batch_size=1,
        desc="t",
        include_events=False,
        extract_record=bf._extract_slug_record,
        save_batch=lambda x: (_ for _ in ()).throw(RuntimeError("should not save")),
    )
    assert processed == 1
    assert saved == 0


def test_process_market_chunks_record_saved_without_on_saved(
    no_sleep_tqdm, monkeypatch
):
    monkeypatch.setattr(bf, "ensure_duck_db", lambda: None)
    saved_batches = []
    client = MagicMock()
    client.get.return_value = [{"id": "1", "slug": "slug-1"}]
    processed, saved, _api = bf._process_market_chunks(
        client=client,
        market_ids=["1"],
        batch_size=1,
        desc="t",
        include_events=False,
        extract_record=bf._extract_slug_record,
        save_batch=lambda rows: saved_batches.extend(rows),
    )
    assert processed == 1
    assert saved == 1
    assert saved_batches == [("1", "slug-1")]


def test_process_market_chunks_count_errors_request_exception(
    no_sleep_tqdm, monkeypatch
):
    import requests

    monkeypatch.setattr(bf, "ensure_duck_db", lambda: None)
    client = MagicMock()
    client.get.side_effect = requests.RequestException("gamma down")
    processed, saved, _api = bf._process_market_chunks(
        client=client,
        market_ids=["1"],
        batch_size=1,
        desc="t",
        include_events=False,
        extract_record=bf._extract_tokens_record,
        save_batch=lambda x: None,
        count_errors_as_processed=True,
    )
    assert processed >= 1
    processed2, _, _ = bf._process_market_chunks(
        client=client,
        market_ids=["1"],
        batch_size=1,
        desc="t",
        include_events=False,
        extract_record=bf._extract_tokens_record,
        save_batch=lambda x: None,
        count_errors_as_processed=False,
    )
    assert processed2 == 0


def test_process_market_chunks_count_errors(no_sleep_tqdm, monkeypatch):
    monkeypatch.setattr(bf, "ensure_duck_db", lambda: None)
    client = MagicMock()
    client.get.side_effect = RuntimeError("api")
    processed, saved, _api = bf._process_market_chunks(
        client=client,
        market_ids=["1", "2"],
        batch_size=2,
        desc="t",
        include_events=False,
        extract_record=bf._extract_tokens_record,
        save_batch=lambda x: None,
        count_errors_as_processed=True,
    )
    assert processed >= 2


def test_fill_from_events_empty_remaining():
    c = MagicMock()
    saved, meta = bf._fill_from_events_endpoint(c, set())
    assert saved == 0
    assert meta["events_fallback_pages"] == 0


def test_fill_from_events_pagination(no_sleep_tqdm, monkeypatch):
    c = MagicMock()
    c.get.side_effect = [
        [
            {
                "slug": "es",
                "markets": [{"id": "99"}],
            }
        ],
        [],
    ]
    remaining = {"99"}
    with patch.object(bf_events_fallback, "save_event_slugs_batch", lambda x: None):
        n, meta = bf._fill_from_events_endpoint(c, remaining)
    assert n >= 1
    assert meta["events_fallback_truncated"] is False


def test_fill_from_events_skip_bad_slug(no_sleep_tqdm, monkeypatch):
    c = MagicMock()
    c.get.side_effect = [
        [{"slug": None, "markets": [{"id": "1"}]}],
        [{"slug": "s", "markets": []}],
        [],
    ]
    with patch.object(bf_events_fallback, "save_event_slugs_batch", lambda x: None):
        bf._fill_from_events_endpoint(c, {"1"})


def test_fill_from_events_unmatched_market_keeps_remaining(no_sleep_tqdm):
    c = MagicMock()
    c.get.side_effect = [[{"slug": "s", "markets": [{"id": "other"}]}], []]
    remaining = {"1"}
    with patch.object(bf_events_fallback, "save_event_slugs_batch", lambda x: None):
        saved, meta = bf._fill_from_events_endpoint(c, remaining)
    assert saved == 0
    assert remaining == {"1"}
    assert meta["events_fallback_remaining_ids"] == 1


def test_backfill_tokens_early_and_empty(monkeypatch):
    monkeypatch.setattr(bf_tokens, "get_backfill_fully_checked", lambda t: True)
    bf.backfill_tokens(force=False)

    monkeypatch.setattr(bf_tokens, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_tokens, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(bf_tokens, "get_markets_without_tokens", lambda limit=None: [])
    monkeypatch.setattr(bf_tokens, "set_backfill_fully_checked", lambda *a: None)
    bf.backfill_tokens(max_markets=None, force=True)


def test_backfill_slugs_early(monkeypatch):
    monkeypatch.setattr(bf_slugs, "get_backfill_fully_checked", lambda t: True)
    bf.backfill_slugs(force=False)


def test_backfill_end_dates_early(monkeypatch):
    monkeypatch.setattr(bf_end_dates, "get_backfill_fully_checked", lambda t: True)
    bf.backfill_end_dates(force=False)


def test_backfill_event_slugs_early_and_progress(monkeypatch, no_sleep_tqdm):
    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: True)
    bf.backfill_event_slugs(force=False)

    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_event_slugs, "get_markets_without_event_slugs", lambda limit=None: []
    )
    monkeypatch.setattr(bf_event_slugs, "set_backfill_fully_checked", lambda *a: None)
    monkeypatch.setattr(bf_event_slugs, "set_backfill_progress", lambda *a: None)
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: 0)
    bf.backfill_event_slugs(max_markets=None, force=True)

    gm_calls = []

    def get_event_slugs_twice(limit=None):
        gm_calls.append(limit)
        if len(gm_calls) == 1:
            return ["1", "2"]
        return []

    monkeypatch.setattr(
        bf_event_slugs, "get_markets_without_event_slugs", get_event_slugs_twice
    )
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: 5)

    def proc(**kw):
        for mid in kw["market_ids"]:
            kw["on_record_saved"](mid)
        return (2, 2, 1)

    monkeypatch.setattr(bf_event_slugs, "_process_market_chunks", proc)
    monkeypatch.setattr(
        bf_event_slugs,
        "_fill_from_events_endpoint",
        lambda *a, **k: (
            0,
            {
                "events_fallback_pages": 0,
                "events_fallback_truncated": False,
                "events_fallback_remaining_ids": 0,
            },
        ),
    )
    monkeypatch.setattr(bf_gamma, "APIClient", lambda *a, **k: MagicMock())
    bf.backfill_event_slugs(max_markets=None, force=False)


def test_backfill_event_slugs_negative_progress_clamped(monkeypatch, no_sleep_tqdm):
    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    gm_calls = []

    def get_event_slugs_twice(limit=None):
        gm_calls.append(limit)
        if len(gm_calls) == 1:
            return ["1"]
        return []

    monkeypatch.setattr(
        bf_event_slugs, "get_markets_without_event_slugs", get_event_slugs_twice
    )
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: -5)

    def proc(**kw):
        for mid in kw["market_ids"]:
            kw["on_record_saved"](mid)
        return (1, 1, 1)

    monkeypatch.setattr(bf_event_slugs, "_process_market_chunks", proc)
    monkeypatch.setattr(
        bf_event_slugs,
        "_fill_from_events_endpoint",
        lambda *a, **k: (
            0,
            {
                "events_fallback_pages": 0,
                "events_fallback_truncated": False,
                "events_fallback_remaining_ids": 0,
            },
        ),
    )
    monkeypatch.setattr(bf_event_slugs, "set_backfill_progress", lambda *a: None)
    monkeypatch.setattr(bf_event_slugs, "set_backfill_fully_checked", lambda *a: None)
    monkeypatch.setattr(bf_gamma, "APIClient", lambda *a, **k: MagicMock())
    bf.backfill_event_slugs(max_markets=None, force=False)


def test_backfill_tokens_with_markets_finally(no_sleep_tqdm, monkeypatch):
    monkeypatch.setattr(bf_tokens, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_tokens, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_tokens, "get_markets_without_tokens", lambda limit=None: ["10"]
    )
    monkeypatch.setattr(bf_tokens, "set_backfill_fully_checked", lambda *a: None)

    client = MagicMock()
    client.get.return_value = [{"id": "10", "clobTokenIds": ["z"]}]
    with patch.object(bf_gamma, "APIClient", lambda *a, **k: client):
        with patch.object(bf_tokens, "_process_market_chunks", return_value=(1, 1, 1)):
            bf.backfill_tokens(batch_size=1, max_markets=1, force=True)


def test_backfill_slugs_run_finally(no_sleep_tqdm, monkeypatch):
    monkeypatch.setattr(bf_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_slugs, "get_markets_without_slugs", lambda limit=None: ["20"]
    )
    monkeypatch.setattr(bf_slugs, "set_backfill_fully_checked", lambda *a: None)
    client = MagicMock()
    client.get.return_value = [{"id": "20", "slug": "s"}]
    with patch.object(bf_gamma, "APIClient", lambda *a, **k: client):
        with patch.object(bf_slugs, "_process_market_chunks", return_value=(1, 1, 1)):
            bf.backfill_slugs(batch_size=1, max_markets=1, force=True)


def test_backfill_end_dates_run(no_sleep_tqdm, monkeypatch):
    monkeypatch.setattr(bf_end_dates, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_end_dates, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_end_dates, "get_markets_without_end_date", lambda limit=None: ["30"]
    )
    monkeypatch.setattr(bf_end_dates, "set_backfill_fully_checked", lambda *a: None)
    client = MagicMock()
    client.get.return_value = [{"id": "30", "endDate": "2025-01-01"}]
    with patch.object(bf_gamma, "APIClient", lambda *a, **k: client):
        with patch.object(
            bf_end_dates, "_process_market_chunks", lambda **kw: (1, 1, 1)
        ):
            bf.backfill_end_dates(batch_size=1, max_markets=1, force=True)


def test_backfill_end_dates_max_markets_flag(monkeypatch, no_sleep_tqdm):
    monkeypatch.setattr(bf_end_dates, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_end_dates, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_end_dates, "get_markets_without_end_date", lambda limit=None: ["40"]
    )
    monkeypatch.setattr(bf_end_dates, "set_backfill_fully_checked", lambda *a: None)
    with patch.object(bf_gamma, "APIClient", MagicMock):
        with patch.object(
            bf_end_dates, "_process_market_chunks", lambda **kw: (1, 0, 1)
        ):
            bf.backfill_end_dates(batch_size=1, max_markets=5, force=True)


def test_backfill_empty_partial_runs_do_not_mark_complete(monkeypatch):
    flags = []

    monkeypatch.setattr(bf_tokens, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_tokens, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_tokens,
        "set_backfill_fully_checked",
        lambda task, val: flags.append((task, val)),
    )
    monkeypatch.setattr(bf_tokens, "get_markets_without_tokens", lambda limit=None: [])
    bf.backfill_tokens(max_markets=1, force=True)

    monkeypatch.setattr(bf_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_slugs,
        "set_backfill_fully_checked",
        lambda task, val: flags.append((task, val)),
    )
    monkeypatch.setattr(bf_slugs, "get_markets_without_slugs", lambda limit=None: [])
    bf.backfill_slugs(max_markets=1, force=True)

    monkeypatch.setattr(bf_end_dates, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_end_dates, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_end_dates,
        "set_backfill_fully_checked",
        lambda task, val: flags.append((task, val)),
    )
    monkeypatch.setattr(
        bf_end_dates, "get_markets_without_end_date", lambda limit=None: []
    )
    bf.backfill_end_dates(max_markets=1, force=True)

    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_event_slugs,
        "set_backfill_fully_checked",
        lambda task, val: flags.append((task, val)),
    )
    monkeypatch.setattr(
        bf_event_slugs, "get_markets_without_event_slugs", lambda limit=None: []
    )
    monkeypatch.setattr(bf_event_slugs, "set_backfill_progress", lambda *a: None)
    bf.backfill_event_slugs(max_markets=1, force=True)

    assert flags == []


def test_backfill_event_slugs_stale_ledger_does_not_skip_work(
    monkeypatch, no_sleep_tqdm
):
    """Large saved progress must not skip unresolved rows (no list slicing resume)."""
    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    gm_calls = []

    def get_event_slugs_twice(limit=None):
        gm_calls.append(limit)
        if len(gm_calls) == 1:
            return ["a", "b"]
        return []

    monkeypatch.setattr(
        bf_event_slugs, "get_markets_without_event_slugs", get_event_slugs_twice
    )
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: 2)
    monkeypatch.setattr(bf_event_slugs, "set_backfill_progress", lambda *a: None)
    monkeypatch.setattr(bf_event_slugs, "set_backfill_fully_checked", lambda *a: None)
    chunks = {"n": 0}

    def proc(**kw):
        chunks["n"] += 1
        assert kw["market_ids"] == ["a", "b"]
        for mid in kw["market_ids"]:
            kw["on_record_saved"](mid)
        return (2, 2, 1)

    monkeypatch.setattr(bf_event_slugs, "_process_market_chunks", proc)

    def boom(*a, **k):
        raise RuntimeError("fallback should not run when all resolved")

    monkeypatch.setattr(bf_event_slugs, "_fill_from_events_endpoint", boom)
    monkeypatch.setattr(bf_gamma, "APIClient", lambda *a, **k: MagicMock())
    bf.backfill_event_slugs(max_markets=None, force=False)
    assert chunks["n"] == 1


def test_process_market_chunks_missing_returned_market(no_sleep_tqdm, monkeypatch):
    """Market id in chunk but not in API response — no record, still processed."""
    monkeypatch.setattr(bf, "ensure_duck_db", lambda: None)
    client = MagicMock()
    client.get.return_value = [{"id": "other", "clobTokenIds": ["x"]}]
    processed, saved, _api = bf._process_market_chunks(
        client=client,
        market_ids=["wanted"],
        batch_size=1,
        desc="t",
        include_events=False,
        extract_record=bf._extract_tokens_record,
        save_batch=lambda x: None,
    )
    assert processed >= 1


def test_backfill_event_slugs_fallback_finally(no_sleep_tqdm, monkeypatch):
    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: 0)
    monkeypatch.setattr(
        bf_event_slugs, "get_markets_without_event_slugs", lambda limit=None: ["50"]
    )
    monkeypatch.setattr(bf_event_slugs, "set_backfill_progress", lambda *a: None)
    monkeypatch.setattr(bf_event_slugs, "set_backfill_fully_checked", lambda *a: None)

    client = MagicMock()

    with patch.object(bf_gamma, "APIClient", lambda *a, **k: client):
        with patch.object(
            bf_event_slugs, "_process_market_chunks", return_value=(1, 0, 1)
        ):
            with patch.object(
                bf_event_slugs,
                "_fill_from_events_endpoint",
                return_value=(
                    0,
                    {
                        "events_fallback_pages": 0,
                        "events_fallback_truncated": False,
                        "events_fallback_remaining_ids": 0,
                    },
                ),
            ):
                bf.backfill_event_slugs(batch_size=1, max_markets=1, force=True)


def test_backfill_event_slugs_without_fallback_when_remaining_empty(
    monkeypatch, no_sleep_tqdm
):
    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: 0)
    gm_calls = []

    def get_event_slugs_twice(limit=None):
        gm_calls.append(limit)
        if len(gm_calls) == 1:
            return ["50"]
        return []

    monkeypatch.setattr(
        bf_event_slugs, "get_markets_without_event_slugs", get_event_slugs_twice
    )
    monkeypatch.setattr(bf_event_slugs, "set_backfill_progress", lambda *a: None)
    monkeypatch.setattr(bf_event_slugs, "set_backfill_fully_checked", lambda *a: None)
    client = MagicMock()
    with patch.object(bf_gamma, "APIClient", lambda *a, **k: client):
        with patch.object(
            bf_event_slugs,
            "_process_market_chunks",
            side_effect=lambda **kw: (kw["on_record_saved"]("50"), (1, 1, 1))[1],
        ):
            with patch.object(
                bf_event_slugs,
                "_fill_from_events_endpoint",
                side_effect=RuntimeError("should not run"),
            ):
                bf.backfill_event_slugs(batch_size=1, max_markets=None, force=False)


def test_backfill_event_slugs_incomplete_full_run_updates_progress(
    monkeypatch, no_sleep_tqdm
):
    progress = []
    flags = []
    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: 0)
    monkeypatch.setattr(
        bf_event_slugs, "get_markets_without_event_slugs", lambda limit=None: ["1", "2"]
    )
    monkeypatch.setattr(
        bf_event_slugs, "_process_market_chunks", lambda **kw: (1, 1, 1)
    )
    monkeypatch.setattr(
        bf_event_slugs,
        "_fill_from_events_endpoint",
        lambda *a, **k: (
            0,
            {
                "events_fallback_pages": 0,
                "events_fallback_truncated": False,
                "events_fallback_remaining_ids": 2,
            },
        ),
    )
    monkeypatch.setattr(
        bf_event_slugs,
        "set_backfill_progress",
        lambda task, value: progress.append((task, value)),
    )
    monkeypatch.setattr(
        bf_event_slugs,
        "set_backfill_fully_checked",
        lambda task, value: flags.append((task, value)),
    )
    monkeypatch.setattr(bf_gamma, "APIClient", lambda *a, **k: MagicMock())
    bf.backfill_event_slugs(batch_size=1, max_markets=None, force=False)
    assert progress[-1] == ("event_slugs", 1)
    assert flags[-1] == ("event_slugs", False)


def test_backfill_event_slugs_truncated_fallback_never_marks_fully_checked(
    monkeypatch, no_sleep_tqdm
):
    flags = []
    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: 0)
    monkeypatch.setattr(
        bf_event_slugs, "get_markets_without_event_slugs", lambda limit=None: ["99"]
    )
    monkeypatch.setattr(bf_event_slugs, "set_backfill_progress", lambda *a, **k: None)
    monkeypatch.setattr(
        bf_event_slugs,
        "set_backfill_fully_checked",
        lambda task, value: flags.append((task, value)),
    )
    with patch.object(bf_gamma, "APIClient", lambda *a, **k: MagicMock()):
        with patch.object(
            bf_event_slugs, "_process_market_chunks", return_value=(1, 0, 1)
        ):
            with patch.object(
                bf_event_slugs,
                "_fill_from_events_endpoint",
                return_value=(
                    0,
                    {
                        "events_fallback_pages": 1,
                        "events_fallback_truncated": True,
                        "events_fallback_remaining_ids": 1,
                    },
                ),
            ):
                out = bf.backfill_event_slugs(
                    batch_size=1, max_markets=None, force=False
                )
    assert out["events_fallback_truncated"] is True
    assert flags[-1] == ("event_slugs", False)


def test_backfill_event_slugs_force_true_processes_full_unresolved_list(
    monkeypatch, no_sleep_tqdm
):
    seen = []
    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: 999)
    gm_calls = []

    def gm(limit=None):
        gm_calls.append(limit)
        if len(gm_calls) == 1:
            return ["10", "20"]
        return []

    monkeypatch.setattr(bf_event_slugs, "get_markets_without_event_slugs", gm)

    def proc(**kw):
        seen.append(list(kw["market_ids"]))
        for mid in kw["market_ids"]:
            kw["on_record_saved"](mid)
        return (2, 2, 1)

    monkeypatch.setattr(bf_event_slugs, "_process_market_chunks", proc)
    monkeypatch.setattr(
        bf_event_slugs,
        "_fill_from_events_endpoint",
        lambda *a, **k: (
            0,
            {
                "events_fallback_pages": 0,
                "events_fallback_truncated": False,
                "events_fallback_remaining_ids": 0,
            },
        ),
    )
    monkeypatch.setattr(bf_gamma, "APIClient", lambda *a, **k: MagicMock())
    monkeypatch.setattr(bf_event_slugs, "set_backfill_progress", lambda *a, **k: None)
    monkeypatch.setattr(
        bf_event_slugs, "set_backfill_fully_checked", lambda *a, **k: None
    )
    bf.backfill_event_slugs(max_markets=None, force=True)
    assert seen == [["10", "20"]]


def test_backfill_event_slugs_db_mismatch_after_run_warns_and_skips_fully_checked(
    monkeypatch, no_sleep_tqdm, caplog
):
    import logging

    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: 0)
    gm_calls = []

    def gm(limit=None):
        gm_calls.append(limit)
        if len(gm_calls) == 1:
            return ["7"]
        if len(gm_calls) == 2:
            return ["7"]
        return []

    monkeypatch.setattr(bf_event_slugs, "get_markets_without_event_slugs", gm)

    def proc(**kw):
        kw["on_record_saved"]("7")
        return (1, 1, 1)

    monkeypatch.setattr(bf_event_slugs, "_process_market_chunks", proc)
    monkeypatch.setattr(
        bf_event_slugs,
        "_fill_from_events_endpoint",
        lambda *a, **k: (
            0,
            {
                "events_fallback_pages": 0,
                "events_fallback_truncated": False,
                "events_fallback_remaining_ids": 0,
            },
        ),
    )
    flags = []
    monkeypatch.setattr(bf_gamma, "APIClient", lambda *a, **k: MagicMock())
    monkeypatch.setattr(bf_event_slugs, "set_backfill_progress", lambda *a, **k: None)
    monkeypatch.setattr(
        bf_event_slugs,
        "set_backfill_fully_checked",
        lambda task, value: flags.append((task, value)),
    )
    caplog.set_level(logging.WARNING)
    bf.backfill_event_slugs(max_markets=None, force=False)
    assert flags[-1] == ("event_slugs", False)
    assert any(
        "DuckDB still has markets without event_slug" in r.message
        for r in caplog.records
    )


def test_backfill_end_dates_partial_run_marks_false(monkeypatch, no_sleep_tqdm):
    flags = []
    monkeypatch.setattr(bf_end_dates, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_end_dates, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_end_dates, "get_markets_without_end_date", lambda limit=None: ["40"]
    )
    monkeypatch.setattr(
        bf_end_dates,
        "set_backfill_fully_checked",
        lambda task, value: flags.append((task, value)),
    )
    with patch.object(bf_gamma, "APIClient", MagicMock):
        with patch.object(
            bf_end_dates, "_process_market_chunks", lambda **kw: (1, 1, 1)
        ):
            bf.backfill_end_dates(batch_size=1, max_markets=5, force=True)
    assert flags[-1] == ("end_dates", False)


def test_backfill_end_dates_full_run_marks_completion(monkeypatch, no_sleep_tqdm):
    flags = []
    monkeypatch.setattr(bf_end_dates, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_end_dates, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_end_dates, "get_markets_without_end_date", lambda limit=None: ["40"]
    )
    monkeypatch.setattr(
        bf_end_dates,
        "set_backfill_fully_checked",
        lambda task, value: flags.append((task, value)),
    )
    with patch.object(bf_gamma, "APIClient", MagicMock):
        with patch.object(
            bf_end_dates, "_process_market_chunks", lambda **kw: (1, 1, 1)
        ):
            bf.backfill_end_dates(batch_size=1, max_markets=None, force=True)
    assert flags[-1] == ("end_dates", True)


def test_process_market_chunks_progress_callback(monkeypatch):
    monkeypatch.setattr(bf, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf,
        "tqdm",
        lambda *a, **k: MagicMock(__enter__=lambda s: s, __exit__=lambda *x: None),
    )
    calls = []

    def cb(phase, payload):
        calls.append((phase, payload))

    client = MagicMock()
    client.get.return_value = [{"id": "1", "slug": "s"}]
    bf._process_market_chunks(
        client=client,
        market_ids=["1"],
        batch_size=1,
        desc="t",
        include_events=False,
        extract_record=bf._extract_slug_record,
        save_batch=lambda x: None,
        progress_phase="test_phase",
        progress_callback=cb,
        progress_every_n_batches=1,
    )
    assert any(phase == "test_phase" for phase, _ in calls)
    assert any("batch_index" in pl for _, pl in calls)


def test_iter_gamma_events_keyset_stops_on_empty_events_page():
    from oddsfox.ingestion.polymarket.gamma_events import (
        iter_gamma_events_keyset,
    )

    client = MagicMock()
    client.get.return_value = {"events": [], "next_cursor": "cursor-2"}
    pages = list(iter_gamma_events_keyset(client, max_pages=5))
    assert len(pages) == 1
    assert pages[0][0] == []


def test_iter_gamma_events_keyset_stops_on_non_advancing_cursor():
    from oddsfox.ingestion.polymarket.gamma_events import (
        iter_gamma_events_keyset,
    )

    client = MagicMock()
    client.get.side_effect = [
        {
            "events": [{"id": "1", "slug": "world-cup-winner"}],
            "next_cursor": "stuck-cursor",
        },
        {"events": [], "next_cursor": "stuck-cursor"},
    ]
    pages = list(iter_gamma_events_keyset(client, max_pages=None))
    # Page 1 yields events; page 2 echoes the same cursor with no new rows (EOF).
    assert len(pages) == 2
    assert pages[0][0] == [{"id": "1", "slug": "world-cup-winner"}]
    assert pages[1][0] == []
    assert pages[1][1].truncated is False
    assert client.get.call_count == 2


def test_iter_gamma_events_keyset_non_advancing_duplicate_data_is_eof():
    from oddsfox.ingestion.polymarket.gamma_events import (
        iter_gamma_events_keyset,
    )

    client = MagicMock()
    client.get.side_effect = [
        {
            "events": [{"id": "1", "slug": "world-cup-winner"}],
            "next_cursor": "stuck-cursor",
        },
        {
            "events": [{"id": "1", "slug": "world-cup-winner"}],
            "next_cursor": "stuck-cursor",
        },
    ]
    pages = list(iter_gamma_events_keyset(client, max_pages=None))
    assert len(pages) == 2
    assert pages[1][0] == []
    assert pages[1][1].truncated is False
    assert client.get.call_count == 2


def test_iter_gamma_events_keyset_closed_filter():
    from oddsfox.ingestion.polymarket.gamma_events import (
        iter_gamma_events_keyset,
    )

    client = MagicMock()
    client.get.return_value = {
        "events": [{"id": "1", "slug": "open-event"}],
        "next_cursor": None,
    }
    list(iter_gamma_events_keyset(client, max_pages=5, keyset_closed=False))
    params = client.get.call_args.kwargs.get("params") or {}
    assert params.get("closed") is False

    client.reset_mock()
    client.get.return_value = {
        "events": [{"id": "2", "slug": "any-event"}],
        "next_cursor": None,
    }
    list(iter_gamma_events_keyset(client, max_pages=5))
    params = client.get.call_args.kwargs.get("params") or {}
    assert "closed" not in params


def test_iter_gamma_events_keyset_tag_and_volume_filters():
    from oddsfox.ingestion.polymarket.gamma_events import (
        iter_gamma_events_keyset,
    )

    client = MagicMock()
    client.get.return_value = {
        "events": [{"id": "1", "slug": "world-cup-winner"}],
        "next_cursor": None,
    }
    list(
        iter_gamma_events_keyset(
            client,
            max_pages=5,
            keyset_closed=False,
            keyset_tag_slug="fifa-world-cup",
            keyset_related_tags=True,
            keyset_volume_min=100000,
        )
    )
    params = client.get.call_args.kwargs.get("params") or {}
    assert params.get("closed") is False
    assert params.get("tag_slug") == "fifa-world-cup"
    assert params.get("related_tags") == "true"
    assert params.get("volume_min") == 100000


def test_iter_gamma_events_keyset_related_tags_param():
    from oddsfox.ingestion.polymarket.gamma_events import (
        iter_gamma_events_keyset,
    )

    client = MagicMock()
    client.get.return_value = {
        "events": [{"id": "1", "slug": "wc-event"}],
        "next_cursor": None,
    }
    list(iter_gamma_events_keyset(client, max_pages=5, keyset_related_tags=True))
    params = client.get.call_args.kwargs.get("params") or {}
    assert params.get("related_tags") == "true"


def test_fill_from_events_max_pages_truncates():
    c = MagicMock()
    c.get.return_value = {
        "events": [{"slug": "s", "markets": [{"id": "9"}]}],
        "next_cursor": "cursor-2",
    }
    remaining = {"9", "orphan"}
    with patch.object(bf_events_fallback, "save_event_slugs_batch", lambda x: None):
        saved, meta = bf._fill_from_events_endpoint(c, remaining, max_pages=1)
    assert saved >= 0
    assert meta["events_fallback_pages"] == 1
    assert meta["events_fallback_truncated"] is True
    assert "orphan" in remaining


def test_fill_from_events_no_progress_truncates():
    c = MagicMock()
    c.get.return_value = {
        "events": [{"slug": "s", "markets": [{"id": "not-target"}]}],
        "next_cursor": "cursor-2",
    }
    remaining = {"target"}
    with patch.object(bf_events_fallback, "save_event_slugs_batch", lambda x: None):
        saved, meta = bf._fill_from_events_endpoint(
            c,
            remaining,
            max_pages=None,
            max_pages_without_progress=2,
        )
    assert saved == 0
    assert meta["events_fallback_pages"] == 2
    assert meta["events_fallback_truncated"] is True
    assert "target" in remaining


def test_gamma_client_forwards_requests_per_second(monkeypatch):
    captured = []

    def ctor(*a, **kw):
        captured.append(kw)
        return MagicMock()

    monkeypatch.setattr(bf_gamma, "APIClient", ctor)
    bf._gamma_client(12.5)
    assert captured[0]["requests_per_second"] == 12.5
    bf._gamma_client(None)
    assert captured[1]["requests_per_second"] is None


def test_process_market_chunks_disables_tqdm_when_stderr_not_tty(monkeypatch):
    monkeypatch.setattr(bf_gamma.sys.stderr, "isatty", lambda: False)
    kwargs_seen = []

    def fake_tqdm(*a, **kw):
        kwargs_seen.append(kw)
        return MagicMock(__enter__=lambda s: s, __exit__=lambda *x: None)

    monkeypatch.setattr(bf_gamma, "tqdm", fake_tqdm)
    client = MagicMock()
    client.get.return_value = []
    bf._process_market_chunks(
        client=client,
        market_ids=["1"],
        batch_size=1,
        desc="t",
        include_events=False,
        extract_record=bf._extract_slug_record,
        save_batch=lambda x: None,
    )
    assert kwargs_seen[0]["disable"] is True


def test_fill_from_events_progress_callback():
    calls = []
    c = MagicMock()
    c.get.side_effect = [
        {"events": [{"slug": "s", "markets": [{"id": "1"}]}], "next_cursor": None}
    ]
    with patch.object(bf_events_fallback, "save_event_slugs_batch", lambda x: None):
        bf._fill_from_events_endpoint(
            c,
            {"1"},
            progress_callback=lambda ph, pl: calls.append((ph, pl)),
            progress_every_pages=1,
            progress_phase="fb",
        )
    assert any(ph == "fb" for ph, _ in calls)


def test_fill_from_events_empty_keyset_page_stops():
    c = MagicMock()
    c.get.return_value = {"events": [], "next_cursor": "unused"}
    saved, meta = bf._fill_from_events_endpoint(c, {"target"})
    assert saved == 0
    assert meta["events_fallback_pages"] == 1
    assert meta["events_fallback_truncated"] is False


def test_fill_from_events_uses_keyset_cursor():
    calls = []
    c = MagicMock()
    c.get.side_effect = [
        {
            "events": [{"slug": "s", "markets": [{"id": "nope"}]}],
            "next_cursor": "cursor-2",
        },
        {
            "events": [{"slug": "s2", "markets": [{"id": "target"}]}],
            "next_cursor": None,
        },
    ]
    with patch.object(
        bf_events_fallback, "save_event_slugs_batch", lambda x: calls.append(x)
    ):
        saved, meta = bf._fill_from_events_endpoint(c, {"target"})
    assert saved == 1
    assert meta["events_fallback_pages"] == 2
    first_params = c.get.call_args_list[0].kwargs["params"]
    second_params = c.get.call_args_list[1].kwargs["params"]
    assert "next_cursor" not in first_params
    assert second_params["next_cursor"] == "cursor-2"


def test_backfill_tokens_progress_start_complete(no_sleep_tqdm, monkeypatch):
    calls = []
    monkeypatch.setattr(bf_tokens, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_tokens, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_tokens, "get_markets_without_tokens", lambda limit=None: ["1"]
    )
    monkeypatch.setattr(bf_tokens, "set_backfill_fully_checked", lambda *a: None)
    client = MagicMock()
    client.get.return_value = [{"id": "1", "clobTokenIds": ["z"]}]
    with patch.object(bf_gamma, "APIClient", lambda *a, **k: client):
        bf.backfill_tokens(
            batch_size=1,
            max_markets=1,
            force=True,
            progress_callback=lambda p, d: calls.append((p, d)),
            progress_every_n_batches=1,
        )
    assert any(d.get("stage") == "start" for _, d in calls)
    assert any(d.get("stage") == "complete" for _, d in calls)


def test_backfill_slugs_progress_start_complete(no_sleep_tqdm, monkeypatch):
    calls = []
    monkeypatch.setattr(bf_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(bf_slugs, "get_markets_without_slugs", lambda limit=None: ["1"])
    monkeypatch.setattr(bf_slugs, "set_backfill_fully_checked", lambda *a: None)
    client = MagicMock()
    client.get.return_value = [{"id": "1", "slug": "s"}]
    with patch.object(bf_gamma, "APIClient", lambda *a, **k: client):
        bf.backfill_slugs(
            batch_size=1,
            max_markets=1,
            force=True,
            progress_callback=lambda p, d: calls.append((p, d)),
            progress_every_n_batches=1,
        )
    assert any(d.get("stage") == "start" for _, d in calls)
    assert any(d.get("stage") == "complete" for _, d in calls)


def test_backfill_end_dates_progress_start_complete(no_sleep_tqdm, monkeypatch):
    calls = []
    monkeypatch.setattr(bf_end_dates, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_end_dates, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(
        bf_end_dates, "get_markets_without_end_date", lambda limit=None: ["1"]
    )
    monkeypatch.setattr(bf_end_dates, "set_backfill_fully_checked", lambda *a: None)
    client = MagicMock()
    client.get.return_value = [{"id": "1", "endDate": "2025-01-01"}]
    with patch.object(bf_gamma, "APIClient", lambda *a, **k: client):
        bf.backfill_end_dates(
            batch_size=1,
            max_markets=1,
            force=True,
            progress_callback=lambda p, d: calls.append((p, d)),
            progress_every_n_batches=1,
        )
    assert any(d.get("stage") == "start" for _, d in calls)
    assert any(d.get("stage") == "complete" for _, d in calls)


def test_backfill_event_slugs_progress_fallback_start(no_sleep_tqdm, monkeypatch):
    calls = []
    monkeypatch.setattr(bf_event_slugs, "get_backfill_fully_checked", lambda t: False)
    monkeypatch.setattr(bf_event_slugs, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(bf_event_slugs, "get_backfill_progress", lambda t: 0)
    monkeypatch.setattr(
        bf_event_slugs, "get_markets_without_event_slugs", lambda limit=None: ["50"]
    )
    monkeypatch.setattr(bf_event_slugs, "set_backfill_progress", lambda *a: None)
    monkeypatch.setattr(bf_event_slugs, "set_backfill_fully_checked", lambda *a: None)
    with patch.object(bf_gamma, "APIClient", lambda *a, **k: MagicMock()):
        with patch.object(
            bf_event_slugs, "_process_market_chunks", return_value=(1, 0, 1)
        ):
            with patch.object(
                bf_event_slugs,
                "_fill_from_events_endpoint",
                return_value=(
                    0,
                    {
                        "events_fallback_pages": 1,
                        "events_fallback_truncated": False,
                        "events_fallback_remaining_ids": 0,
                    },
                ),
            ):
                bf.backfill_event_slugs(
                    batch_size=1,
                    max_markets=1,
                    force=True,
                    progress_callback=lambda p, d: calls.append((p, d)),
                )
    assert any(d.get("stage") == "start" for _, d in calls)
    assert any(d.get("stage") == "events_fallback_start" for _, d in calls)
    assert any(d.get("stage") == "complete" for _, d in calls)
