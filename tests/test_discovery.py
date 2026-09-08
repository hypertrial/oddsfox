import json

import httpx
import pytest

from oddsfox.catalog import event_list
from oddsfox.discovery import pages, save_event, tradable, volume
from oddsfox.documents import extract, validate_url
from oddsfox.ingest import import_capture
from oddsfox.store import Store
from oddsfox.sync import SyncRunner


def pm(identity="one", **kwargs):
    return {
        "id": identity,
        "question": "Will it exceed 10?",
        "description": "At noon, the value must exceed 10.",
        "active": True,
        "closed": False,
        "acceptingOrders": True,
        "outcomes": ["yes", "no"],
        "volume": "100001",
        **kwargs,
    }


@pytest.mark.parametrize(
    "amount,known",
    [
        ("100000", "100000"),
        ("100000.000000000001", "100000.000000000001"),
        (None, None),
        ("NaN", None),
        ("-1", None),
        (True, None),
    ],
)
def test_lifetime_volume_exact_and_unknown(amount, known):
    assert volume({"volume": amount}, [], "polymarket")["amount"] == known


def test_volume_includes_closed_children_and_face_value():
    rows = [
        {"volume_fp": "80000.5", "notional_value_dollars": "1"},
        {"volume_fp": "20000", "notional_value_dollars": "2", "status": "settled"},
    ]
    assert volume({}, rows, "kalshi")["amount"] == "120000.5"
    assert volume({}, rows, "kalshi", complete=False)["amount"] is None
    assert volume({}, [{"volume_24h_fp": "1000000"}], "kalshi")["amount"] is None


def test_trading_state_is_independent_from_event_active():
    assert tradable(pm(), "polymarket")
    assert not tradable(pm(closed=True), "polymarket")
    assert not tradable(pm(acceptingOrders=False), "polymarket")
    assert tradable(pm(status="MARKET_STATUS_OPEN"), "polymarket_us")
    assert not tradable(pm(status="MARKET_STATUS_HALTED"), "polymarket_us")


def test_keyset_pages_and_repeat_detection():
    count = 0

    def response(request):
        nonlocal count
        count += 1
        return httpx.Response(200, json={"events": [{"id": str(count)}], "next_cursor": "again"})

    with httpx.Client(transport=httpx.MockTransport(response)) as client:
        iterator = pages(client, "polymarket")
        assert next(iterator)[0][0]["id"] == "1"
        with pytest.raises(ValueError, match="repeated"):
            next(iterator)


def test_us_missing_volume_stays_visible_without_invented_total(store):
    event = {
        "id": "one",
        "title": "Championship",
        "markets": [pm(volume=None, status="MARKET_STATUS_OPEN")],
    }
    with httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(500))) as client:
        save_event(
            store,
            client,
            "polymarket_us",
            event,
            "run",
            store.put_artifact(b"{}"),
            "https://gateway.polymarket.us/v1/events",
        )
    assert event_list(store)["total"] == 0
    result = event_list(store, qualification="unknown")
    assert result["total"] == 1
    assert result["items"][0]["data"]["volume"]["amount"] is None


def test_snapshot_refresh_preserves_semantic_revision_and_dependencies(store):
    raw = pm()
    first = import_capture(store, "polymarket", json.dumps(raw).encode())
    with store.transaction():
        review = store.insert("review", first, {"approved": True}, [first], "REVIEWED")
    for patch in [{"volume": "200000"}, {"updatedAt": "2026-09-08"}, {"closed": True}]:
        assert import_capture(store, "polymarket", json.dumps(raw | patch).encode()) == first
        assert store.get(review)["current"]
    changed = import_capture(
        store, "polymarket", json.dumps(raw | {"description": "Different rules."}).encode()
    )
    assert changed != first
    assert not store.get(review)["current"]
    assert len(store._rows("SELECT * FROM snapshots")) == 5


def test_over_250_and_migration_backup_integrity(store, tmp_path):
    first = None
    for i in range(251):
        current = import_capture(store, "polymarket", json.dumps(pm(str(i))).encode())
        first = first or current
    assert len(store.list("contract")) == 251
    backup = tmp_path / "backup"
    store.backup(backup)
    restored = Store(backup)
    try:
        assert restored.get(first)["id"] == first
        assert restored.db.execute("SELECT version FROM metadata").fetchall() == [(2,)]
        restored.verify_integrity()
    finally:
        restored.close()
    extra = store.put_artifact(b"new snapshot only")
    with store.transaction():
        store.db.execute(
            "INSERT INTO snapshots VALUES (?,?,?,?,?)", ["a", first, extra, "{}", "now"]
        )
    (store.artifacts / extra).unlink()
    with pytest.raises(ValueError, match="missing referenced artifact"):
        store.verify_integrity()


def test_existing_v1_migration_retains_nodes(tmp_path):
    path = tmp_path / "legacy"
    original = Store(path)
    identity = import_capture(original, "polymarket", json.dumps(pm()).encode())
    original.db.execute("UPDATE metadata SET version=1")
    original.db.execute("DROP TABLE semantic_heads")
    original.close()
    migrated = Store(path)
    try:
        assert migrated.get(identity)["current"]
        assert (
            import_capture(migrated, "polymarket", json.dumps(pm(volume="900000")).encode())
            == identity
        )
    finally:
        migrated.close()


def test_sync_coalesces_and_pause_persists(store):
    runner = SyncRunner(store)
    try:
        assert runner.request("polymarket") == runner.request("polymarket")
        runner.pause(True)
        runner.tick()
        assert runner.futures == {}
        assert all(r["paused"] for r in store._rows("SELECT * FROM sync_state"))
    finally:
        runner.close()


def test_document_boundaries(monkeypatch):
    for url in [
        "http://kalshi.com/rules",
        "https://evil.example/rules",
        "https://kalshi.com:444/rules",
        "https://x@kalshi.com/rules",
    ]:
        with pytest.raises(ValueError):
            validate_url(url)
    monkeypatch.setattr(
        "socket.getaddrinfo", lambda *a, **k: [(None, None, None, None, ("127.0.0.1", 443))]
    )
    with pytest.raises(ValueError, match="non-public"):
        validate_url("https://kalshi.com/rules")
    assert "evil" not in extract(b"<script>evil</script><p>Official rule.</p>")
    assert "Official rule." in extract(b"<p>Official rule.</p>")


def test_partial_membership_retains_previous_and_recovers(store):
    state = {"failed": False}
    market = {
        "ticker": "EVENT-A",
        "event_ticker": "EVENT",
        "market_type": "binary",
        "title": "Example",
        "rules_primary": "At noon above 10.",
        "status": "active",
        "volume_fp": "120001",
        "notional_value_dollars": "1",
    }
    event = {"event_ticker": "EVENT", "title": "Example", "markets": [market]}

    def handler(request):
        path = request.url.path
        if path.endswith("/events/multivariate"):
            return httpx.Response(200, json={"events": [], "cursor": ""})
        if path.endswith("/events"):
            return httpx.Response(200, json={"events": [event], "cursor": ""})
        if "/historical/" in path and state["failed"]:
            return httpx.Response(404)
        if path.endswith("/markets"):
            return httpx.Response(
                200, json={"markets": [] if "/historical/" in path else [market], "cursor": ""}
            )
        return httpx.Response(200, json={"market": market})

    runner = SyncRunner(
        store, client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler))
    )
    try:
        runner.run_venue("kalshi", runner.request("kalshi"))
        first = store._rows("SELECT * FROM events")[0]
        assert first["qualification"] == "qualified"
        state["failed"] = True
        runner.run_venue("kalshi", runner.request("kalshi"))
        row = store._rows("SELECT * FROM events")[0]
        assert row["qualification"] == "qualified" and row["semantic_id"] == first["semantic_id"]
        assert "stale_reason" in row["data"]
        assert (
            store._rows("SELECT data FROM sync_state WHERE venue='kalshi'")[0]["data"]["state"]
            == "partial"
        )
        state["failed"] = False
        runner.run_venue("kalshi", runner.request("kalshi"))
        assert "stale_reason" not in store._rows("SELECT data FROM events")[0]["data"]
    finally:
        runner.close()


def test_chunk_citations_and_complete_source_coverage(store, tmp_path):
    from oddsfox.explanations import AnalysisEngine, chunks, decode_chunk

    market = pm(description="Rules line.\n" + "x" * 22000)
    event = {"id": "e", "title": "Election example", "volume": "120001", "markets": [market]}
    with httpx.Client(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=market))
    ) as client:
        save_event(store, client, "polymarket", event, "run", store.put_artifact(b"{}"), "source")
    row = store._rows("SELECT * FROM events")[0]
    spans = [s for c in chunks(store, row["semantic_id"]) for s in c]
    for span in spans:
        assert (
            store.artifact(span["artifact_id"]).decode()[span["start"] : span["end"]]
            == span["text"]
        )
    assert (
        max(len("".join(s["text"] for s in c)) for c in chunks(store, row["semantic_id"])) <= 10000
    )
    with pytest.raises(ValueError, match="citation"):
        decode_chunk(
            json.dumps(
                {
                    "facts": [{"topic": "question", "text": "Unsupported", "lines": [999]}],
                    "entities": [],
                    "dates": [],
                    "family": "other",
                    "gaps": [],
                }
            ),
            spans[:1],
        )

    def generate(prompt, schema):
        return json.dumps(
            {
                "facts": [
                    {"topic": "question", "text": "An unreviewed explanation.", "lines": [0]}
                ],
                "entities": ["Distinct Entity"],
                "dates": ["2026-10-01"],
                "family": "other",
                "gaps": [],
            }
        )

    engine = AnalysisEngine(
        store, tmp_path / "no-model", generator=generate, manifest={"identity": "test"}
    )
    for _ in range(30):
        engine.step()
        if store.current("explanation", row["semantic_id"]):
            break
    assert store.current("explanation", row["semantic_id"])
    assert not store.list("assertion")
    before = len(store.list("explanation_chunk", True))
    restarted = AnalysisEngine(
        store, tmp_path / "no-model", generator=generate, manifest={"identity": "test"}
    )
    assert restarted.config_id == engine.config_id
    restarted.step()
    assert len(store.list("explanation_chunk", True)) == before
    with httpx.Client(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=market))
    ) as client:
        save_event(store, client, "polymarket", event, "run2", store.put_artifact(b"{}"), "source")
    assert store._rows("SELECT * FROM event_terms WHERE term='distinct'")


def test_structured_combination_legs_are_citable(store):
    from oddsfox.explanations import chunks

    raw = {
        "ticker": "COMBO",
        "event_ticker": "C",
        "market_type": "binary",
        "rules_primary": "All legs must win.",
        "mve_selected_legs": [{"event_ticker": "LEG_ONLY_JSON", "side": "yes"}],
    }
    contract = import_capture(store, "kalshi", json.dumps(raw).encode())
    with store.transaction():
        sid = store.insert(
            "event_semantics", "kalshi:C", {"contracts": [contract]}, [contract], "CAPTURED"
        )
    assert "LEG_ONLY_JSON" in "".join(s["text"] for c in chunks(store, sid) for s in c)


def test_cached_comparisons_resume_and_report_reads_no_solver(store, monkeypatch):
    from oddsfox.demo import load_demo
    from oddsfox.pipeline import Pipeline

    load_demo(store)
    pipeline = Pipeline(store)
    for _ in range(4):
        pipeline.refresh_comparisons(max_pairs=1)
    assert pipeline.cached_comparisons()["state"] == "complete"
    assert max(r["pair_cursor"] for r in store._rows("SELECT * FROM comparison_groups")) == 1
    monkeypatch.setattr(
        "oddsfox.pipeline.verify",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("solver on read")),
    )
    assert pipeline.cached_comparisons()["processed"] == 2
    from fastapi.testclient import TestClient

    from oddsfox.app import create_app

    with TestClient(create_app(store, auto_sync=False), base_url="http://127.0.0.1:8777") as client:
        assert len(client.get("/api/report").json()["comparisons"]) == 2
        assert client.post("/api/sync", json={}).status_code == 403
        assert client.get("/api/events?limit=101").status_code == 422
        assert client.get("/api/sync").json()["interval_seconds"] == 900


def test_ready_matches_progress_without_failed_candidate(store, tmp_path):
    from oddsfox.explanations import AnalysisEngine

    def seed(identity, venue, explained):
        with store.transaction():
            sid = store.insert("event_semantics", identity, {"contracts": []}, [], "CAPTURED")
            store.db.execute(
                "INSERT INTO events VALUES (?,?,?,?,?,120001,'qualified',true,'run',?,?, 'now')",
                [identity, venue, identity, "Shared election", "Politics", sid, "{}"],
            )
            store.db.execute("INSERT INTO event_terms VALUES ('shared',?)", [identity])
        if explained:
            explain(sid)
        return sid

    def explain(sid):
        with store.transaction():
            return store.insert(
                "explanation",
                sid,
                {"entities": ["Shared"], "dates": [], "facts": [{"text": sid, "citations": []}]},
                [sid],
                "UNREVIEWED",
            )

    a = seed("a", "polymarket", True)
    b = seed("b", "kalshi", True)
    c = seed("c", "polymarket_us", False)
    calls = []

    def generate(prompt, schema):
        candidates = schema["$defs"]["Match"]["properties"]["candidate"]["enum"]
        calls.append(candidates)
        return json.dumps(
            {
                "matches": [
                    {
                        "candidate": candidate,
                        "relationship": "possible_match",
                        "reason": "Unreviewed similar question",
                        "lines": [0, i + 1],
                    }
                    for i, candidate in enumerate(candidates)
                ]
            }
        )

    engine = AnalysisEngine(store, tmp_path, generator=generate, manifest={"test": True})
    event = store._rows("SELECT * FROM events WHERE id='a'")[0]
    assert engine.match_step(event, store.current("explanation", a))
    result = store.current("suggestions", a)
    assert calls == [[b]]
    assert result["data"]["omitted_pending_explanations"] == [c]
    assert result["status"] == "UNREVIEWED" and not store.list("assertion")
    explain(c)
    assert engine.match_step(event, store.current("explanation", a))
    assert set(calls[-1]) == {b, c}
    assert store.current("suggestions", a)["id"] != result["id"]
    assert not store.list("assertion")
    restored_from = store.current("suggestions", a)["id"]
    store.db.execute("UPDATE events SET active=false WHERE id='c'")
    assert engine.match_step(event, store.current("explanation", a))
    assert calls[-1] == [b]
    store.db.execute("UPDATE events SET active=true WHERE id='c'")
    assert engine.match_step(event, store.current("explanation", a))
    assert set(calls[-1]) == {b, c}
    assert store.current("suggestions", a)["id"] != restored_from


def test_manual_sync_works_without_automatic_refresh(store, monkeypatch):
    from concurrent.futures import Future

    class Immediate:
        def submit(self, fn, *args):
            future = Future()
            try:
                future.set_result(fn(*args))
            except Exception as exc:
                future.set_exception(exc)
            return future

    runner = SyncRunner(
        store,
        client_factory=lambda: httpx.Client(
            transport=httpx.MockTransport(
                lambda r: httpx.Response(200, json={"events": [], "next_cursor": ""})
            )
        ),
    )
    try:
        runner.discovery_enabled = False
        monkeypatch.setattr(runner.network, "submit", Immediate().submit)
        runner.tick()
        assert not runner.futures
        runner.request("polymarket", manual=True)
        runner.tick()
        row = store._rows("SELECT * FROM sync_state WHERE venue='polymarket'")[0]
        assert row["data"]["state"] == "complete"
        assert not row["data"]["manual"]
        import time

        assert time.time() + 890 < row["due"] < time.time() + 901
        old = row["job_id"]
        runner.tick()
        assert (
            store._rows("SELECT job_id FROM sync_state WHERE venue='polymarket'")[0]["job_id"]
            == old
        )
    finally:
        runner.close()


def test_processing_failure_is_visible_and_backed_off(store):
    import time
    from concurrent.futures import Future

    runner = SyncRunner(store)
    future = Future()
    future.set_exception(ValueError("model unavailable"))
    runner.analysis_future = future
    try:
        runner.launch_lane(
            "analysis_future", "analysis", runner.analysis, lambda: pytest.fail("early retry")
        )
        row = store._rows("SELECT * FROM lane_state WHERE lane='analysis'")[0]
        assert row["state"] == "failed" and "model unavailable" in row["diagnostic"]
        assert row["retry_at"] > time.time()
        assert runner.analysis_future is None
    finally:
        runner.close()


def test_offset_discovery_deduplicates_events_and_scans_past_250(store):
    def response(request):
        offset = int(request.url.params.get("offset", "0"))
        identities = list(range(offset, min(offset + 100, 301)))
        if offset == 100:
            identities[0] = 99  # Overlap with the previous page, still follow the offset.
        return httpx.Response(
            200,
            json={
                "events": [
                    {
                        "id": str(i),
                        "markets": [pm(str(i), volume=None, status="MARKET_STATUS_OPEN")],
                    }
                    for i in identities
                ]
            },
        )

    runner = SyncRunner(
        store, client_factory=lambda: httpx.Client(transport=httpx.MockTransport(response))
    )
    try:
        runner.run_venue("polymarket_us", runner.request("polymarket_us"))
        assert event_list(store, qualification="unknown")["total"] == 300
        assert (
            store._rows("SELECT data FROM sync_state WHERE venue='polymarket_us'")[0]["data"][
                "pages"
            ]
            == 4
        )
    finally:
        runner.close()


def test_retiring_events_requires_successful_complete_scan(store):
    event = {"id": "e", "markets": [pm(volume=None, status="MARKET_STATUS_OPEN")]}
    failing = False

    def response(request):
        if failing:
            return httpx.Response(403)
        return httpx.Response(200, json={"events": [event]})

    runner = SyncRunner(
        store, client_factory=lambda: httpx.Client(transport=httpx.MockTransport(response))
    )
    try:
        runner.run_venue("polymarket_us", runner.request("polymarket_us"))
        failing = True
        runner.run_venue("polymarket_us", runner.request("polymarket_us"))
        assert event_list(store, qualification="unknown")["total"] == 1
        assert (
            store._rows("SELECT data FROM sync_state WHERE venue='polymarket_us'")[0]["data"][
                "state"
            ]
            == "failed"
        )
    finally:
        runner.close()


def test_unsupported_family_cannot_be_auto_compiled_by_wrong_model_label(
    store, tmp_path, monkeypatch
):
    from oddsfox.explanations import AnalysisEngine

    contract = import_capture(
        store, "polymarket", json.dumps(pm(description="Candidate Red wins the election.")).encode()
    )
    engine = AnalysisEngine(store, tmp_path, generator=lambda *a: "", manifest={"test": True})
    monkeypatch.setattr(
        "oddsfox.compiler.prepare_compile", lambda *a: pytest.fail("unsupported compile")
    )
    assert not engine.compile_step(
        {"data": {"combination": False, "contracts": [contract]}},
        {"data": {"families": ["instantaneous_threshold"]}},
    )
    assert not store.list("assertion")


def test_unrelated_observation_preserves_completed_comparison_cache(store):
    from oddsfox.demo import load_demo, sample
    from oddsfox.pipeline import Pipeline

    pipeline = Pipeline(store)
    load_demo(store)
    for _ in range(3):
        pipeline.refresh_comparisons()
    before = store._rows("SELECT * FROM comparison_groups ORDER BY group_id")
    claims = pipeline.cached_rows()
    ir = sample(store, "unrelated", source="Unrelated independent source")
    pipeline.register("unrelated", ir["observation"], "test", "separate observation")
    pipeline.interpret(json.dumps(ir))
    after = store._rows("SELECT * FROM comparison_groups ORDER BY group_id")
    assert before == after
    assert claims == pipeline.cached_rows()


def test_decimal_aggregation_does_not_round_qualification_boundary():
    amount = volume(
        {},
        [pm(volume="100000"), pm("two", volume="0.000000000000000000000000000001")],
        "polymarket",
    )
    assert amount["amount"] == "100000.000000000000000000000000000001"


def test_discovery_restart_resumes_after_persisted_page(tmp_path):
    path = tmp_path / "resume"
    store = Store(path)
    requests = []
    runner = None

    def response(request):
        offset = int(request.url.params.get("offset", "0"))
        requests.append(offset)
        if offset == 100 and len(requests) == 2:
            runner.stop_event.set()
        return httpx.Response(
            200,
            json={
                "events": [
                    {
                        "id": str(i),
                        "markets": [pm(str(i), volume=None, status="MARKET_STATUS_OPEN")],
                    }
                    for i in range(offset, min(offset + 100, 101))
                ]
            },
        )

    factory = lambda: httpx.Client(transport=httpx.MockTransport(response))  # noqa: E731
    runner = SyncRunner(store, client_factory=factory)
    job = runner.request("polymarket_us")
    runner.run_venue("polymarket_us", job)
    assert event_list(store, qualification="unknown")["total"] == 100
    runner.close()
    store.close()
    resumed = Store(path)
    runner = SyncRunner(resumed, client_factory=factory)
    try:
        assert runner.request("polymarket_us") == job
        runner.run_venue("polymarket_us", job)
        assert requests == [0, 100, 100]
        assert event_list(resumed, qualification="unknown")["total"] == 101
        assert (
            resumed._rows("SELECT data FROM sync_state WHERE venue='polymarket_us'")[0]["data"][
                "state"
            ]
            == "complete"
        )
    finally:
        runner.close()
        resumed.close()


def test_rate_limit_honors_bounded_retry_after(monkeypatch):
    from oddsfox.discovery import request_json

    delays = []
    monkeypatch.setattr("oddsfox.discovery.time.sleep", delays.append)
    attempts = 0

    def response(request):
        nonlocal attempts
        attempts += 1
        return (
            httpx.Response(429, headers={"retry-after": "999"})
            if attempts == 1
            else httpx.Response(200, json={})
        )

    with httpx.Client(transport=httpx.MockTransport(response)) as client:
        assert request_json(client, "https://example.org")[0] == {}
    assert delays == [10] and attempts == 2


def test_unreadable_pdf_preserves_download_and_blocks_completeness(store, monkeypatch):
    from io import BytesIO

    from pypdf import PdfWriter

    from oddsfox.documents import capture_document

    writer = PdfWriter()
    writer.add_blank_page(width=100, height=100)
    stream = BytesIO()
    writer.write(stream)
    raw = stream.getvalue()
    monkeypatch.setattr("oddsfox.documents.retrieve", lambda url: raw)
    monkeypatch.setattr("oddsfox.documents.extract_bounded", extract)
    result = capture_document(store, "https://kalshi.com/blank.pdf")
    assert result["status"] == "inaccessible"
    assert "no extractable text" in result["reason"]
    assert store.artifact(result["raw_artifact"]) == raw
