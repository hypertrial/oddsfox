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
    assert tradable({"status": "active"}, "kalshi")
    assert not tradable({"status": "settled"}, "kalshi")


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


def test_missing_volume_stays_visible_without_invented_total(store):
    event = {
        "id": "one",
        "title": "Championship",
        "markets": [pm(volume=None)],
    }
    with httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(500))) as client:
        save_event(
            store,
            client,
            "polymarket",
            event,
            "run",
            store.put_artifact(b"{}"),
            "https://gamma-api.polymarket.com/events/keyset",
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


def test_inaccessible_document_refetch_preserves_semantic_revision(store):
    texts = {"rules": "same governing text", "document:0": "official pdf"}
    metadata = {"title": "t", "lifecycle": "open"}
    captured = [{"url": "https://kalshi.com/a.pdf", "status": "captured", "text_key": "document:0"}]
    first = store.capture("kalshi", "x", b"{}", texts, metadata, captured)
    with store.transaction():
        review = store.insert("review", first, {"approved": True}, [first], "REVIEWED")
    failed = store.capture(
        "kalshi",
        "x",
        b"{}",
        {"rules": "same governing text"},
        metadata,
        [{"url": "https://kalshi.com/a.pdf", "status": "inaccessible", "reason": "timeout"}],
    )
    assert failed == first
    assert store.get(review)["current"]
    assert len(store._rows("SELECT * FROM snapshots")) == 2
    dropped = store.capture(
        "kalshi",
        "x",
        b"{}",
        {"rules": "same governing text"},
        metadata,
        [],
    )
    assert dropped != first
    assert not store.get(review)["current"]


def test_known_event_rules_cannot_be_omitted(store):
    store.capture(
        "kalshi",
        "parent",
        b"{}",
        {"event_rules": "shared event rules", "rules": "child"},
        {"title": "t", "lifecycle": "open"},
        [],
    )
    with pytest.raises(ValueError, match="parent governing material"):
        store.capture(
            "kalshi",
            "parent",
            b"{}",
            {"rules": "child"},
            {"title": "t", "lifecycle": "open"},
            [],
        )


def test_blank_event_rules_cannot_drop_parent_material(store):
    store.capture(
        "kalshi",
        "parent",
        b"{}",
        {"event_rules": "shared event rules", "rules": "child"},
        {"title": "t", "lifecycle": "open"},
        [],
    )
    with pytest.raises(ValueError, match="parent governing material"):
        store.capture(
            "kalshi",
            "parent",
            b"{}",
            {"event_rules": "", "rules": "child"},
            {"title": "t", "lifecycle": "open"},
            [],
        )


def test_governing_fingerprint_ignores_status_reason_and_url_order(store):
    texts = {"rules": "same governing text", "document:0": "pdf-a", "document:1": "pdf-b"}
    metadata = {"title": "t", "lifecycle": "open"}
    first = store.capture(
        "kalshi",
        "x",
        b"{}",
        texts,
        metadata,
        [
            {"url": "https://kalshi.com/a.pdf", "status": "captured", "text_key": "document:0"},
            {"url": "https://kalshi.com/b.pdf", "status": "captured", "text_key": "document:1"},
        ],
    )
    reordered = store.capture(
        "kalshi",
        "x",
        b"{}",
        texts,
        metadata,
        [
            {
                "url": "https://kalshi.com/b.pdf",
                "status": "inaccessible",
                "reason": "timeout",
                "text_key": "document:1",
            },
            {
                "url": "https://kalshi.com/a.pdf",
                "status": "captured",
                "reason": "ok",
                "text_key": "document:0",
            },
        ],
    )
    assert reordered == first


def test_omitted_document_text_is_retained_per_url(store):
    texts = {"rules": "same governing text", "document:0": "pdf-a", "document:1": "pdf-b"}
    metadata = {"title": "t", "lifecycle": "open"}
    refs = [
        {"url": "https://kalshi.com/a.pdf", "status": "captured", "text_key": "document:0"},
        {"url": "https://kalshi.com/b.pdf", "status": "captured", "text_key": "document:1"},
    ]
    first = store.capture("kalshi", "x", b"{}", texts, metadata, refs)
    retained = store.capture(
        "kalshi",
        "x",
        b"{}",
        {"rules": "same governing text"},
        metadata,
        [
            {"url": "https://kalshi.com/a.pdf", "status": "inaccessible", "reason": "timeout"},
            {"url": "https://kalshi.com/b.pdf", "status": "captured"},
        ],
    )
    assert retained == first
    blobs = {
        store.artifact(identity)
        for identity in store.get(retained)["data"]["text_artifacts"].values()
    }
    assert blobs == {b"same governing text", b"pdf-a", b"pdf-b"}
    foreign = store.capture(
        "kalshi",
        "x",
        b"{}",
        {"rules": "same governing text"},
        metadata,
        [{"url": "https://kalshi.com/other.pdf", "status": "inaccessible", "reason": "timeout"}],
    )
    assert foreign != first
    assert list(store.get(foreign)["data"]["text_artifacts"]) == ["rules"]


def test_retained_document_reuses_previous_text_key_after_index_hole(store):
    metadata = {"title": "t", "lifecycle": "open"}
    first = store.capture(
        "kalshi",
        "x",
        b"{}",
        {"rules": "same governing text", "document:1": "pdf-b"},
        metadata,
        [
            {"url": "https://kalshi.com/a.pdf", "status": "inaccessible", "reason": "timeout"},
            {
                "url": "https://kalshi.com/b.pdf",
                "status": "captured",
                "text_key": "document:1",
            },
        ],
    )
    later = store.capture(
        "kalshi",
        "x",
        b"{}",
        {"rules": "same governing text"},
        metadata,
        [
            {"url": "https://kalshi.com/a.pdf", "status": "inaccessible", "reason": "timeout"},
            {"url": "https://kalshi.com/b.pdf", "status": "inaccessible", "reason": "timeout"},
        ],
    )
    assert later == first
    artifacts = store.get(later)["data"]["text_artifacts"]
    assert "document:1" in artifacts
    assert store.artifact(artifacts["document:1"]) == b"pdf-b"


def test_import_inaccessible_document_keeps_prior_capture(store):
    payload = json.dumps(pm()).encode()
    first = import_capture(
        store,
        "polymarket",
        payload,
        documents=[{"url": "https://example.com/a.pdf", "text": "official", "status": "captured"}],
    )
    later = import_capture(
        store,
        "polymarket",
        payload,
        documents=[{"url": "https://example.com/a.pdf", "text": "", "status": "inaccessible"}],
    )
    assert later == first
    assert b"official" in {
        store.artifact(identity) for identity in store.get(later)["data"]["text_artifacts"].values()
    }


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
        assert restored.db.execute("SELECT version FROM metadata").fetchall() == [(4,)]
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
    with pytest.raises(ValueError, match="migrat"):
        Store(path)
    Store.migrate(path, tmp_path / "legacy-backup")
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
    c = seed("c", "kalshi", False)
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


def test_keyset_discovery_deduplicates_events_and_scans_past_250(store):
    def response(request):
        offset = int(request.url.params.get("after_cursor", "0"))
        assert request.url.path == "/events/keyset"
        assert "offset" not in request.url.params
        identities = list(range(offset, min(offset + 100, 301)))
        if offset == 100:
            identities[0] = 99  # Overlap with the previous page, still follow the cursor.
        return httpx.Response(
            200,
            json={
                "events": [
                    {
                        "id": str(i),
                        "markets": [pm(str(i), volume=None)],
                    }
                    for i in identities
                ],
                "next_cursor": str(offset + 100) if offset < 300 else "",
            },
        )

    runner = SyncRunner(
        store, client_factory=lambda: httpx.Client(transport=httpx.MockTransport(response))
    )
    try:
        runner.run_venue("polymarket", runner.request("polymarket"))
        assert event_list(store, qualification="unknown")["total"] == 300
        assert (
            store._rows("SELECT data FROM sync_state WHERE venue='polymarket'")[0]["data"]["pages"]
            == 4
        )
    finally:
        runner.close()


def test_retiring_events_requires_successful_complete_scan(store):
    event = {"id": "e", "markets": [pm(volume=None)]}
    failing = False

    def response(request):
        if failing:
            return httpx.Response(403)
        return httpx.Response(200, json={"events": [event]})

    runner = SyncRunner(
        store, client_factory=lambda: httpx.Client(transport=httpx.MockTransport(response))
    )
    try:
        runner.run_venue("polymarket", runner.request("polymarket"))
        failing = True
        runner.run_venue("polymarket", runner.request("polymarket"))
        assert event_list(store, qualification="unknown")["total"] == 1
        assert (
            store._rows("SELECT data FROM sync_state WHERE venue='polymarket'")[0]["data"]["state"]
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


def test_comparison_coverage_reports_truncated_cached_rows(store):
    from oddsfox.demo import load_demo
    from oddsfox.pipeline import Pipeline

    pipeline = Pipeline(store)
    load_demo(store)
    for _ in range(3):
        pipeline.refresh_comparisons()
    signatures = [context["signature"] for context in pipeline.comparison_contexts().values()]
    assert signatures
    with store.transaction():
        for index in range(251):
            store.db.execute(
                "INSERT INTO comparison_rows VALUES (?,?,?) ON CONFLICT DO NOTHING",
                [signatures[0], f"extra-{index}", "{}"],
            )
    coverage = pipeline.cached_row_coverage()
    assert coverage["total"] >= 251
    assert coverage["processed"] == 250
    assert coverage["complete"] is False
    assert len(pipeline.cached_rows()) == 250
    from fastapi.testclient import TestClient

    from oddsfox.app import create_app

    with TestClient(create_app(store, auto_sync=False), base_url="http://127.0.0.1:8777") as client:
        payload = client.get("/api/report").json()["comparison_coverage"]
        assert payload["complete"] is False
        assert payload["processed"] == 250
        assert payload["total"] >= 251


def test_comparison_coverage_is_complete_at_exactly_250_rows(store):
    from oddsfox.demo import load_demo
    from oddsfox.pipeline import Pipeline

    pipeline = Pipeline(store)
    load_demo(store)
    for _ in range(3):
        pipeline.refresh_comparisons()
    signatures = [context["signature"] for context in pipeline.comparison_contexts().values()]
    existing = pipeline.cached_row_coverage()
    assert signatures and existing["total"] < 250
    with store.transaction():
        for index in range(250 - existing["total"]):
            store.db.execute(
                "INSERT INTO comparison_rows VALUES (?,?,?) ON CONFLICT DO NOTHING",
                [signatures[0], f"fill-{index}", "{}"],
            )
    coverage = pipeline.cached_row_coverage()
    assert coverage == {"processed": 250, "total": 250, "complete": True}
    assert len(pipeline.cached_rows()) == 250


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
        offset = int(request.url.params.get("after_cursor", "0"))
        assert request.url.path == "/events/keyset"
        assert "offset" not in request.url.params
        requests.append(offset)
        if offset == 100 and len(requests) == 2:
            runner.stop_event.set()
        return httpx.Response(
            200,
            json={
                "events": [
                    {
                        "id": str(i),
                        "markets": [pm(str(i), volume=None)],
                    }
                    for i in range(offset, min(offset + 100, 101))
                ],
                "next_cursor": "100" if offset == 0 else "",
            },
        )

    factory = lambda: httpx.Client(transport=httpx.MockTransport(response))  # noqa: E731
    runner = SyncRunner(store, client_factory=factory)
    job = runner.request("polymarket")
    runner.run_venue("polymarket", job)
    assert event_list(store, qualification="unknown")["total"] == 100
    runner.close()
    store.close()
    resumed = Store(path)
    runner = SyncRunner(resumed, client_factory=factory)
    try:
        assert runner.request("polymarket") == job
        runner.run_venue("polymarket", job)
        assert requests == [0, 100, 100]
        assert event_list(resumed, qualification="unknown")["total"] == 101
        assert (
            resumed._rows("SELECT data FROM sync_state WHERE venue='polymarket'")[0]["data"][
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
        assert request_json(client, "https://gamma-api.polymarket.com/markets")[0] == {}
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


def test_parent_rules_invalidate_reviewed_child_claims_but_volume_does_not(store):
    from oddsfox.demo import load_demo
    from oddsfox.pipeline import Pipeline

    load_demo(store, approve=True)
    pipeline = Pipeline(store)
    original = next(
        r
        for r in store.list("interpretation")
        if store.get(r["logical"])["logical"] == "polymarket:example-high"
    )
    market = json.loads(store.artifact(store.get(original["logical"])["data"]["payload_artifact"]))
    market.update(active=True, closed=False, acceptingOrders=True, volume="120001")
    event = {
        "id": "parent",
        "title": "Parent rules",
        "description": "Use FIRST published value.",
        "volume": "120001",
        "markets": [market],
    }
    with httpx.Client(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=market))
    ) as client:

        def capture():
            save_event(
                store,
                client,
                "polymarket",
                event,
                "run",
                store.put_artifact(json.dumps(event).encode()),
                "https://gamma-api.polymarket.com/events/keyset",
            )
            return store._rows("SELECT data FROM events WHERE id='polymarket:parent'")[0]["data"][
                "contracts"
            ][0]

        first = capture()
        ir = original["data"]["ir"] | {"contract_version_id": first}
        interpretation = pipeline.interpret(json.dumps(ir))
        review = pipeline.review(
            interpretation, "fixture", "Reviewed complete parent and child text.", True, True
        )
        pipeline.publish()
        assert store.list("assertion")
        from oddsfox.ingest import fetch

        with pytest.raises(ValueError, match="event discovery"):
            import_capture(store, "polymarket", json.dumps(market).encode())
        failed = fetch(store, "polymarket", [market["id"]], client=client)
        assert failed[0]["state"] == "failed"
        assert store.get(review)["current"]
        assert store.current("contract", "polymarket:" + market["id"])["id"] == first
        event["volume"] = "900000"
        assert capture() == first
        assert store.get(review)["current"]
        event["description"] = "Use FINAL revised value, overriding individual market descriptions."
        assert capture() != first
        assert not store.get(review)["current"]
        assert not store.list("assertion")


@pytest.mark.parametrize("interruption", ["pause", "crash"])
def test_partial_page_failure_survives_pause_or_crash(tmp_path, monkeypatch, interruption):
    import oddsfox.sync as sync_module
    from oddsfox.catalog import set_sync_state

    path = tmp_path / "partial-restart"
    store = Store(path)
    bad = {"id": "bad", "markets": [pm("b", volume=None)]}
    unseen = {"id": "unseen", "markets": [pm("u", volume=None)]}
    tail = {"id": "tail", "markets": [pm("t", volume=None)]}

    def factory():
        return httpx.Client(
            transport=httpx.MockTransport(
                lambda r: httpx.Response(200, json={"events": [bad, tail]})
            )
        )

    with factory() as client:
        for event in [bad, unseen]:
            save_event(
                store, client, "polymarket", event, "old", store.put_artifact(b"{}"), "source"
            )
    bad["markets"] = None
    runner = SyncRunner(store, client_factory=factory)
    set_sync_state(store, "polymarket", last_success="previous-success")
    real = sync_module.save_event
    interrupted = False

    def capturing(*args, **kwargs):
        nonlocal interrupted
        try:
            return real(*args, **kwargs)
        finally:
            if not interrupted:
                interrupted = True
                if interruption == "crash":
                    raise KeyboardInterrupt("simulate process loss after event transaction")
                runner.pause(True)

    monkeypatch.setattr(sync_module, "save_event", capturing)
    job = runner.request("polymarket")
    if interruption == "crash":
        with pytest.raises(KeyboardInterrupt):
            runner.run_venue("polymarket", job)
    else:
        runner.run_venue("polymarket", job)
    runner.close()
    store.close()
    reopened = Store(path)
    runner = SyncRunner(reopened, client_factory=factory)
    monkeypatch.setattr(sync_module, "save_event", real)
    try:
        runner.pause(False)
        runner.run_venue("polymarket", runner.request("polymarket"))
        state = reopened._rows("SELECT data FROM sync_state WHERE venue='polymarket'")[0]["data"]
        assert state["state"] == "partial"
        assert state["error_count"] >= 1
        assert state["last_success"] == "previous-success"
        assert reopened._rows("SELECT active FROM events WHERE id='polymarket:unseen'")[0]["active"]
    finally:
        runner.close()
        reopened.close()


def test_exhausted_formal_job_is_not_prepared_forever(store, tmp_path, monkeypatch):
    from oddsfox.explanations import AnalysisEngine

    contract = import_capture(store, "polymarket", json.dumps(pm()).encode())
    engine = AnalysisEngine(store, tmp_path, generator=lambda *a: "", manifest={"test": True})
    job = store.enqueue("interpret", [contract], {"test": True})
    for _ in range(3):
        store.claim_job(job)
        store.fail_job(job, "failed strict evidence validation")
    calls = []

    def prepare(*args):
        calls.append(args)
        return job

    monkeypatch.setattr("oddsfox.compiler.prepare_compile", prepare)
    event = {"data": {"combination": False, "contracts": [contract]}}
    explanation = {"data": {"families": ["instantaneous_threshold"]}}
    engine.compile_step(event, explanation)
    engine.compile_step(event, explanation)
    assert len(calls) == 1
    assert (
        "failed strict evidence validation" in store.list("compilation_issue")[0]["data"]["reason"]
    )


@pytest.mark.parametrize("venue", ["polymarket_us", "unknown", "", "POLYMARKET"])
def test_removed_and_unknown_venues_rejected_without_work(store, monkeypatch, venue):
    from fastapi.testclient import TestClient

    from oddsfox.app import create_app
    from oddsfox.ingest import fetch

    with monkeypatch.context() as network:
        network.setattr(
            httpx.Client,
            "send",
            lambda *a, **k: pytest.fail("invalid venue caused network work"),
        )
        runner = SyncRunner(store)
        try:
            with pytest.raises(ValueError):
                runner.request(venue)
            with pytest.raises(ValueError):
                fetch(store, venue, ["one"])
            with pytest.raises(ValueError):
                import_capture(store, venue, json.dumps(pm()).encode())
        finally:
            runner.close()
    assert store._rows("SELECT * FROM jobs") == []
    assert store.list("contract", True) == []

    monkeypatch.setattr(SyncRunner, "start", lambda *a, **k: None)
    monkeypatch.setattr("oddsfox.app.fetch", lambda *a, **k: pytest.fail("capture dispatched"))
    with TestClient(
        create_app(store, auto_sync=False, token="fixture-session"),
        base_url="http://127.0.0.1:8777",
        headers={"X-Oddsfox-Token": "fixture-session"},
    ) as client:
        for route, body in [
            ("/api/sync", {"venues": ["kalshi", venue]}),
            ("/api/capture", {"platform": venue, "native_ids": ["one"]}),
            ("/api/import", {"platform": venue, "payload": json.dumps(pm())}),
        ]:
            assert client.post(route, json=body).status_code == 422
        assert client.get("/api/events", params={"venue": venue}).status_code == 422
    assert store._rows("SELECT * FROM jobs") == []
    assert store.list("contract", True) == []


def test_sync_status_and_default_request_contain_only_supported_venues(store, monkeypatch):
    from fastapi.testclient import TestClient

    from oddsfox.app import create_app

    monkeypatch.setattr(SyncRunner, "start", lambda *a, **k: None)
    with TestClient(
        create_app(store, auto_sync=False, token="fixture-session"),
        base_url="http://127.0.0.1:8777",
        headers={"X-Oddsfox-Token": "fixture-session"},
    ) as client:
        assert {r["venue"] for r in client.get("/api/sync").json()["venues"]} == {
            "kalshi",
            "polymarket",
        }
        response = client.post("/api/sync", json={})
        assert response.status_code == 202
        jobs = store._rows("SELECT * FROM jobs")
        assert len(jobs) == 2
        assert {j["config"]["venue"] for j in jobs} == {"kalshi", "polymarket"}
        assert all(j["stage"] == "discover" and j["state"] == "pending" for j in jobs)


@pytest.mark.parametrize("venue", ["polymarket_us", "unknown"])
@pytest.mark.parametrize("command", ["sync", "capture", "import"])
def test_cli_rejects_removed_or_unknown_venue_before_opening_dataset(tmp_path, venue, command):
    from oddsfox.cli import main

    path = tmp_path / "must-not-exist"
    args = ["--data", str(path), command]
    args += ["--venue", venue] if command == "sync" else [venue, "one"]
    with pytest.raises(SystemExit) as rejected:
        main(args)
    assert rejected.value.code == 2
    assert not path.exists()


@pytest.mark.parametrize(
    "url",
    [
        "https://polymarket.us/rules",
        "https://docs.polymarket.us/rules",
        "https://gateway.polymarket.us/v1/events",
        "https://polymarketexchange.com/files/legal/latest/rulebook",
    ],
)
def test_removed_venue_document_hosts_rejected_before_dns(monkeypatch, url):
    monkeypatch.setattr("socket.getaddrinfo", lambda *a, **k: pytest.fail("removed host resolved"))
    with pytest.raises(ValueError):
        validate_url(url)
