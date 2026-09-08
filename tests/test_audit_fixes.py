"""Catalog, restore, and compiler regression coverage."""

import hashlib
import json
from decimal import Decimal, localcontext

import duckdb
import httpx
import pytest
from hypothesis import given
from hypothesis import strategies as st

from oddsfox.catalog import event_detail, event_list
from oddsfox.cli import main
from oddsfox.demo import load_demo, sample
from oddsfox.discovery import save_event
from oddsfox.explanations import AnalysisEngine
from oddsfox.ingest import import_capture
from oddsfox.pipeline import Pipeline
from oddsfox.store import Store, volume_order_key


@pytest.mark.parametrize("field", ["yes_sub_title", "no_sub_title"])
def test_kalshi_outcome_revision_withdraws_accepted_claims(store, field):
    load_demo(store, True)
    contract = next(c for c in store.list("contract") if c["data"]["platform"] == "kalshi")
    raw = json.loads(store.artifact(contract["data"]["payload_artifact"]))
    raw["market"][field] = "Different outcome definition"
    changed = import_capture(store, "kalshi", json.dumps(raw).encode())
    assert changed != contract["id"]
    assert any("Different outcome definition" in t for t in store.source_texts(changed).values())
    assert Pipeline(store).export()["assertions"] == []
    raw["market"]["volume_fp"] = "900000"
    assert import_capture(store, "kalshi", json.dumps(raw).encode()) == changed


def hashes(directory):
    return {
        p.relative_to(directory): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in directory.rglob("*")
        if p.is_file()
    }


@pytest.mark.parametrize("corrupt", [False, True])
def test_restore_preserves_legacy_source_and_only_publishes_verified_copy(tmp_path, corrupt):
    source = tmp_path / "backup"
    store = Store(source)
    ir = sample(store, "a")
    store.close()
    db = duckdb.connect(str(source / "oddsfox.duckdb"))
    db.execute("UPDATE metadata SET version=1")
    for table in (
        "snapshots",
        "semantic_heads",
        "events",
        "event_terms",
        "sync_state",
        "comparison_rows",
        "comparison_groups",
        "lane_state",
    ):
        db.execute(f"DROP TABLE {table}")
    db.close()
    if corrupt:
        next((source / "artifacts").iterdir()).unlink()
    before = hashes(source)
    destination = tmp_path / "restored"
    assert main(["--data", str(destination), "restore", str(source)]) == int(corrupt)
    assert hashes(source) == before
    assert destination.exists() is not corrupt
    if not corrupt:
        with pytest.raises(ValueError, match="migration required"):
            Store(destination)
        Store.migrate(destination, tmp_path / "pre-migration")
        restored = Store(destination)
        try:
            assert restored.get(ir["contract_version_id"])["current"]
            restored.verify_integrity()
        finally:
            restored.close()


@pytest.mark.parametrize("failures", [1, 3])
def test_automatic_compile_retries_until_success_or_exhaustion(tmp_path, monkeypatch, failures):
    store = Store(tmp_path / "retry")
    contract = import_capture(
        store,
        "polymarket",
        json.dumps(
            {
                "id": "a",
                "question": "Value above 10 at noon?",
                "description": "Value above 10 at noon.",
                "outcomes": ["no", "yes"],
            }
        ).encode(),
    )
    engine = AnalysisEngine(
        store, tmp_path / "model", generator=lambda *a: "", manifest={"test": True}
    )
    job = store.enqueue("interpret", [contract], {"test": True})
    monkeypatch.setattr("oddsfox.compiler.prepare_compile", lambda *a: job)
    from oddsfox.ir import fingerprint

    # A pre-fix issue must not strand its still-pending job after upgrade/restart.
    issue_key = fingerprint({"contract": contract, "config": engine.config_id})
    with store.transaction():
        old_issue = store.insert(
            "compilation_issue",
            issue_key,
            {"reason": "old transient failure", "contract": contract},
            [contract, engine.config_id],
            "NEEDS_REVIEW",
        )
    assert store.claim_job(job)
    store.fail_job(job, "old transient failure")
    store.close()
    store = Store(tmp_path / "retry")
    engine = AnalysisEngine(
        store, tmp_path / "model", generator=lambda *a: "", manifest={"test": True}
    )
    assert store._rows("SELECT attempts FROM jobs WHERE id=?", [job])[0]["attempts"] == 1
    calls = [job]

    def run(s, identity, path):
        assert s.claim_job(identity)
        calls.append(identity)
        if len(calls) <= failures:
            s.fail_job(identity, "temporary memory failure")
            raise RuntimeError("temporary memory failure")
        with s.transaction():
            output = s.insert("interpretation", contract, {}, [contract])
            s.complete_job(identity, output)

    monkeypatch.setattr("oddsfox.compiler.run_compile_job", run)
    event = {"data": {"combination": False, "contracts": [contract]}}
    explanation = {"data": {"families": ["instantaneous_threshold"]}}
    try:
        for _ in range(5):
            engine.compile_step(event, explanation)
        state = store._rows("SELECT state,attempts FROM jobs WHERE id=?", [job])[0]
        assert not store.get(old_issue)["current"]
        assert len(calls) == (2 if failures == 1 else 3)
        assert state == {"state": "done" if failures == 1 else "failed", "attempts": len(calls)}
        assert len(store.list("compilation_issue")) == (0 if failures == 1 else 1)
    finally:
        store.close()


def save_volume(store, eid, amount):
    market = {
        "id": eid,
        "question": "Value above 10 at noon?",
        "description": "Value above 10 at noon.",
        "active": True,
        "closed": False,
        "acceptingOrders": True,
        "outcomes": ["no", "yes"],
    }
    event = {"id": eid, "title": eid, "volume": amount, "markets": [market]}
    with httpx.Client(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=market))
    ) as client:
        save_event(
            store,
            client,
            "polymarket",
            event,
            "run",
            store.put_artifact(json.dumps(event).encode()),
            "https://gamma-api.polymarket.com/events/keyset",
        )


def test_catalog_retains_exact_volume_and_paginated_order(store):
    amounts = ["100000.00000000000001", "100000.00000000000002", "200000", None, "0", "100000.0100"]
    for i, amount in enumerate(amounts):
        save_volume(store, str(i), amount)
    expected = ["2", "5", "1", "0", "4", "3"]
    actual = [
        event_list(store, qualification="all", offset=i, limit=1)["items"][0] for i in range(6)
    ]
    assert [r["native_id"] for r in actual] == expected
    for row in actual:
        amount = amounts[int(row["native_id"])]
        assert row["volume"] == amount
        assert event_detail(store, row["id"])["volume"] == amount
    assert Decimal(actual[2]["volume"]) > Decimal(actual[3]["volume"])


def legacy_v2(path, *, corrupt=False):
    dataset = Store(path)
    load_demo(dataset, True)
    save_volume(dataset, "a", "100000.00000000000001")
    save_volume(dataset, "b", "100000.00000000000002")
    nodes = dataset._rows("SELECT * FROM nodes ORDER BY id")
    dataset.close()
    db = duckdb.connect(str(path / "oddsfox.duckdb"))
    db.execute("DROP INDEX event_catalog_order")
    db.execute("ALTER TABLE events ALTER COLUMN volume TYPE DECIMAL(38,12)")
    db.execute("CREATE INDEX event_catalog_order ON events(volume)")
    db.execute("UPDATE metadata SET version=2")
    if corrupt:
        data = json.loads(
            db.execute("SELECT data FROM events WHERE id='polymarket:a'").fetchone()[0]
        )
        data["volume"]["amount"] = "invalid"
        db.execute("UPDATE events SET data=? WHERE id='polymarket:a'", [json.dumps(data)])
    db.close()
    return nodes


def test_indexed_v2_migration_recovers_precision_and_preserves_history(tmp_path):
    path = tmp_path / "v2"
    original = legacy_v2(path)
    Store.migrate(path, tmp_path / "backup")
    dataset = Store(path)
    try:
        assert dataset.db.execute("SELECT version FROM metadata").fetchone() == (4,)
        assert dataset._rows("SELECT * FROM nodes ORDER BY id") == original
        assert [r["native_id"] for r in event_list(dataset)["items"]] == ["b", "a"]
        assert event_detail(dataset, "polymarket:a")["volume"] == "100000.00000000000001"
        assert len(Pipeline(dataset).export()["assertions"]) == 2
        dataset.verify_integrity()
    finally:
        dataset.close()


def test_failed_v2_migration_rolls_back_and_releases_source_lock(tmp_path):
    path = tmp_path / "v2"
    original = legacy_v2(path, corrupt=True)
    with pytest.raises(ValueError, match="volume"):
        Store.migrate(path, tmp_path / "backup")
    db = duckdb.connect(str(path / "oddsfox.duckdb"))
    try:
        assert db.execute("SELECT version FROM metadata").fetchone() == (2,)
        assert db.execute(
            "SELECT data_type FROM information_schema.columns WHERE table_name='events' AND column_name='volume'"
        ).fetchone() == ("DECIMAL(38,12)",)
        assert db.execute(
            "SELECT count(*) FROM duckdb_indexes() WHERE index_name='event_catalog_order'"
        ).fetchone() == (1,)
        assert db.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name='events_v3'"
        ).fetchone() == (0,)
        assert db.execute("SELECT count(*) FROM nodes").fetchone() == (len(original),)
        # Repair the fixture evidence and prove the failed constructor released its lock.
        data = json.loads(
            db.execute("SELECT data FROM events WHERE id='polymarket:a'").fetchone()[0]
        )
        data["volume"]["amount"] = "100000.00000000000001"
        db.execute("UPDATE events SET data=? WHERE id='polymarket:a'", [json.dumps(data)])
    finally:
        db.close()
    Store.migrate(path, tmp_path / "retry-backup")
    retry = Store(path)
    retry.close()


@pytest.mark.parametrize("lockless", [False, True])
def test_restore_stopped_backup_without_mutation(store, tmp_path, lockless):
    sample(store, "a")
    backup = tmp_path / "backup"
    store.backup(backup)
    if lockless:
        (backup / "writer.lock").unlink()
    before = hashes(backup)
    Store.restore(backup, tmp_path / "restored")
    assert hashes(backup) == before
    assert not list(tmp_path.glob(".oddsfox-restore-*"))


def test_restore_rejects_live_nested_and_existing_destinations(store, tmp_path):
    sample(store, "a")
    before = hashes(store.directory)
    with pytest.raises(RuntimeError, match="stop"):
        Store.restore(store.directory, tmp_path / "restored")
    for destination in (store.directory / "nested", tmp_path):
        with pytest.raises(ValueError, match="new directory"):
            Store.restore(store.directory, destination)
    assert hashes(store.directory) == before
    assert not (tmp_path / "restored").exists()


def test_api_correction_preserves_derivations_and_rejects_mismatch(store):
    from fastapi.testclient import TestClient

    from oddsfox.app import create_app

    p = Pipeline(store)
    a = sample(store, "a", source="Reviewed alias")
    b = sample(store, "b")
    p.register(
        "canonical",
        b["observation"],
        "reviewer",
        "checked alias",
        [{"definition": a["observation"], "unit_factor": "1"}],
    )
    original = store.get(p.interpret(json.dumps(a)))["data"]
    client = TestClient(
        create_app(store, token="fixture", auto_sync=False), base_url="http://127.0.0.1:8777"
    )
    edited = json.loads(json.dumps(original["ir"]))
    edited["predicate"]["threshold"] = "200000"
    body = {"ir": edited, "derivations": original["derivations"]}
    assert (
        client.post("/api/interpret", json=body, headers={"X-Oddsfox-Token": "fixture"}).status_code
        == 200
    )
    edited["observation"]["source"] = "Unreviewed new source"
    assert (
        client.post("/api/interpret", json=body, headers={"X-Oddsfox-Token": "fixture"}).status_code
        == 422
    )


def test_restore_recovers_wal_without_changing_source(tmp_path):
    import subprocess
    import sys

    source = tmp_path / "interrupted"
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import os,sys; from oddsfox.store import Store; from oddsfox.demo import sample; s=Store(sys.argv[1]); sample(s,'wal'); os._exit(0)",
            str(source),
        ],
        check=True,
    )
    assert (source / "oddsfox.duckdb.wal").exists()
    before = hashes(source)
    Store.restore(source, tmp_path / "restored")
    assert hashes(source) == before
    restored = Store(tmp_path / "restored")
    try:
        assert restored.current("contract", "polymarket:wal")
        restored.verify_integrity()
    finally:
        restored.close()


@given(st.lists(st.tuples(st.integers(0, 10**26 - 1), st.integers(0, 10**200 - 1)), max_size=20))
def test_exact_volume_key_preserves_numeric_order_independent_of_context(parts):
    values = [f"{whole}.{fraction:0200d}" for whole, fraction in parts]
    values += ["-0", "0.000", "1", "1.00", "1E-200", "99999999999999999999999999.999"]
    with localcontext() as context:
        context.prec = 6
        ordered = sorted(values, key=volume_order_key)
    assert list(map(Decimal, ordered)) == sorted(map(Decimal, values))
    assert volume_order_key("1") == volume_order_key("1.000")
    assert volume_order_key("-0") == volume_order_key("0")
    assert volume_order_key(None) is None


@pytest.mark.parametrize("amount", ["NaN", "Infinity", "-1", "1e26", "bad"])
def test_volume_order_key_rejects_values_outside_its_order_contract(amount):
    with pytest.raises(ValueError, match="volume"):
        volume_order_key(amount)
