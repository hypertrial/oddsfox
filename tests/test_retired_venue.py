"""Destructive migration regressions: legacy US history and supported-data preservation."""

import hashlib
import json
from pathlib import Path

import duckdb
import pytest

from oddsfox.cli import main
from oddsfox.demo import load_demo, sample
from oddsfox.explanations import chunks
from oddsfox.ir import fingerprint
from oddsfox.store import Store, now

US = "polymarket_us"


def hashes(path):
    return {
        str(p.relative_to(path)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in path.rglob("*")
        if p.is_file()
    }


def seed(path):
    store = Store(path)
    load_demo(store, True)
    supported = store._rows("SELECT * FROM nodes ORDER BY id")
    original = store.list("contract")[0]
    data = original["data"]
    private_text = "Old US-only source text"
    us = store.capture(US, "old", b'{"id":"old"}', {"rules": private_text}, data["metadata"], [])
    unique = store.get(us)["data"]["text_artifacts"]["rules"]
    shared = next(iter(original["data"]["text_artifacts"].values()))
    config = store.insert("analysis_config", "active", {"model": "test"}, [], "CONFIGURED")
    sids = {}
    for venue, contract, artifact in [(US, us, unique), ("polymarket", original["id"], shared)]:
        key = venue + ":old"
        semantic = store.insert(
            "event_semantics",
            key,
            {"venue": venue, "contracts": [contract], "text_artifacts": {"rules": artifact}},
            [contract],
            "CAPTURED",
        )
        sids[venue] = semantic
        store.db.execute(
            "INSERT INTO events VALUES (?,?,?,'Old','Test','00000000000000000000200000.','qualified',true,'run',?,?,?)",
            [
                key,
                venue,
                "old",
                semantic,
                json.dumps(
                    {
                        "contracts": [contract],
                        "volume": {"amount": "200000"},
                        "shared_artifact": shared,
                    }
                ),
                now(),
            ],
        )
        store.db.execute("INSERT INTO event_terms VALUES ('old',?)", [key])
    # An identical shared chunk (same artifact and spans) belongs to both venues.
    shared_sid = store.insert(
        "event_semantics",
        US + ":shared",
        {"venue": US, "contracts": [], "text_artifacts": {"rules": shared}},
        [],
        "CAPTURED",
    )
    chunk_nodes, chunk_jobs = {}, {}
    for sid in [sids[US], sids["polymarket"], shared_sid]:
        for spans in chunks(store, sid):
            key = fingerprint({"spans": spans, "config": config})
            job = store.enqueue("explain", [config], {"chunk": key})
            chunk_jobs.setdefault(sid, []).append(job)
            # Gaps-only outputs have no citations or event edges.
            if store.current("explanation_chunk", key) is None:
                output = store.insert(
                    "explanation_chunk",
                    key,
                    {"facts": [], "gaps": ["Unclear"]},
                    [config],
                    "UNREVIEWED",
                )
                store.claim_job(job)
                store.complete_job(job, output)
            chunk_nodes[key] = store.current("explanation_chunk", key)["id"]
    older_config = store.insert("analysis_config", "older", {"model": "older"}, [], "CONFIGURED")
    pending_key = fingerprint({"spans": list(chunks(store, sids[US]))[0], "config": older_config})
    pending_chunk = store.enqueue("explain", [older_config], {"chunk": pending_key})
    discover = store.enqueue("discover", [], {"venue": US, "generation": "old"})
    store.claim_job(discover)
    capture = store.enqueue("capture", [], {"platform": US, "native_ids": ["old"]})
    store.db.execute(
        "INSERT INTO sync_state VALUES (?,?,0,false,?)",
        [US, discover, json.dumps({"state": "running"})],
    )
    deleted = {us, sids[US], shared_sid}
    for kind, logical in [
        ("discovery_run", discover),
        ("discovery_page", discover + ":0"),
        ("measurement", capture + ":1"),
    ]:
        deleted.add(
            store.insert(
                kind, logical, {"raw_artifact": unique}, [], "CAPTURED", make_current=False
            )
        )
    interpretation = store.insert(
        "interpretation",
        us,
        {"ir": {"contract_version_id": us, "observation": {"quantity": "old"}}},
        [us],
        "PROVISIONAL",
    )
    deleted.add(interpretation)
    for kind, payload, parents in [
        ("review", {"interpretation": interpretation}, [interpretation]),
        ("assertion", {"a": us, "b": original["id"]}, [us, original["id"]]),
        ("suggestions", {"omitted_pending_explanations": [sids[US]]}, [sids["polymarket"]]),
        ("quarantine", {"proposals": [{"a": us}]}, []),
    ]:
        deleted.add(store.insert(kind, "removed-" + kind, payload, parents, "ACCEPTED"))
    group = fingerprint({"quantity": "old"})
    store.db.execute(
        "INSERT INTO comparison_groups VALUES (?, 'signature','running',2,1,0)", [group]
    )
    store.db.execute(
        "INSERT INTO comparison_rows VALUES ('signature','old',?)", [json.dumps({"a": us})]
    )
    store.db.execute("INSERT INTO comparison_groups VALUES ('unrelated','keep','complete',2,1,0)")
    store.db.execute(
        "INSERT INTO comparison_rows VALUES ('keep','keep',?)", [json.dumps({"a": original["id"]})]
    )
    orphan = store.put_artifact(b"unrelated unreferenced file")
    (path / "models").mkdir()
    (path / "models" / "weights").write_bytes(b"leave weights alone")
    store.db.execute("UPDATE metadata SET version=3")
    store.close()
    return {
        "supported": supported,
        "deleted": deleted,
        "unique": unique,
        "shared": shared,
        "orphan": orphan,
        "jobs": {discover, capture, pending_chunk, *chunk_jobs[sids[US]]},
        "shared_jobs": chunk_jobs[shared_sid],
    }


def test_purge_mixed_history_jobs_cross_references_and_shared_chunks(tmp_path):
    path, backup = tmp_path / "data", tmp_path / "backup"
    expected = seed(path)
    before = hashes(path)
    result = Store.migrate(path, backup)
    assert result["removed_nodes"] >= len(expected["deleted"])
    assert result["version"] == 4
    store = Store(path)
    try:
        remaining = {n["id"]: n for n in store._rows("SELECT * FROM nodes")}
        assert not expected["deleted"] & remaining.keys()
        assert all(remaining[n["id"]] == n for n in expected["supported"])
        jobs = {j["id"] for j in store._rows("SELECT * FROM jobs")}
        assert not expected["jobs"] & jobs
        assert set(expected["shared_jobs"]) <= jobs
        assert not store._rows("SELECT * FROM sync_state")
        assert not store._rows("SELECT * FROM attempts WHERE job_id NOT IN (SELECT id FROM jobs)")
        assert store._rows("SELECT id FROM comparison_rows") == [{"id": "keep"}]
        assert not (path / "artifacts" / expected["unique"]).exists()
        assert store.artifact(expected["shared"])
        assert store.artifact(expected["orphan"]) == b"unrelated unreferenced file"
        assert (path / "models" / "weights").read_bytes() == b"leave weights alone"
        store.verify_integrity()
    finally:
        store.close()
    old = Store(backup, _maintenance=True)
    try:
        assert old.version == 3
        assert old._rows("SELECT state FROM jobs WHERE stage='discover'") == [{"state": "running"}]
    finally:
        old.close()
    assert all(hashes(backup)[k] == v for k, v in before.items() if k.startswith("artifacts/"))
    snapshot = hashes(backup)
    assert Store.restore(backup, tmp_path / "restored") == 3
    assert hashes(backup) == snapshot
    with pytest.raises(ValueError, match="migration required"):
        Store(tmp_path / "restored")
    assert Store.migrate(path, tmp_path / "unused") == {"version": 4, "migration_required": False}
    assert not (tmp_path / "unused").exists()


@pytest.mark.parametrize("version", [1, 2, 3])
def test_legacy_startup_never_recovers_jobs_and_cli_migration(tmp_path, version):
    path = tmp_path / "data"
    store = Store(path)
    sample(store, "keep")
    job = store.enqueue("capture", [], {"platform": "polymarket", "native_ids": ["keep"]})
    store.claim_job(job)
    store.db.execute("UPDATE metadata SET version=?", [version])
    store.close()
    before = hashes(path)
    with pytest.raises(ValueError, match="migration required"):
        Store(path)
    assert hashes(path) == before
    assert main(["--data", str(path), "migrate", "--backup", str(tmp_path / "backup")]) == 0
    old = Store(tmp_path / "backup", _maintenance=True)
    try:
        assert old.version == version
        assert old._rows("SELECT state,attempts FROM jobs") == [{"state": "running", "attempts": 1}]
    finally:
        old.close()
    store = Store(path)
    assert store._rows("SELECT state,attempts FROM jobs") == [{"state": "pending", "attempts": 1}]
    store.close()


def test_backup_failure_aborts_purge_and_database_failure_rolls_back(tmp_path, monkeypatch):
    path, backup = tmp_path / "data", tmp_path / "backup"
    seed(path)
    before = hashes(path)
    import oddsfox.migration as migration

    actual = migration.purge_us

    def fail(store):
        actual(store)
        raise RuntimeError("injected before commit")

    monkeypatch.setattr(migration, "purge_us", fail)
    with pytest.raises(RuntimeError, match="injected"):
        Store.migrate(path, backup)
    store = Store(path, _maintenance=True)
    assert store.version == 3
    assert store._rows("SELECT * FROM events WHERE venue=?", [US])
    store.verify_integrity()
    store.close()
    assert all(hashes(path)[k] == v for k, v in before.items() if k != "oddsfox.duckdb")
    monkeypatch.setattr(migration, "purge_us", actual)
    with pytest.raises(ValueError, match="new directory"):
        Store.migrate(path, backup)
    store = Store(path, _maintenance=True)
    orphan = store.put_artifact(b"orphan")
    store.close()
    (path / "artifacts" / orphan).write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="digest mismatch"):
        Store.migrate(path, tmp_path / "bad-backup")
    assert not (tmp_path / "bad-backup").exists()
    db = duckdb.connect(str(path / "oddsfox.duckdb"), read_only=True)
    assert db.execute("SELECT version FROM metadata").fetchone() == (3,)
    db.close()


def test_interrupted_cleanup_resumes_before_processing(tmp_path, monkeypatch):
    path = tmp_path / "data"
    expected = seed(path)
    unlink = Path.unlink
    failed = False

    def interrupt(p, *args, **kwargs):
        nonlocal failed
        if p.parent == path / "artifacts" and not failed:
            failed = True
            unlink(p, *args, **kwargs)
            raise OSError("interrupted after unlink")
        return unlink(p, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", interrupt)
    with pytest.raises(OSError, match="interrupted"):
        Store.migrate(path, tmp_path / "backup")
    monkeypatch.setattr(Path, "unlink", unlink)
    store = Store(path)
    try:
        assert store.version == 4
        assert not store._rows("SELECT * FROM artifact_cleanup")
        assert not (path / "artifacts" / expected["unique"]).exists()
        store.verify_integrity()
    finally:
        store.close()


def test_migrate_refuses_live_writer_and_nested_backup(tmp_path):
    path = tmp_path / "data"
    seed(path)
    store = Store(path, _maintenance=True)
    try:
        with pytest.raises(RuntimeError, match="already running"):
            Store.migrate(path, tmp_path / "backup")
    finally:
        store.close()
    with pytest.raises(ValueError, match="outside"):
        Store.migrate(path, path / "backup")
    assert not (tmp_path / "backup").exists()


def test_removed_suggestions_regenerate_despite_historical_completed_job(tmp_path, monkeypatch):
    from oddsfox.explanations import AnalysisEngine

    path = tmp_path / "data"
    store = Store(path)
    config = store.insert("analysis_config", "active", {}, [])
    semantics = {}
    explanations = {}
    for key, venue in [("kalshi:k", "kalshi"), (US + ":u", US), ("polymarket:p", "polymarket")]:
        sid = store.insert(
            "event_semantics", key, {"venue": venue, "contracts": [], "text_artifacts": {}}, []
        )
        semantics[key] = sid
        explanations[key] = store.insert(
            "explanation", sid, {"facts": [], "entities": [], "dates": []}, [sid, config]
        )
    engine = AnalysisEngine.__new__(AnalysisEngine)
    engine.store, engine.config_id = store, config
    engine.generator = lambda *a: '{"matches":[]}'
    event = {"id": "kalshi:k", "semantic_id": semantics["kalshi:k"]}
    monkeypatch.setattr("oddsfox.explanations.candidates_for", lambda *a: [])
    assert engine.match_step(event, store.get(explanations["kalshi:k"]))
    unrelated = {"id": "polymarket:p", "semantic_id": semantics["polymarket:p"]}
    assert engine.match_step(unrelated, store.get(explanations["polymarket:p"]))
    preserved = store.current("suggestions", semantics["polymarket:p"])
    monkeypatch.setattr(
        "oddsfox.explanations.candidates_for", lambda *a: [{"semantic_id": semantics[US + ":u"]}]
    )
    assert engine.match_step(event, store.get(explanations["kalshi:k"]))
    store.db.execute("UPDATE metadata SET version=3")
    store.close()
    Store.migrate(path, tmp_path / "backup")
    store = Store(path)
    try:
        engine.store = store
        monkeypatch.setattr("oddsfox.explanations.candidates_for", lambda *a: [])
        assert engine.match_step(event, store.get(explanations["kalshi:k"]))
        assert store.current("suggestions", semantics["kalshi:k"]) is not None
        assert store.current("suggestions", semantics["polymarket:p"]) == preserved
    finally:
        store.close()


def test_clean_startup_does_not_scan_all_historical_artifacts(tmp_path, monkeypatch):
    path = tmp_path / "data"
    store = Store(path)
    store.close()

    def no_scan(*args):
        raise AssertionError("no pending cleanup should scan history")

    monkeypatch.setattr(Store, "referenced_artifacts", no_scan)
    store = Store(path)
    store.close()
