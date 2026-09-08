import json

import duckdb
import pytest

from oddsfox.demo import load_demo, sample
from oddsfox.pipeline import Pipeline
from oddsfox.store import StaleInput, Store


def test_full_workflow_and_source_revision_withdrawal(store):
    result = load_demo(store, approve=False)
    pipeline = Pipeline(store)
    assert pipeline.compare()
    assert pipeline.publish() == []
    for identity in result["interpretations"]:
        pipeline.review(identity, "reviewer", "checked exact quotations", True, True)
    assert len(pipeline.publish()) == 2
    original = pipeline.export()["assertions"]
    old_proposals = pipeline.compare()
    sample(store, "example-high", "160000")
    assert pipeline.export()["assertions"] == []
    assert len(pipeline.export(True)["assertions"]) == len(original)
    with pytest.raises(StaleInput):
        pipeline.publish(old_proposals)


@pytest.mark.parametrize("change", ["approval", "registry", "configuration"])
def test_all_dependency_types_withdraw_transitively(store, change):
    result = load_demo(store, True)
    pipeline = Pipeline(store)
    assert len(pipeline.export()["assertions"]) == 2
    if change == "approval":
        pipeline.review(
            result["interpretations"][0], "test", "withdraw mistaken interpretation", False
        )
    elif change == "registry":
        observation = store.current("registry", "example-btc-2027")["data"]["definition"]
        pipeline.register(
            "example-btc-2027",
            observation | {"revision_policy": "revised value"},
            "test",
            "registry revision",
        )
    else:
        pipeline.configure({"prompt": "changed/2"})
    assert pipeline.export()["assertions"] == []
    assert pipeline.publish() == []


def test_forged_registry_and_unresolved_approval_rejected(store):
    raw = sample(store, "one")
    pipeline = Pipeline(store)
    identity = pipeline.interpret(json.dumps(raw))
    with pytest.raises(ValueError, match="complete"):
        pipeline.review(identity, "test", "not enough evidence", True, True)
    raw["observation"].update(
        canonical_observation_id="forged", canonical_observation_version="fake"
    )
    with pytest.raises(ValueError, match="canonical identity"):
        pipeline.interpret(json.dumps(raw))


def test_artifact_atomicity_idempotent_capture_and_backup(store, tmp_path):
    first = sample(store, "one")
    second = sample(store, "one")
    assert first["contract_version_id"] == second["contract_version_id"]
    assert len(store.list("contract", True)) == 1
    assert len(store.status()["refreshes"]) == 2
    destination = tmp_path / "backup"
    store.backup(destination)
    restored = Store(destination)
    try:
        assert restored.source_texts(first["contract_version_id"]) == store.source_texts(
            first["contract_version_id"]
        )
    finally:
        restored.close()
    with pytest.raises(RuntimeError, match="already running"):
        Store(store.directory)


def test_job_completion_rollback_and_restart(tmp_path):
    store = Store(tmp_path / "data")
    ir = sample(store, "one")
    inputs = [ir["contract_version_id"]]
    job = store.enqueue("test", inputs, {"compiler": "one"})
    assert store.enqueue("test", inputs, {"compiler": "one"}) == job
    assert store.enqueue("test", inputs, {"compiler": "two"}) != job
    assert store.claim_job(job)
    store.close()
    store = Store(tmp_path / "data")
    try:
        assert store.claim_job(job)
        sample(store, "one", "300000")
        with pytest.raises(StaleInput), store.transaction():
            output = store.insert("test", "output", {}, [], make_current=False)
            store.complete_job(job, output)
        assert store.list("test", True) == []
    finally:
        store.close()


def test_parquet_export_contains_only_current_accepted(store, tmp_path):
    load_demo(store, True)
    output = tmp_path / "accepted.parquet"
    Pipeline(store).export_parquet(output)
    conn = duckdb.connect()
    try:
        assert (
            conn.execute("SELECT count(*) FROM read_parquet(?)", [str(output)]).fetchone()[0] == 2
        )
    finally:
        conn.close()


def test_manual_correction_can_restore_content_without_restoring_review(store):
    result = load_demo(store, True)
    pipeline = Pipeline(store)
    original_id = result["interpretations"][0]
    original = store.get(original_id)["data"]["ir"]
    original_id = pipeline.interpret(json.dumps(original))
    pipeline.review(original_id, "reviewer", "reviewed original manual candidate", True, True)
    assert len(pipeline.publish()) == 2
    corrected = json.loads(json.dumps(original))
    corrected["predicate"]["threshold"] = "200000"
    correction_id = pipeline.interpret(json.dumps(corrected))
    restored_id = pipeline.interpret(json.dumps(original))
    assert restored_id not in {original_id, correction_id}
    assert pipeline.interpret(json.dumps(original)) == restored_id
    assert store.get(original_id)["current"] is False
    assert store.get(correction_id)["current"] is False
    assert pipeline.export()["assertions"] == []
    assert pipeline.publish() == []
    pipeline.review(restored_id, "reviewer", "reviewed restored interpretation anew", True, True)
    assert len(pipeline.publish()) == 2


def test_rebound_alias_keeps_rule_dependency(store):
    p = Pipeline(store)
    a = sample(store, "a", "150000", source="Reviewed alias")
    b = sample(store, "b", "100000")
    rule = p.register(
        "alias",
        b["observation"],
        "test",
        "source alias",
        [{"definition": a["observation"], "unit_factor": "1"}],
    )
    normalized = store.get(p.interpret(json.dumps(a)))["data"]
    canonical = p.register("preferred", b["observation"], "test", "preferred identity")
    for raw in (normalized["ir"], b):
        raw["observation"].update(
            canonical_observation_id="preferred", canonical_observation_version=canonical
        )
    ai = p.interpret(json.dumps(normalized["ir"]), derivations=normalized["derivations"])
    bi = p.interpret(json.dumps(b))
    for identity in (ai, bi):
        p.review(identity, "test", "checked exact evidence", True, True)
    assert len(p.publish()) == 2
    p.register("alias", store.get(rule)["data"]["definition"], "test", "withdraw alias")
    assert store.get(ai)["current"] is False
    assert p.export()["assertions"] == []
    with pytest.raises(ValueError):
        p.interpret(json.dumps(normalized["ir"]), derivations=normalized["derivations"])


def test_unreviewed_intermediate_does_not_suppress_reviewed_pair(store):
    p = Pipeline(store)
    raws = [
        sample(store, name, threshold) for name, threshold in [("a", "3"), ("c", "1"), ("b", "2")]
    ]
    p.register("obs", raws[0]["observation"], "test", "same observation")
    for i, raw in enumerate(raws):
        identity = p.interpret(json.dumps(raw))
        if i < 2:
            p.review(identity, "test", "checked exact evidence", True, True)
    claims = p.compare()
    endpoints = {r["contract_version_id"] for r in raws[:2]}
    assert {c["scope"] for c in claims if {c["a"], c["b"]} == endpoints} == {
        "OBSERVED_EVENT",
        "SETTLEMENT_OUTCOME",
    }
    assert len(p.publish()) == 2


def test_nonadjacent_settlement_is_emitted_but_observed_edges_stay_sparse(store):
    p = Pipeline(store)
    raws = [
        sample(store, name, threshold) for name, threshold in [("a", "3"), ("b", "2"), ("c", "1")]
    ]
    p.register("obs", raws[0]["observation"], "test", "same observation")
    for raw in raws:
        p.interpret(json.dumps(raw))
    claims = p.compare()
    assert sum(c["scope"] == "OBSERVED_EVENT" for c in claims) == 2
    assert sum(c["scope"] == "SETTLEMENT_OUTCOME" for c in claims) == 3


@pytest.mark.parametrize("rule_kind", ["unknown", "wrong-kind", "stale"])
def test_referenced_derivation_rule_must_exist_and_be_current(store, rule_kind):
    p = Pipeline(store)
    raw = sample(store, "a")
    rule = p.register("obs", raw["observation"], "test", "reviewed observation")
    if rule_kind == "unknown":
        rule = "unknown-rule"
    elif rule_kind == "wrong-kind":
        rule = raw["contract_version_id"]
    else:
        p.register("obs", raw["observation"], "test", "revised observation")
    pointer = "/predicate/threshold"
    spans = raw["field_evidence"][pointer]["source_spans"]
    raw["field_evidence"][pointer] = {"source_spans": [], "derivation_ref": "derived"}
    with pytest.raises(ValueError):
        p.interpret(
            json.dumps(raw),
            derivations={
                "derived": {
                    "pointer": pointer,
                    "value": raw["predicate"]["threshold"],
                    "source_spans": spans,
                    "reviewed_rule": rule,
                }
            },
        )
    assert store.list("interpretation") == []


def test_unused_derivation_does_not_create_dependency(store):
    raw = sample(store, "a")
    identity = Pipeline(store).interpret(
        json.dumps(raw), derivations={"unused": {"reviewed_rule": "unknown"}}
    )
    assert store.get(identity)["current"]


def test_derivation_rule_withdrawal_at_commit_rejects_interpretation(store, monkeypatch):
    p = Pipeline(store)
    raw = sample(store, "a")
    rule = p.register("obs", raw["observation"], "test", "reviewed observation")
    pointer = "/predicate/threshold"
    spans = raw["field_evidence"][pointer]["source_spans"]
    raw["field_evidence"][pointer] = {"source_spans": [], "derivation_ref": "derived"}
    original = store.insert

    def withdraw_before_insert(kind, *args, **kwargs):
        if kind == "interpretation":
            store.invalidate(rule, "withdrawn between validation and insertion")
        return original(kind, *args, **kwargs)

    monkeypatch.setattr(store, "insert", withdraw_before_insert)
    with pytest.raises(StaleInput):
        p.interpret(
            json.dumps(raw),
            derivations={
                "derived": {
                    "pointer": pointer,
                    "value": raw["predicate"]["threshold"],
                    "source_spans": spans,
                    "reviewed_rule": rule,
                }
            },
        )
    assert store.list("interpretation", True) == []
