import json

import pytest

from oddsfox.consensus import (
    HUMAN_REVIEW,
    LOCAL_MODEL_CONSENSUS,
    accepted,
    approve_interpretation,
)
from oddsfox.demo import load_demo
from oddsfox.evaluation import evaluate
from oddsfox.ir import SemanticIR, fingerprint
from oddsfox.judge import (
    ContractBallot,
    collect_unanimous,
    disjoint_panels,
    gold_claim_from_pair,
    panel_config,
    parse_ballot,
    persist_config,
)
from oddsfox.pipeline import Pipeline
from oddsfox.validation import freeze_corpus, scans_complete, stratified_sample, write_bundle


def manifest(name, family):
    return {
        "identity": name,
        "family": family,
        "weight_revision": f"rev-{name}",
        "files": {"w.safetensors": name},
        "quantization": {"bits": 4},
        "chat_template_sha256": "0" * 64,
        "chat_template_chars": 8,
        "runtime": "0",
        "outlines": "0",
        "platform": "test",
    }


PRODUCERS = [manifest(f"p{i}", f"family-p{i}") for i in range(3)]
EVALUATORS = [manifest(f"e{i}", f"family-e{i}") for i in range(3)]


def contract_ballot(**updates):
    data = {
        "eligible": True,
        "family": "instantaneous_threshold",
        "quantity": "BTC price",
        "source": "Example reference feed",
        "instrument_or_series": "BTC/USD",
        "unit": "USD",
        "timestamp": "2027-01-01T00:00:00Z",
        "timezone": "UTC",
        "measurement_method": "instantaneous",
        "precision": "0.01",
        "revision_policy": "first published value",
        "canonical_observation_id": None,
        "canonical_observation_version": None,
        "comparator": "GTE",
        "threshold": "150000",
        "resolution_source": "Example reference feed",
        "cutoff": "2027-01-02T00:00:00Z",
        "rounding": "none",
        "missing_data_policy": "cancel if absent",
        "cancellation_policy": "return stake",
        "exceptional_outcome_policy": "cancel",
        "dispute_policy": "manual adjudication",
        "clarification_policy": "recompile before settlement",
        "unknowns": [],
        "citations": [],
    }
    data.update(updates)
    return json.dumps(data)


def test_panel_overlap_and_same_family_rejected():
    with pytest.raises(ValueError, match="identities"):
        disjoint_panels(PRODUCERS, PRODUCERS)
    overlap = [manifest("p0", "family-x")] + EVALUATORS[1:]
    with pytest.raises(ValueError, match="identities"):
        disjoint_panels(PRODUCERS, overlap)
    with pytest.raises(ValueError, match="families"):
        disjoint_panels(PRODUCERS, [manifest("e0", "family-p0")] + EVALUATORS[1:])
    with pytest.raises(ValueError, match="families"):
        panel_config(
            "producer",
            [manifest("p0", "shared"), manifest("p1", "shared"), manifest("p2", "other")],
        )
    with pytest.raises(ValueError, match="revisions"):
        disjoint_panels(
            PRODUCERS,
            [manifest("e0", "family-e0") | {"weight_revision": "rev-p0"}] + EVALUATORS[1:],
        )
    with pytest.raises(ValueError, match="three"):
        panel_config("producer", PRODUCERS[:2])


def test_unanimous_and_dissent_and_timeout(store, tmp_path):
    config_id = persist_config(store, "evaluator-panel", panel_config("evaluator", EVALUATORS))
    paths = [tmp_path / m["identity"] for m in EVALUATORS]
    for path in paths:
        path.mkdir()

    def agreeing(prompt, schema):
        return contract_ballot()

    result = collect_unanimous(
        store,
        kind="contract",
        subject="s1",
        logical="agree",
        config_id=config_id,
        paths=paths,
        prompt="p",
        schema=ContractBallot.model_json_schema(),
        generator=agreeing,
        manifests=EVALUATORS,
    )
    assert result["label"]["threshold"] == "150000"
    assert result["abstention"] is None

    def dissenting(prompt, schema):
        if not hasattr(dissenting, "n"):
            dissenting.n = 0
        dissenting.n += 1
        return contract_ballot(threshold="1" if dissenting.n == 2 else "150000")

    result = collect_unanimous(
        store,
        kind="contract",
        subject="s2",
        logical="dissent",
        config_id=config_id,
        paths=paths,
        prompt="p",
        schema=ContractBallot.model_json_schema(),
        generator=dissenting,
        manifests=EVALUATORS,
    )
    assert result["label"] is None
    assert result["disagreement"]

    def boom(prompt, schema):
        raise TimeoutError("budget")

    result = collect_unanimous(
        store,
        kind="contract",
        subject="s3",
        logical="timeout",
        config_id=config_id,
        paths=paths,
        prompt="p",
        schema=ContractBallot.model_json_schema(),
        generator=boom,
        manifests=EVALUATORS,
    )
    assert result["abstention"] == "timeout"
    frozen = store.current("consensus_label_set", f"timeout:{config_id}")["id"]
    collect_unanimous(
        store,
        kind="contract",
        subject="s3",
        logical="timeout",
        config_id=config_id,
        paths=paths,
        prompt="p",
        schema=ContractBallot.model_json_schema(),
        generator=agreeing,
        manifests=EVALUATORS,
    )
    assert store.current("consensus_label_set", f"timeout:{config_id}")["id"] == frozen


def test_malformed_ballot_abstains(store, tmp_path):
    config_id = persist_config(store, "evaluator-panel", panel_config("evaluator", EVALUATORS))
    paths = [tmp_path / m["identity"] for m in EVALUATORS]
    for path in paths:
        path.mkdir()

    def bad(prompt, schema):
        return "{not json"

    result = collect_unanimous(
        store,
        kind="contract",
        subject="s4",
        logical="bad",
        config_id=config_id,
        paths=paths,
        prompt="p",
        schema=ContractBallot.model_json_schema(),
        generator=bad,
        manifests=EVALUATORS,
    )
    assert result["label"] is None
    with pytest.raises(ValueError):
        parse_ballot("contract", "{")

    def extra_field(prompt, schema):
        payload = json.loads(contract_ballot())
        payload["invented"] = True
        return json.dumps(payload)

    result = collect_unanimous(
        store,
        kind="contract",
        subject="s4b",
        logical="extra",
        config_id=config_id,
        paths=paths,
        prompt="p",
        schema=ContractBallot.model_json_schema(),
        generator=extra_field,
        manifests=EVALUATORS,
    )
    assert result["label"] is None
    assert result["abstention"]


def test_metrics_v4_rejects_human_flags_and_reports_agreement():
    bench = {
        "benchmark_id": "synthetic-test",
        "revision": "1",
        "label_version": "1",
        "labeling_guide": "exact-string-set/1",
        "split": "development",
        "independent_human_labels": False,
        "label_source": "local_unanimous_consensus",
        "metric_definition_version": "oddsfox-metrics/4",
        "label_abstentions": [],
        "contracts": [
            {
                "id": c,
                "venue": "polymarket" if c == "a" else "kalshi",
                "template": "test",
                "eligible": True,
            }
            for c in "abc"
        ],
        "comparisons": [
            {"a": a, "b": b, "scope": "OBSERVED_EVENT", "conditions": []}
            for a, b in [("a", "b"), ("b", "c"), ("a", "c")]
        ],
        "gold_claims": [
            {
                "a": "a",
                "b": "b",
                "relation": "IMPLIES",
                "scope": "OBSERVED_EVENT",
                "conditions": [],
            }
        ],
        "stage_labels": [],
    }
    run = {
        "benchmark_hash": fingerprint(bench),
        "scored_before_case_review": True,
        "pipeline_configuration": {"compiler": "test", "model": None, "schema": "1.0.0"},
        "acceptance_policy": {
            "version": "test/1",
            "allowed_scopes": ["OBSERVED_EVENT"],
            "allowed_settlement_states": ["CONDITIONAL"],
            "require_resolved": True,
            "normalization": "exact-string-set/1",
        },
        "proposals": [
            {
                "a": "a",
                "b": "b",
                "relation": "IMPLIES",
                "scope": "OBSERVED_EVENT",
                "conditions": [],
                "proof": {"state": "PROVEN_UNDER_PREMISES"},
                "settlement": {"state": "CONDITIONAL"},
                "interpretation_assessments": ["SUPPORTED", "SUPPORTED"],
            }
        ],
        "stage_outputs": [],
        "outcomes": [],
        "complete_zero_error_scans": True,
        "unanimous_cross_venue_relationship": True,
        "unanimous_near_match_rejection": True,
        "zero_accepted_known_false_equivalences": True,
        "provenance_invalidation": True,
    }
    report = evaluate(bench, run)
    assert report["metric_definition_version"] == "oddsfox-metrics/4"
    assert report["release_gates"]["independent_human_labels"] is False
    assert "selected_agreement_target" in report["release_gates"]
    with pytest.raises(ValueError, match="human-validation"):
        evaluate(bench, run | {"cross_venue_human_validation": True})
    human = dict(bench)
    human["independent_human_labels"] = True
    with pytest.raises(ValueError, match="independent_human_labels"):
        evaluate(human, run | {"benchmark_hash": fingerprint(human)})
    v3 = dict(bench)
    human_v3 = {**v3, "metric_definition_version": "oddsfox-metrics/3"}
    with pytest.raises(ValueError, match="oddsfox-metrics/4"):
        evaluate(human_v3, run | {"benchmark_hash": fingerprint(human_v3)})
    missing_source = dict(bench)
    del missing_source["label_source"]
    with pytest.raises(ValueError, match="label_source"):
        evaluate(missing_source, run | {"benchmark_hash": fingerprint(missing_source)})


def test_incomplete_scan_cannot_freeze(store):
    from oddsfox.sync import SyncRunner

    SyncRunner(store).close()
    with pytest.raises(ValueError, match="partial or failed"):
        scans_complete(store)


def test_stratified_sample_is_deterministic_over_250():
    contracts = [
        {
            "id": f"{venue}-{i:03d}",
            "venue": venue,
            "template": f"t{i % 5}",
            "eligible": True,
            "event_id": f"{venue}-{i}",
            "title": f"Title {i}",
            "lexical": f"title {i}",
        }
        for venue in ("kalshi", "polymarket")
        for i in range(180)
    ]
    first, meta = stratified_sample(contracts)
    second, _ = stratified_sample(contracts)
    assert meta["inventory"] == 360
    assert len(first) == 250
    assert [c["id"] for c in first] == [c["id"] for c in second]
    assert {c["venue"] for c in first} == {"kalshi", "polymarket"}


def test_write_once_bundle(tmp_path):
    directory = tmp_path / "bundle"
    write_bundle(directory, {"corpus.json": {"ok": True}})
    with pytest.raises(FileExistsError):
        write_bundle(directory, {"corpus.json": {"ok": True}})


def test_human_veto_blocks_consensus_publication(store, tmp_path):
    demo = load_demo(store)
    interpretation = store.get(demo["interpretations"][0])
    ir = SemanticIR.model_validate(interpretation["data"]["ir"])
    from oddsfox.consensus import canonicalize_contract_from_ir

    expected = canonicalize_contract_from_ir(ir)
    paths = [tmp_path / m["identity"] for m in PRODUCERS]
    for path in paths:
        path.mkdir()

    def gen(prompt, schema):
        return json.dumps(expected)

    approve_interpretation(store, interpretation["id"], paths, generator=gen, manifests=PRODUCERS)
    approval = store.current("consensus_approval", interpretation["id"])
    assert approval["data"]["acceptance_basis"] == LOCAL_MODEL_CONSENSUS
    pipeline = Pipeline(store, allow_consensus=True)
    pipeline.review(
        interpretation["id"],
        "operator",
        "veto",
        False,
        False,
    )
    with pytest.raises(ValueError, match="vetoes"):
        approve_interpretation(
            store, interpretation["id"], paths, generator=gen, manifests=PRODUCERS
        )
    published = Pipeline(store, allow_consensus=True).publish()
    assert published == []


def test_consensus_can_publish_when_enabled(store, tmp_path):
    demo = load_demo(store)
    paths = [tmp_path / m["identity"] for m in PRODUCERS]
    for path in paths:
        path.mkdir()
    for identity in demo["interpretations"][:2]:
        ir = SemanticIR.model_validate(store.get(identity)["data"]["ir"])
        from oddsfox.consensus import canonicalize_contract_from_ir

        expected = canonicalize_contract_from_ir(ir)

        def gen(prompt, schema, expected=expected):
            return json.dumps(expected)

        approve_interpretation(store, identity, paths, generator=gen, manifests=PRODUCERS)
    published = Pipeline(store, allow_consensus=True).publish()
    assert published
    export = Pipeline(store).export()
    assert export["assertions"][0]["data"]["acceptance_basis"] == LOCAL_MODEL_CONSENSUS
    assert export["consensus_approvals"]
    assert Pipeline(store, allow_consensus=False).publish() == []


def test_completed_ballots_resume_without_regenerating(store, tmp_path):
    config_id = persist_config(store, "evaluator-panel", panel_config("evaluator", EVALUATORS))
    paths = [tmp_path / m["identity"] for m in EVALUATORS]
    for path in paths:
        path.mkdir()
    calls = []

    def agreeing(prompt, schema):
        calls.append(1)
        return contract_ballot()

    first = collect_unanimous(
        store,
        kind="contract",
        subject="replay",
        logical="replay",
        config_id=config_id,
        paths=paths,
        prompt="p",
        schema=ContractBallot.model_json_schema(),
        generator=agreeing,
        manifests=EVALUATORS,
    )
    assert first["label"]["threshold"] == "150000"
    assert len(calls) == 3
    second = collect_unanimous(
        store,
        kind="contract",
        subject="replay",
        logical="replay",
        config_id=config_id,
        paths=paths,
        prompt="p",
        schema=ContractBallot.model_json_schema(),
        generator=agreeing,
        manifests=EVALUATORS,
    )
    assert len(calls) == 3
    assert second["label"] == first["label"]
    assert second["ballots"] == first["ballots"]


def test_partial_or_error_scans_cannot_freeze(store):
    from oddsfox.catalog import set_sync_state
    from oddsfox.sync import SyncRunner

    SyncRunner(store).close()
    set_sync_state(
        store,
        "kalshi",
        state="complete",
        last_success="2026-01-01T00:00:00Z",
        error_count=0,
        errors=[],
    )
    set_sync_state(
        store,
        "polymarket",
        state="partial",
        last_success="2026-01-01T00:00:00Z",
        error_count=1,
        errors=["page failed"],
    )
    with pytest.raises(ValueError, match="partial or failed"):
        scans_complete(store)
    with pytest.raises(ValueError, match="partial or failed"):
        freeze_corpus(store, [], generator=lambda *a: contract_ballot(), manifests=EVALUATORS)
    set_sync_state(
        store,
        "polymarket",
        state="complete",
        last_success="2026-01-01T00:00:00Z",
        error_count=0,
        errors=["page failed"],
    )
    with pytest.raises(ValueError, match="partial or failed"):
        scans_complete(store)
    set_sync_state(store, "polymarket", errors=[], last_success=None)
    with pytest.raises(ValueError, match="successful timestamp"):
        scans_complete(store)


def test_empty_gold_is_label_abstention_not_perfect_agreement():
    bench = {
        "benchmark_id": "synthetic-test",
        "revision": "1",
        "label_version": "local-unanimous-consensus/1",
        "labeling_guide": "exact-string-set/1",
        "split": "held-out-consensus",
        "independent_human_labels": False,
        "label_source": "local_unanimous_consensus",
        "metric_definition_version": "oddsfox-metrics/4",
        "label_abstentions": [
            {"id": "a", "reason": "dissent"},
            {"id": "a", "reason": "dissent"},
            {"id": "b", "reason": "timeout"},
            {"id": "c", "reason": "invalid ballot"},
            {"id": "a/b", "reason": "dissent"},
        ],
        "contracts": [
            {
                "id": c,
                "venue": "polymarket" if c == "a" else "kalshi",
                "template": "test",
                "eligible": True,
            }
            for c in "abc"
        ],
        "comparisons": [
            {"a": a, "b": b, "scope": "OBSERVED_EVENT", "conditions": []}
            for a, b in [("a", "b"), ("b", "c"), ("a", "c")]
        ],
        "gold_claims": [],
        "stage_labels": [],
    }
    run = {
        "benchmark_hash": fingerprint(bench),
        "scored_before_case_review": True,
        "pipeline_configuration": {"compiler": "test", "model": None, "schema": "1.0.0"},
        "acceptance_policy": {
            "version": "test/1",
            "allowed_scopes": ["OBSERVED_EVENT"],
            "allowed_settlement_states": ["CONDITIONAL"],
            "require_resolved": True,
            "normalization": "exact-string-set/1",
        },
        "proposals": [
            {
                "a": "a",
                "b": "b",
                "relation": "IMPLIES",
                "scope": "OBSERVED_EVENT",
                "conditions": [],
                "proof": {"state": "PROVEN_UNDER_PREMISES"},
                "settlement": {"state": "CONDITIONAL"},
                "interpretation_assessments": ["SUPPORTED", "SUPPORTED"],
            }
        ],
        "stage_outputs": [],
        "outcomes": [],
        "complete_zero_error_scans": True,
        "unanimous_cross_venue_relationship": False,
        "unanimous_near_match_rejection": True,
        "zero_accepted_known_false_equivalences": True,
        "provenance_invalidation": True,
    }
    report = evaluate(bench, run)
    assert report["consensus"]["label_abstentions"] == 3
    assert report["consensus"]["label_coverage"] == 0
    assert report["release_gates"]["evaluator_label_coverage"] is False
    assert report["relationships"]["selected_precision"]["value"] == 0
    assert report["relationships"]["selected_recall"]["value"] is None
    assert report["release_gates"]["selected_agreement_target"] is False
    assert (
        gold_claim_from_pair(
            "a",
            "b",
            {
                "relationship": "NEAR_MATCH",
                "scope": "OBSERVED_EVENT",
                "conditions": [],
                "settlement_compatibility": "DIFFERENT",
                "near_match_differences": ["source"],
                "citations": [],
            },
        )
        is None
    )
    assert (
        gold_claim_from_pair(
            "a",
            "b",
            {
                "relationship": "NONE",
                "scope": "OBSERVED_EVENT",
                "conditions": [],
                "settlement_compatibility": "UNKNOWN",
                "near_match_differences": [],
                "citations": [],
            },
        )
        is None
    )
    claim = gold_claim_from_pair(
        "a",
        "b",
        {
            "relationship": "IMPLIES",
            "scope": "OBSERVED_EVENT",
            "conditions": [],
            "settlement_compatibility": "CONDITIONAL",
            "near_match_differences": [],
            "citations": [],
        },
    )
    assert claim["relation"] == "IMPLIES"
    assert claim["a"] == "a" and claim["b"] == "b"
    outside = gold_claim_from_pair(
        "a",
        "b",
        {
            "relationship": "IMPLIES",
            "scope": "SETTLEMENT_OUTCOME",
            "conditions": [],
            "settlement_compatibility": "CONDITIONAL",
            "near_match_differences": [],
            "citations": [],
        },
        pair={"a": "a", "b": "b", "scope": "OBSERVED_EVENT", "conditions": []},
    )
    assert outside is None


def test_manifest_change_and_stale_digest_invalidate_consensus(store, tmp_path):
    demo = load_demo(store)
    interpretation = store.get(demo["interpretations"][0])
    ir = SemanticIR.model_validate(interpretation["data"]["ir"])
    from oddsfox.consensus import canonicalize_contract_from_ir

    expected = canonicalize_contract_from_ir(ir)
    paths = [tmp_path / m["identity"] for m in PRODUCERS]
    for path in paths:
        path.mkdir()

    def gen(prompt, schema):
        return json.dumps(expected)

    approval_id = approve_interpretation(
        store, interpretation["id"], paths, generator=gen, manifests=PRODUCERS
    )
    assert accepted(store, interpretation, allow_consensus=True)
    assert interpretation["data"]["assessment"] != "REVIEWED"
    assert store.current("review", interpretation["id"]) is None
    changed = [m | {"chat_template_sha256": "1" * 64} for m in PRODUCERS]
    persist_config(store, "producer-panel", panel_config("producer", changed))
    assert store.current("consensus_approval", interpretation["id"]) is None
    assert not store.get(approval_id)["current"]
    assert not accepted(store, store.get(interpretation["id"]), allow_consensus=True)
    with store.transaction():
        store.insert(
            "consensus_approval",
            interpretation["id"],
            {
                "protocol": "local-unanimous-consensus/1",
                "ir_digest": "stale-digest",
                "panel": "x",
                "ballots": [],
                "acceptance_basis": LOCAL_MODEL_CONSENSUS,
            },
            [interpretation["id"]],
            "ACCEPTED",
        )
    assert not accepted(store, store.get(interpretation["id"]), allow_consensus=True)

    other = store.get(demo["interpretations"][1])
    expected_b = canonicalize_contract_from_ir(SemanticIR.model_validate(other["data"]["ir"]))

    def gen_b(prompt, schema, expected=expected_b):
        return json.dumps(expected)

    approve_interpretation(store, other["id"], paths, generator=gen_b, manifests=changed)
    pipeline = Pipeline(store)
    pipeline.configure({"prompt": "second/2"})
    pipeline.interpret(json.dumps(other["data"]["ir"]))
    current = store.current("interpretation", other["data"]["ir"]["contract_version_id"])
    assert current["id"] != other["id"]
    assert store.current("consensus_approval", other["id"]) is None
    assert not accepted(store, current, allow_consensus=True)


def test_human_review_still_publishes_and_outranks_consensus(store, tmp_path):
    load_demo(store, approve=True)
    export = Pipeline(store, allow_consensus=False).export()
    assert export["assertions"]
    assert {row["data"]["acceptance_basis"] for row in export["assertions"]} == {HUMAN_REVIEW}

    from oddsfox.store import Store

    dataset_path = tmp_path / "mixed"

    dataset = Store(dataset_path)
    try:
        identities = load_demo(dataset)["interpretations"][:2]
        paths = [tmp_path / "mixed-models" / m["identity"] for m in PRODUCERS]
        for path in paths:
            path.mkdir(parents=True)
        for identity in identities:
            ir = SemanticIR.model_validate(dataset.get(identity)["data"]["ir"])
            from oddsfox.consensus import canonicalize_contract_from_ir

            expected = canonicalize_contract_from_ir(ir)

            def gen(prompt, schema, expected=expected):
                return json.dumps(expected)

            approve_interpretation(dataset, identity, paths, generator=gen, manifests=PRODUCERS)
            Pipeline(dataset, allow_consensus=True).review(
                identity,
                "operator",
                "human approval of the exact IR",
                True,
                True,
            )
        published = Pipeline(dataset, allow_consensus=True).publish()
        assert published
        export = Pipeline(dataset).export()
        assert {row["data"]["acceptance_basis"] for row in export["assertions"]} == {HUMAN_REVIEW}
    finally:
        dataset.close()
