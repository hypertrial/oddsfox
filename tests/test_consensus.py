import hashlib
import json
import re
from copy import deepcopy
from pathlib import Path

import pytest

from oddsfox.consensus import (
    HUMAN_REVIEW,
    LOCAL_MODEL_CONSENSUS,
    accepted,
    approve_interpretation,
)
from oddsfox.demo import load_demo
from oddsfox.evaluation import (
    comparison_identity,
    comparison_key,
    evaluate,
    invalidation_partition,
)
from oddsfox.ir import SemanticIR, fingerprint
from oddsfox.judge import (
    ContractBallot,
    collect_unanimous,
    disjoint_panels,
    gold_claim_from_pair,
    panel_config,
    parse_ballot,
    persist_config,
    run_ballot,
    validate_citations,
)
from oddsfox.pipeline import Pipeline
from oddsfox.validation import (
    freeze_corpus,
    scans_complete,
    stratified_sample,
    validate_dataset,
    write_bundle,
)


def manifest(name, family):
    return {
        "identity": name,
        "family": family,
        "architecture": f"architecture-{name}",
        "lineage": family,
        "lineage_schema": "oddsfox-model-lineage/1",
        "weights_revision": f"weights-{name}",
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
EVIDENCE = {"artifact": "captured evidence"}


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
        "citations": [{"artifact_id": "artifact", "start": 0, "end": 8}],
    }
    data.update(updates)
    return json.dumps(data)


def cite_expected(store, contract_id, expected):
    artifact_id, text = next(iter(store.source_texts(contract_id).items()))
    return expected | {
        "citations": [{"artifact_id": artifact_id, "start": 0, "end": min(8, len(text))}]
    }


def local_model(path, *, model_type, lineage, weights):
    path.mkdir()
    (path / "config.json").write_text(
        json.dumps({"model_type": model_type, "quantization": {"bits": 4}})
    )
    (path / "tokenizer_config.json").write_text(json.dumps({"chat_template": "{{ bos }}"}))
    (path / "weights.safetensors").write_bytes(weights)
    weights_revision = fingerprint([hashlib.sha256(weights).hexdigest()])
    path.with_name(f"{path.name}.oddsfox-lineage.json").write_text(
        json.dumps(
            {
                "schema": "oddsfox-model-lineage/1",
                "architecture": model_type,
                "lineage": lineage,
                "weights_revision": weights_revision,
                "operator_reviewed": True,
            }
        )
    )


def metric_stage_labels(contract_ids, pair_labels):
    contract_fields = {
        "ir_fields": {
            "quantity": None,
            "source": None,
            "instrument_or_series": None,
            "unit": None,
            "comparator": None,
            "threshold": None,
            "measurement_method": None,
            "timestamp": None,
            "timezone": None,
            "precision": None,
            "revision_policy": None,
        },
        "canonical_resolution": {
            "canonical_observation_id": None,
            "canonical_observation_version": None,
        },
        "settlement_fields": {
            "resolution_source": None,
            "cutoff": None,
            "rounding": None,
            "missing_data_policy": None,
            "cancellation_policy": None,
            "exceptional_outcome_policy": None,
            "dispute_policy": None,
            "clarification_policy": None,
        },
    }
    rows = [
        {
            "id": identity,
            "stage": stage,
            "mode": "pipeline",
            "expected": expected,
        }
        for identity in contract_ids
        for stage, expected in contract_fields.items()
    ]
    rows.extend(
        {
            "id": fingerprint({key: pair[key] for key in ("a", "b", "scope", "conditions")}),
            "operands": [pair["a"], pair["b"]],
            "stage": "settlement_compatibility",
            "mode": "pipeline",
            "expected": {"state": "COMPATIBLE", "conditions": pair["conditions"]},
        }
        for pair in pair_labels
    )
    return rows


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
    with pytest.raises(ValueError, match="weight sets"):
        disjoint_panels(
            PRODUCERS,
            [manifest("e0", "family-e0") | {"weights_revision": "weights-p0"}] + EVALUATORS[1:],
        )
    aliases = [
        manifest(f"alias-{index}", f"alias-lineage-{index}")
        | {"weights_revision": "same-weight-bytes"}
        for index in range(3)
    ]
    with pytest.raises(ValueError, match="weight sets"):
        panel_config("producer", aliases)
    with pytest.raises(ValueError, match="three"):
        panel_config("producer", PRODUCERS[:2])


def test_cloned_weight_directories_cannot_form_a_panel(tmp_path, monkeypatch):
    from oddsfox.models import model_manifest

    monkeypatch.setattr("oddsfox.models.importlib.metadata.version", lambda name: "0.test")
    manifests = []
    for index in range(3):
        path = tmp_path / f"clone-{index}"
        local_model(
            path,
            model_type=f"alias-{index}",
            lineage=f"operator-lineage-{index}",
            weights=b"identical weights",
        )
        manifests.append(model_manifest(path))
    assert len({row["weights_revision"] for row in manifests}) == 1
    with pytest.raises(ValueError, match="weight sets"):
        panel_config("producer", manifests)


def test_manifest_drift_during_generation_abstains(store, tmp_path, monkeypatch):
    from oddsfox.models import model_manifest

    monkeypatch.setattr("oddsfox.models.importlib.metadata.version", lambda name: "0.test")
    paths = []
    manifests = []
    for index in range(3):
        path = tmp_path / f"model-{index}"
        local_model(
            path,
            model_type=f"architecture-{index}",
            lineage=f"lineage-{index}",
            weights=f"weights-{index}".encode(),
        )
        paths.append(path)
        manifests.append(model_manifest(path))
    config_id = persist_config(store, "evaluator-panel", panel_config("evaluator", manifests))
    changed = False

    def mutate(prompt, schema):
        nonlocal changed
        if not changed:
            (paths[0] / "weights.safetensors").write_bytes(b"changed")
            changed = True
        return contract_ballot()

    result = collect_unanimous(
        store,
        kind="contract",
        subject="drift",
        logical="drift",
        config_id=config_id,
        paths=paths,
        prompt="p",
        schema=ContractBallot.model_json_schema(),
        generator=mutate,
        manifests=manifests,
        artifacts=EVIDENCE,
    )
    assert result["label"] is None
    assert "weights" in result["abstention"]


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
        artifacts=EVIDENCE,
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
        artifacts=EVIDENCE,
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
        artifacts=EVIDENCE,
    )
    assert result["abstention"] == "timeout"
    frozen = store.current("consensus_label_set", f"timeout:{config_id}")["id"]
    retried = collect_unanimous(
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
        artifacts=EVIDENCE,
    )
    node = store.get(frozen)
    assert store.current("consensus_label_set", f"timeout:{config_id}")["id"] == frozen
    assert retried["id"] == frozen
    assert retried["label"] is None
    assert retried["abstention"] == "timeout"
    assert retried["ballots"] == node["data"]["ballots"]
    assert node["data"]["label"] is None
    assert node["data"]["abstention"] == "timeout"


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
        artifacts=EVIDENCE,
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
        artifacts=EVIDENCE,
    )
    assert result["label"] is None
    assert result["abstention"]


def test_metrics_v4_rejects_human_flags_and_reports_agreement():
    comparisons = [
        {"a": a, "b": b, "scope": "OBSERVED_EVENT", "conditions": []}
        for a, b in [("a", "b"), ("b", "c"), ("a", "c")]
    ]
    pair_labels = [
        comparisons[0] | {"relationship": "IMPLIES"},
        comparisons[1] | {"relationship": "NONE"},
        comparisons[2] | {"relationship": "NEAR_MATCH"},
    ]
    stage_labels = metric_stage_labels("abc", pair_labels)
    bench = {
        "benchmark_id": "synthetic-test",
        "revision": "1",
        "label_version": "local-unanimous-consensus/1",
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
        "comparisons": comparisons,
        "pair_labels": pair_labels,
        "pair_outcomes": [row | {"state": "complete"} for row in comparisons],
        "gold_claims": [
            {
                "a": "a",
                "b": "b",
                "relation": "IMPLIES",
                "scope": "OBSERVED_EVENT",
                "conditions": [],
            }
        ],
        "stage_labels": stage_labels,
        "near_match_rejections": [comparisons[2] | {"differences": ["source"]}],
        "evaluator_panel": panel_config("evaluator", EVALUATORS),
        "diagnostics": {
            "ballots": 1,
            "latency_seconds": 1.0,
            "stage_latency_seconds": {"contract": 0.5, "pair": 0.5},
            "peak_memory_bytes": 1,
            "failures": 0,
            "retries": 0,
        },
    }
    run = {
        "benchmark_hash": fingerprint(bench),
        "evidence_mode": "synthetic-fixture",
        "scored_before_case_review": True,
        "pipeline_configuration": {
            "compiler": "test",
            "protocol": "local-unanimous-consensus/1",
            "producer_panel_manifest": panel_config("producer", PRODUCERS),
            "schema": "1.0.0",
        },
        "acceptance_policy": {
            "version": "consensus-agreement/1",
            "allowed_scopes": ["OBSERVED_EVENT"],
            "allowed_settlement_states": ["CONDITIONAL"],
            "require_resolved": False,
            "normalization": "exact-string-set/1",
        },
        "proposals": [],
        "stage_outputs": [
            {key: value for key, value in row.items() if key != "expected"}
            | {"values": row["expected"]}
            for row in stage_labels
        ],
        "outcomes": [
            {"id": identity, "stage": "interpret", "state": "complete"} for identity in "abc"
        ],
        "pair_outcomes": [row | {"state": "complete"} for row in comparisons],
        "diagnostics": {
            "ballots": 1,
            "latency_seconds": 1.0,
            "stage_latency_seconds": {"contract": 0.5, "pair": 0.5},
            "peak_memory_bytes": 1,
            "failures": 0,
            "retries": 0,
        },
        "complete_zero_error_scans": True,
        "unanimous_cross_venue_relationship": True,
        "unanimous_near_match_rejection": True,
        "zero_accepted_known_false_equivalences": True,
        "provenance_invalidation": True,
    }
    run["pair_outcomes"][0] = comparisons[0] | {"state": "complete"}
    report = evaluate(bench, run)
    assert report["metric_definition_version"] == "oddsfox-metrics/4"
    assert report["release_gates"]["independent_human_labels"] is False
    assert report["release_gates"]["unanimous_cross_venue_relationship"] is True
    assert report["release_gates"]["unanimous_near_match_rejection"] is True
    same_venue = dict(bench)
    same_venue["contracts"] = [row | {"venue": "kalshi"} for row in bench["contracts"]]
    same_report = evaluate(same_venue, run | {"benchmark_hash": fingerprint(same_venue)})
    assert same_report["release_gates"]["unanimous_cross_venue_relationship"] is False
    assert same_report["release_gates"]["unanimous_near_match_rejection"] is True
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


def test_v4_example_corpus_gates_ignore_run_flags():
    root = Path(__file__).resolve().parents[1]
    bench = json.loads((root / "examples/benchmark-v4.json").read_text())
    run = json.loads((root / "examples/run-v4.json").read_text())
    report = evaluate(bench, run)
    assert report["release_gates"]["unanimous_cross_venue_relationship"] is True
    assert report["release_gates"]["unanimous_near_match_rejection"] is True
    assert report["release_gates"]["provenance_invalidation"] is False
    lying = dict(run)
    lying["unanimous_cross_venue_relationship"] = False
    lying["unanimous_near_match_rejection"] = False
    lying["zero_accepted_known_false_equivalences"] = False
    lying["provenance_invalidation"] = True
    still = evaluate(bench, lying)
    assert still["release_gates"]["unanimous_cross_venue_relationship"] is True
    assert still["release_gates"]["unanimous_near_match_rejection"] is True
    assert still["release_gates"]["zero_accepted_known_false_equivalences"] is True
    assert still["release_gates"]["provenance_invalidation"] is False

    conflict = deepcopy(run)
    conflict["proposals"].append(
        {
            "a": "a",
            "b": "c",
            "relation": "IMPLIES",
            "scope": "OBSERVED_EVENT",
            "conditions": [],
        }
    )
    conflict_report = evaluate(bench, conflict)
    assert conflict_report["relationships"]["selected_precision"]["correct"] == 1
    assert conflict_report["relationships"]["selected_precision"]["total"] == 2

    contradictory = deepcopy(run)
    contradictory["pair_outcomes"][0]["state"] = "abstained"
    contradictory["pair_outcomes"][0]["reason"] = "dissent"
    with pytest.raises(ValueError, match="producer pair stages"):
        evaluate(bench, contradictory)


@pytest.mark.parametrize(
    "proposals",
    [
        [
            {
                "a": "a",
                "b": "b",
                "relation": "EQUIVALENT",
                "scope": "OBSERVED_EVENT",
                "conditions": [],
            }
        ],
        [
            {
                "a": "a",
                "b": "b",
                "relation": "IMPLIES",
                "scope": "OBSERVED_EVENT",
                "conditions": [],
            },
            {
                "a": "b",
                "b": "a",
                "relation": "IMPLIES",
                "scope": "OBSERVED_EVENT",
                "conditions": [],
            },
        ],
    ],
    ids=["direct", "closure-induced"],
)
def test_metrics_v4_derives_known_false_equivalences_from_selected_closure(proposals):
    root = Path(__file__).resolve().parents[1]
    bench = json.loads((root / "examples/benchmark-v4.json").read_text())
    run = json.loads((root / "examples/run-v4.json").read_text())
    run["proposals"] = proposals
    run["zero_accepted_known_false_equivalences"] = True

    report = evaluate(bench, run)

    expected = {
        "a": "a",
        "b": "b",
        "relation": "EQUIVALENT",
        "scope": "OBSERVED_EVENT",
        "conditions": [],
    }
    assert report["consensus"]["known_false_equivalences"] == {
        "count": 1,
        "claims": [expected],
    }
    assert report["release_gates"]["zero_accepted_known_false_equivalences"] is False


def test_metrics_v4_does_not_call_evaluator_abstention_known_false():
    root = Path(__file__).resolve().parents[1]
    bench = json.loads((root / "examples/benchmark-v4.json").read_text())
    run = json.loads((root / "examples/run-v4.json").read_text())
    pair = bench["comparisons"][0]
    pair_id = comparison_identity(pair)
    bench["pair_labels"] = bench["pair_labels"][1:]
    bench["pair_outcomes"][0] = pair | {"state": "abstained", "reason": "dissent"}
    bench["gold_claims"] = []
    bench["stage_labels"] = [row for row in bench["stage_labels"] if row.get("id") != pair_id]
    run["benchmark_hash"] = fingerprint(bench)
    run["proposals"] = [
        pair | {"relation": "EQUIVALENT"},
    ]

    report = evaluate(bench, run)

    assert report["consensus"]["known_false_equivalences"] == {"count": 0, "claims": []}
    assert report["release_gates"]["zero_accepted_known_false_equivalences"] is True


def test_metrics_v4_explicit_label_overrides_evaluator_closure_cycle():
    root = Path(__file__).resolve().parents[1]
    bench = json.loads((root / "examples/benchmark-v4.json").read_text())
    run = json.loads((root / "examples/run-v4.json").read_text())
    bench["pair_labels"] = [row | {"relationship": "IMPLIES"} for row in bench["pair_labels"]]
    bench["pair_labels"][2] = bench["pair_labels"][2] | {"a": "c", "b": "a"}
    bench["gold_claims"] = [
        {key: value for key, value in row.items() if key != "relationship"}
        | {"relation": "IMPLIES"}
        for row in bench["pair_labels"]
    ]
    bench["near_match_rejections"] = []
    old_pair_id = comparison_identity(
        {"a": "a", "b": "c", "scope": "OBSERVED_EVENT", "conditions": []}
    )
    new_pair_id = comparison_identity(bench["pair_labels"][2])
    bench["stage_labels"] = [
        row | {"id": new_pair_id} if row.get("id") == old_pair_id else row
        for row in bench["stage_labels"]
    ]
    run["benchmark_hash"] = fingerprint(bench)
    run["proposals"] = [
        {
            "a": "a",
            "b": "b",
            "relation": "EQUIVALENT",
            "scope": "OBSERVED_EVENT",
            "conditions": [],
        }
    ]

    report = evaluate(bench, run)

    assert report["relationships"]["selected_precision"]["value"] == 1
    assert report["consensus"]["known_false_equivalences"]["count"] == 1
    assert report["release_gates"]["zero_accepted_known_false_equivalences"] is False


def test_synthetic_metrics_v4_cannot_self_attest_invalidation():
    root = Path(__file__).resolve().parents[1]
    bench = json.loads((root / "examples/benchmark-v4.json").read_text())
    run = json.loads((root / "examples/run-v4.json").read_text())
    receipt = {
        "protocol": "dependency-invalidation/1",
        "root_contract_id": "a",
        "expected_affected_ids": ["ballot"],
        "observed_stale_ids": ["ballot"],
        "unaffected_current_ids": [],
        "rollback_restored_ids": ["a", "ballot"],
    }
    run["invalidation_evidence"] = receipt
    run["invalidation_evidence_hash"] = fingerprint(receipt)

    with pytest.raises(ValueError, match="synthetic metrics v4 cannot self-attest"):
        evaluate(bench, run)


@pytest.mark.parametrize(
    "mutate,match",
    [
        (lambda bench, run: bench.pop("evaluator_panel"), "panel provenance"),
        (
            lambda bench, run: bench.__setitem__("label_version", "wrong/1"),
            "label protocol",
        ),
        (
            lambda bench, run: bench.__setitem__("independent_human_labels", "false"),
            "independent_human_labels",
        ),
        (
            lambda bench, run: run["acceptance_policy"].__setitem__("version", "test/1"),
            "consensus-agreement/1",
        ),
        (
            lambda bench, run: run["acceptance_policy"].__setitem__("require_resolved", True),
            "solver resolution",
        ),
        (
            lambda bench, run: run["pipeline_configuration"]["producer_panel_manifest"].pop(
                "sampling"
            ),
            "panel role or unanimity",
        ),
        (
            lambda bench, run: run["pipeline_configuration"]["producer_panel_manifest"]["models"][
                0
            ].__setitem__("lineage", "eval-0"),
            "disjoint lineage",
        ),
        (lambda bench, run: bench["stage_labels"].pop(), "mandatory stage"),
        (
            lambda bench, run: bench["near_match_rejections"][0].__setitem__("a", "b"),
            "near-match rejections",
        ),
        (
            lambda bench, run: next(
                row for row in bench["stage_labels"] if row["stage"] == "settlement_compatibility"
            ).__setitem__("operands", ["a", "missing"]),
            "mandatory stage",
        ),
        (
            lambda bench, run: bench["stage_labels"][0]["expected"].__setitem__("invented", "easy"),
            "mandatory stage",
        ),
        (
            lambda bench, run: next(
                row for row in run["stage_outputs"] if row["stage"] == "settlement_compatibility"
            ).__setitem__("operands", ["a", "a"]),
            "producer pair stages",
        ),
        (lambda bench, run: run.pop("diagnostics"), "producer diagnostics"),
        (
            lambda bench, run: run["diagnostics"].__setitem__("failures", 0.5),
            "nonnegative",
        ),
    ],
)
def test_metrics_v4_rejects_incomplete_consensus_provenance(mutate, match):
    root = Path(__file__).resolve().parents[1]
    bench = json.loads((root / "examples/benchmark-v4.json").read_text())
    run = json.loads((root / "examples/run-v4.json").read_text())
    mutate(bench, run)
    run["benchmark_hash"] = fingerprint(bench)
    with pytest.raises(ValueError, match=match):
        evaluate(bench, run)


def test_metrics_v4_rejects_duplicate_producer_stage_evidence():
    root = Path(__file__).resolve().parents[1]
    bench = json.loads((root / "examples/benchmark-v4.json").read_text())
    run = json.loads((root / "examples/run-v4.json").read_text())
    run["stage_outputs"].append(deepcopy(run["stage_outputs"][0]))
    with pytest.raises(ValueError, match="duplicate stage output"):
        evaluate(bench, run)


def test_metrics_v4_rejects_extra_evaluator_pair_stage_field():
    root = Path(__file__).resolve().parents[1]
    bench = json.loads((root / "examples/benchmark-v4.json").read_text())
    run = json.loads((root / "examples/run-v4.json").read_text())
    pair_stage = next(
        row for row in bench["stage_labels"] if row["stage"] == "settlement_compatibility"
    )
    pair_stage["expected"]["invented"] = "value"
    run["benchmark_hash"] = fingerprint(bench)
    with pytest.raises(ValueError, match="complete labels for every mandatory stage"):
        evaluate(bench, run)


@pytest.mark.parametrize("owner", ["evaluator", "producer"])
def test_metrics_v4_rejects_unexplained_contract_abstentions(owner):
    root = Path(__file__).resolve().parents[1]
    bench = json.loads((root / "examples/benchmark-v4.json").read_text())
    run = json.loads((root / "examples/run-v4.json").read_text())
    contract_id = bench["contracts"][0]["id"]
    if owner == "evaluator":
        bench["label_abstentions"].append({"id": contract_id, "reason": ""})
        bench["stage_labels"] = [
            row for row in bench["stage_labels"] if row.get("id") != contract_id
        ]
        run["benchmark_hash"] = fingerprint(bench)
    else:
        run["outcomes"] = [
            row | {"state": "abstained", "reason": ""}
            if row.get("id") == contract_id and row.get("stage") == "interpret"
            else row
            for row in run["outcomes"]
        ]
        run["stage_outputs"] = [row for row in run["stage_outputs"] if row.get("id") != contract_id]
    with pytest.raises(ValueError, match="contract abstention requires a reason"):
        evaluate(bench, run)


@pytest.mark.parametrize("owner", ["evaluator", "producer"])
def test_metrics_v4_rejects_unexplained_pair_abstentions(owner):
    root = Path(__file__).resolve().parents[1]
    bench = json.loads((root / "examples/benchmark-v4.json").read_text())
    run = json.loads((root / "examples/run-v4.json").read_text())
    pair = bench["comparisons"][1]
    pair_id = fingerprint(pair)
    if owner == "evaluator":
        bench["pair_labels"] = [
            row for row in bench["pair_labels"] if comparison_key(row) != comparison_key(pair)
        ]
        bench["stage_labels"] = [row for row in bench["stage_labels"] if row.get("id") != pair_id]
        bench["pair_outcomes"][1] = pair | {"state": "abstained"}
        run["benchmark_hash"] = fingerprint(bench)
    else:
        run["pair_outcomes"][1] = pair | {"state": "abstained"}
        run["stage_outputs"] = [row for row in run["stage_outputs"] if row.get("id") != pair_id]
    with pytest.raises(ValueError, match="abstention.*reason"):
        evaluate(bench, run)


def test_metrics_v4_rejects_self_attested_immutable_evidence():
    root = Path(__file__).resolve().parents[1]
    bench = json.loads((root / "examples/benchmark-v4.json").read_text())
    run = json.loads((root / "examples/run-v4.json").read_text())
    bench["split"] = "held-out-consensus"
    bench["evaluator_label_set_ids"] = ["evaluator-label"]
    run.update(
        evidence_mode="immutable-local-store",
        producer_label_set_ids=["producer-label"],
        judge_evidence_hash="0" * 64,
        provenance_invalidation=True,
    )
    run["benchmark_hash"] = fingerprint(bench)
    with pytest.raises(ValueError, match="immutable judge evidence"):
        evaluate(bench, run)


def test_metrics_v4_rejects_fabricated_but_internally_linked_evidence():
    root = Path(__file__).resolve().parents[1]
    bench = json.loads((root / "examples/benchmark-v4.json").read_text())
    run = json.loads((root / "examples/run-v4.json").read_text())
    bench["split"] = "held-out-consensus"
    subjects = [("contract", row["id"], {row["id"]}) for row in bench["contracts"]]
    subjects += [
        ("pair", comparison_identity(row), {row["a"], row["b"]}) for row in bench["comparisons"]
    ]
    configs = {"evaluator": "fake-evaluator-config", "producer": "fake-producer-config"}
    panels = {
        "evaluator": bench["evaluator_panel"],
        "producer": run["pipeline_configuration"]["producer_panel_manifest"],
    }
    evidence = {
        "configs": {},
        "label_sets": [],
        "ballots": [],
        "raw_responses": {},
        "cited_sources": {},
        "dependencies": [],
    }
    label_ids = {}
    for owner, config_id in configs.items():
        panel = panels[owner]
        evidence["configs"][config_id] = {
            "id": f"not-content-addressed-{config_id}",
            "current": True,
            "data": panel,
        }
        label_ids[owner] = []
        for subject_index, (kind, subject, parents) in enumerate(subjects):
            label_id = f"fake-{owner}-label-{subject_index}"
            ballot_ids = [
                f"fake-{owner}-ballot-{subject_index}-{model_index}"
                for model_index in range(len(panel["models"]))
            ]
            label_ids[owner].append(label_id)
            evidence["label_sets"].append(
                {
                    "id": label_id,
                    "current": True,
                    "data": {
                        "kind": kind,
                        "subject": subject,
                        "config_id": config_id,
                        "ballots": ballot_ids,
                        "label": {},
                    },
                }
            )
            evidence["dependencies"].append({"child": label_id, "parent": config_id})
            for ballot_id, model in zip(ballot_ids, panel["models"], strict=True):
                evidence["ballots"].append(
                    {
                        "id": ballot_id,
                        "current": True,
                        "data": {
                            "kind": kind,
                            "subject": subject,
                            "config_id": config_id,
                            "model": model,
                            "payload": {},
                        },
                    }
                )
                evidence["dependencies"].extend(
                    [
                        {"child": label_id, "parent": ballot_id},
                        {"child": ballot_id, "parent": config_id},
                        *({"child": ballot_id, "parent": parent} for parent in parents),
                    ]
                )
    bench["evaluator_panel_id"] = configs["evaluator"]
    bench["evaluator_label_set_ids"] = label_ids["evaluator"]
    run["pipeline_configuration"]["producer_panel"] = configs["producer"]
    run["producer_label_set_ids"] = label_ids["producer"]
    run["evidence_mode"] = "immutable-local-store"
    run["judge_evidence"] = evidence
    run["judge_evidence_hash"] = fingerprint(evidence)
    run["provenance_invalidation"] = True
    run["benchmark_hash"] = fingerprint(bench)

    with pytest.raises(ValueError, match="evidence"):
        evaluate(bench, run)


def test_metrics_v4_uses_unscored_producer_pair_for_scored_transitive_closure():
    root = Path(__file__).resolve().parents[1]
    bench = json.loads((root / "examples/benchmark-v4.json").read_text())
    run = json.loads((root / "examples/run-v4.json").read_text())
    ab, bc, ac = bench["comparisons"]
    bench["pair_labels"] = [
        ab | {"relationship": "IMPLIES"},
        ac | {"relationship": "IMPLIES"},
    ]
    bench["pair_outcomes"] = [
        ab | {"state": "complete"},
        bc | {"state": "abstained", "reason": "dissent"},
        ac | {"state": "complete"},
    ]
    bench["gold_claims"] = [ab | {"relation": "IMPLIES"}, ac | {"relation": "IMPLIES"}]
    bench["stage_labels"] = [
        row
        for row in bench["stage_labels"]
        if row.get("stage") != "settlement_compatibility" or row.get("id") != fingerprint(bc)
    ]
    bench["near_match_rejections"] = []
    run["benchmark_hash"] = fingerprint(bench)
    run["proposals"] = [ab | {"relation": "IMPLIES"}, bc | {"relation": "IMPLIES"}]

    report = evaluate(bench, run)

    assert report["relationships"]["selected_precision"]["value"] == 1
    assert report["relationships"]["selected_recall"]["value"] == 1
    assert report["relationships"]["raw_unique_selected"] == 1


def test_metrics_v4_release_boundaries():
    def inputs(sampled=100, abstentions=20, wrong=0):
        contracts = [
            {
                "id": f"c{index}",
                "venue": "kalshi" if index % 2 else "polymarket",
                "template": "test",
                "eligible": True,
            }
            for index in range(sampled)
        ]
        pairs = []
        for left in range(sampled):
            for right in range(left + 1, sampled):
                pairs.append(
                    {
                        "a": f"c{left}",
                        "b": f"c{right}",
                        "scope": "OBSERVED_EVENT",
                        "conditions": [],
                    }
                )
                if len(pairs) == 100:
                    break
            if len(pairs) == 100:
                break
        labels = [row | {"relationship": "EXCLUDES"} for row in pairs]
        gold = [row | {"relation": "EXCLUDES"} for row in pairs]
        proposals = [
            row | {"relation": "COMPLEMENT" if index < wrong else "EXCLUDES"}
            for index, row in enumerate(pairs)
        ]
        active_ids = [f"c{index}" for index in range(abstentions, sampled)]
        stages = metric_stage_labels(active_ids, labels)
        bench = {
            "benchmark_id": "boundary",
            "revision": "1",
            "label_version": "local-unanimous-consensus/1",
            "labeling_guide": "exact-string-set/1",
            "split": "development",
            "independent_human_labels": False,
            "label_source": "local_unanimous_consensus",
            "metric_definition_version": "oddsfox-metrics/4",
            "evaluator_panel": panel_config("evaluator", EVALUATORS),
            "diagnostics": {
                "ballots": 1,
                "latency_seconds": 1.0,
                "stage_latency_seconds": {"contract": 0.5, "pair": 0.5},
                "peak_memory_bytes": 1,
                "failures": 0,
                "retries": 0,
            },
            "contracts": contracts,
            "comparisons": pairs,
            "pair_labels": labels,
            "pair_outcomes": [row | {"state": "complete"} for row in pairs],
            "gold_claims": gold,
            "stage_labels": stages,
            "label_abstentions": [
                {"id": f"c{index}", "reason": "dissent"} for index in range(abstentions)
            ],
            "near_match_rejections": [],
        }
        run = {
            "benchmark_hash": fingerprint(bench),
            "evidence_mode": "synthetic-fixture",
            "scored_before_case_review": True,
            "pipeline_configuration": {
                "compiler": "test",
                "protocol": "local-unanimous-consensus/1",
                "producer_panel_manifest": panel_config("producer", PRODUCERS),
                "schema": "1.0.0",
            },
            "acceptance_policy": {
                "version": "consensus-agreement/1",
                "allowed_scopes": ["OBSERVED_EVENT"],
                "allowed_settlement_states": ["CONDITIONAL", "COMPATIBLE"],
                "require_resolved": False,
                "normalization": "exact-string-set/1",
            },
            "proposals": proposals,
            "stage_outputs": [
                {key: value for key, value in row.items() if key != "expected"}
                | {"values": row["expected"]}
                for row in stages
            ],
            "outcomes": [
                {"id": identity, "stage": "interpret", "state": "complete"}
                for identity in active_ids
            ],
            "pair_outcomes": [row | {"state": "complete"} for row in pairs],
            "diagnostics": {
                "ballots": 1,
                "latency_seconds": 1.0,
                "stage_latency_seconds": {"contract": 0.5, "pair": 0.5},
                "peak_memory_bytes": 1,
                "failures": 0,
                "retries": 0,
            },
            "complete_zero_error_scans": True,
            "zero_accepted_known_false_equivalences": True,
            "provenance_invalidation": True,
        }
        return evaluate(bench, run)

    passing = inputs()
    assert passing["release_gates"]["sample_size"] is True
    assert passing["release_gates"]["evaluator_label_coverage"] is True
    assert passing["release_gates"]["selected_agreement_target"] is True
    assert passing["release_gates"]["selected_agreement_wilson_lower"] is True
    assert inputs(sampled=99)["release_gates"]["sample_size"] is False
    assert inputs(abstentions=21)["release_gates"]["evaluator_label_coverage"] is False
    at_target = inputs(wrong=1)["release_gates"]
    assert at_target["selected_agreement_target"] is True
    assert at_target["selected_agreement_wilson_lower"] is False
    assert inputs(wrong=2)["release_gates"]["selected_agreement_target"] is False


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
    assert sum(row["inventory"] for row in meta["strata"].values()) == 360
    assert sum(row["selected"] for row in meta["strata"].values()) == 250


def test_stratified_sample_preserves_original_stratum_counts():
    rows = [
        {
            "id": f"a-{index}",
            "venue": "kalshi",
            "template": "same",
            "eligible": True,
            "event_id": f"event-{index}",
            "title": f"Alpha {index}",
            "lexical": "alpha",
        }
        for index in range(3)
    ]
    selected, meta = stratified_sample(rows)
    assert len(selected) == 3
    assert meta["strata"] == {"kalshi:same:a": {"inventory": 3, "selected": 3}}


def test_write_once_bundle(tmp_path):
    directory = tmp_path / "bundle"
    manifest = write_bundle(directory, {"corpus.json": {"ok": True}})
    assert set(manifest["files"]) == {"corpus.json"}
    assert (directory / "manifest.json").is_file()
    with pytest.raises(FileExistsError):
        write_bundle(directory, {"corpus.json": {"ok": True}})


def test_bundle_failure_is_not_published_and_can_retry(tmp_path):
    directory = tmp_path / "bundle"
    with pytest.raises(ValueError):
        write_bundle(
            directory,
            {"first.json": {"ok": True}, "broken.json": {"invalid": float("nan")}},
        )
    assert not directory.exists()
    assert list(tmp_path.glob(".bundle.stage-*")) == []
    write_bundle(directory, {"first.json": {"ok": True}})
    assert (directory / "manifest.json").is_file()


def test_bundle_destination_race_never_replaces_existing_directory(tmp_path, monkeypatch):
    import oddsfox.validation as validation

    directory = tmp_path / "bundle"
    original_mkdtemp = validation.tempfile.mkdtemp

    def race(*args, **kwargs):
        staging = original_mkdtemp(*args, **kwargs)
        directory.mkdir()
        return staging

    monkeypatch.setattr(validation.tempfile, "mkdtemp", race)
    with pytest.raises(FileExistsError):
        write_bundle(directory, {"corpus.json": {"ok": True}})
    assert directory.is_dir() and list(directory.iterdir()) == []
    assert list(tmp_path.glob(".bundle.stage-*")) == []


def test_invalid_ballot_citations_abstain_without_poisoning_integrity(store, tmp_path):
    config_id = persist_config(store, "evaluator-panel", panel_config("evaluator", EVALUATORS))
    paths = [tmp_path / model["identity"] for model in EVALUATORS]
    for path in paths:
        path.mkdir()
    artifact_id = store.put_artifact(b"captured evidence")
    artifacts = {artifact_id: "captured evidence"}
    invalid = [
        [],
        [{"artifact_id": "f" * 64, "start": 0, "end": 1}],
        [{"artifact_id": artifact_id, "start": 2, "end": 2}],
        [{"artifact_id": artifact_id, "start": 0, "end": 999}],
    ]
    for index, citations in enumerate(invalid):
        result = collect_unanimous(
            store,
            kind="contract",
            subject=f"citation-{index}",
            logical=f"citation-{index}",
            config_id=config_id,
            paths=paths,
            prompt="p",
            schema=ContractBallot.model_json_schema(),
            generator=lambda prompt, schema, citations=citations: contract_ballot(
                citations=citations
            ),
            manifests=EVALUATORS,
            artifacts=artifacts,
        )
        assert result["label"] is None
        assert all(store.get(identity)["data"]["payload"] == {} for identity in result["ballots"])
    store.verify_integrity()


def test_positive_ballots_and_both_pair_operands_require_citations():
    artifacts = {"left": "left source", "right": "right source"}
    unsupported = json.loads(contract_ballot())
    unsupported.update(eligible=False, family="unsupported", citations=[])
    for field in set(unsupported) - {"eligible", "family", "unknowns", "citations"}:
        unsupported[field] = None
    validate_citations("contract", unsupported, artifacts)
    with pytest.raises(ValueError, match="eligible or semantic"):
        validate_citations(
            "contract", json.loads(contract_ballot(eligible=True, citations=[])), artifacts
        )
    pair = {
        "relationship": "NEAR_MATCH",
        "citations": [{"artifact_id": "left", "start": 0, "end": 4}],
    }
    with pytest.raises(ValueError, match="both operands"):
        validate_citations("pair", pair, artifacts, [{"left"}, {"right"}])
    pair["citations"].append({"artifact_id": "right", "start": 0, "end": 5})
    validate_citations("pair", pair, artifacts, [{"left"}, {"right"}])
    validate_citations(
        "pair",
        {
            "relationship": "NONE",
            "settlement_compatibility": "UNKNOWN",
            "conditions": [],
            "near_match_differences": [],
            "citations": [],
        },
        artifacts,
    )


def test_validate_dataset_success_and_atomic_bundle(store, tmp_path, monkeypatch):
    from oddsfox.catalog import set_sync_state
    from oddsfox.store import now
    from oddsfox.sync import SyncRunner

    SyncRunner(store).close()
    contract_ids = []
    for venue in ("kalshi", "polymarket"):
        contract_id = store.capture(
            venue,
            f"{venue}-contract",
            b"{}",
            {"rules": f"{venue} captured governing evidence"},
            {
                "capture_status": "complete",
                "outcome_ids": ["YES", "NO"],
                "category": "test",
            },
        )
        contract_ids.append(contract_id)
        event_id = f"{venue}:event"
        event_data = {
            "contracts": [contract_id],
            "volume": {"amount": "200000"},
        }
        with store.transaction():
            store.db.execute(
                "INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    event_id,
                    venue,
                    "event",
                    "Shared outcome",
                    "test",
                    "00000000000000000000200000.",
                    "qualified",
                    True,
                    "run",
                    contract_id,
                    json.dumps(event_data),
                    now(),
                ],
            )
            store.db.execute("INSERT INTO event_terms VALUES (?,?)", ["shared", event_id])
        set_sync_state(
            store,
            venue,
            state="complete",
            last_success="2026-01-01T00:00:00Z",
            error_count=0,
            errors=[],
        )

    producer_paths = [tmp_path / model["identity"] for model in PRODUCERS]
    evaluator_paths = [tmp_path / model["identity"] for model in EVALUATORS]
    for path in producer_paths + evaluator_paths:
        path.mkdir()

    def generator(prompt, schema):
        artifact_ids = list(dict.fromkeys(re.findall(r"'([a-f0-9]{64})':", prompt)))
        citation = [
            {"artifact_id": artifact_id, "start": 0, "end": 8} for artifact_id in artifact_ids
        ]
        if schema["title"] == "ContractBallot":
            return contract_ballot(citations=citation)
        return json.dumps(
            {
                "relationship": "EQUIVALENT",
                "scope": "OBSERVED_EVENT",
                "conditions": [],
                "settlement_compatibility": "COMPATIBLE",
                "near_match_differences": [],
                "citations": citation,
            }
        )

    output = tmp_path / "validation-bundle"
    report = validate_dataset(
        store,
        output,
        producer_paths,
        evaluator_paths,
        generator=generator,
        producer_manifests=PRODUCERS,
        evaluator_manifests=EVALUATORS,
    )
    assert report["relationships"]["selected_precision"]["value"] == 1
    assert report["release_gates"]["zero_accepted_known_false_equivalences"] is True
    assert report["release_gates"]["provenance_invalidation"] is True
    assert all(
        report["stages"][f"{stage}/pipeline"]["fields"]["total"] > 0
        for stage in (
            "ir_fields",
            "canonical_resolution",
            "settlement_fields",
            "settlement_compatibility",
        )
    )
    manifest = json.loads((output / "manifest.json").read_text())
    evidence = json.loads((output / "judge-evidence.json").read_text())
    invalidation = json.loads((output / "invalidation-evidence.json").read_text())
    bundled_run = json.loads((output / "run.json").read_text())
    bundled_corpus = json.loads((output / "corpus.json").read_text())
    assert bundled_run["judge_evidence_hash"] == fingerprint(evidence)
    assert bundled_run["invalidation_evidence"] == invalidation
    assert bundled_run["invalidation_evidence_hash"] == fingerprint(invalidation)
    assert bundled_run["evidence_mode"] == "immutable-local-store"
    assert evidence["label_sets"] and evidence["ballots"] and evidence["raw_responses"]
    assert evidence["cited_sources"]
    assert all(row["sha256"] == identity for identity, row in evidence["raw_responses"].items())
    exact_ids = {row["id"] for key in ("label_sets", "ballots") for row in evidence[key]}
    children = {}
    for dependency in evidence["dependencies"]:
        if dependency["child"] in exact_ids:
            children.setdefault(dependency["parent"], set()).add(dependency["child"])
    affected = set()
    pending = [invalidation["root_contract_id"]]
    while pending:
        for child in children.get(pending.pop(), set()) - affected:
            affected.add(child)
            pending.append(child)
    unaffected = exact_ids - affected
    assert invalidation["root_contract_id"] == sorted(contract_ids)[0]
    assert affected and unaffected
    assert invalidation == {
        "protocol": "dependency-invalidation/1",
        "root_contract_id": invalidation["root_contract_id"],
        "expected_affected_ids": sorted(affected),
        "observed_stale_ids": sorted(affected),
        "unaffected_current_ids": sorted(unaffected),
        "rollback_restored_ids": sorted({invalidation["root_contract_id"], *exact_ids}),
    }
    assert all(
        store.get(identity)["current"] is True
        for identity in {invalidation["root_contract_id"], *exact_ids}
    )
    assert "invalidation-evidence.json" in manifest["files"]

    missing_probe = deepcopy(bundled_run)
    missing_probe.pop("invalidation_evidence")
    missing_probe.pop("invalidation_evidence_hash")
    with pytest.raises(ValueError, match="requires invalidation evidence"):
        evaluate(bundled_corpus, missing_probe, _trusted_evidence=True)

    tampered_probe = deepcopy(bundled_run)
    tampered_probe["invalidation_evidence"]["observed_stale_ids"] = []
    with pytest.raises(ValueError, match="invalidation evidence hash is invalid"):
        evaluate(bundled_corpus, tampered_probe, _trusted_evidence=True)

    mismatched_probe = deepcopy(bundled_run)
    mismatched_probe["invalidation_evidence"]["observed_stale_ids"] = []
    mismatched_probe["invalidation_evidence_hash"] = fingerprint(
        mismatched_probe["invalidation_evidence"]
    )
    with pytest.raises(ValueError, match="does not match judge dependencies"):
        evaluate(bundled_corpus, mismatched_probe, _trusted_evidence=True)

    substituted_root = next(
        identity
        for identity in sorted(contract_ids)[1:]
        if invalidation_partition(evidence, identity)[1]
    )
    exact, affected, unaffected_for_root = invalidation_partition(evidence, substituted_root)
    substituted_probe = deepcopy(bundled_run)
    substituted_probe["invalidation_evidence"] = {
        "protocol": "dependency-invalidation/1",
        "root_contract_id": substituted_root,
        "expected_affected_ids": sorted(affected),
        "observed_stale_ids": sorted(affected),
        "unaffected_current_ids": sorted(unaffected_for_root),
        "rollback_restored_ids": sorted({substituted_root, *exact}),
    }
    substituted_probe["invalidation_evidence_hash"] = fingerprint(
        substituted_probe["invalidation_evidence"]
    )
    with pytest.raises(ValueError, match="invalidation root is not deterministic"):
        evaluate(bundled_corpus, substituted_probe, _trusted_evidence=True)

    relabeled_corpus = deepcopy(bundled_corpus)
    relabeled_run = deepcopy(bundled_run)
    relabeled_corpus["pair_labels"][0]["relationship"] = "EXCLUDES"
    relabeled_corpus["gold_claims"][0]["relation"] = "EXCLUDES"
    relabeled_run["proposals"][0]["relation"] = "EXCLUDES"
    relabeled_run["benchmark_hash"] = fingerprint(relabeled_corpus)
    with pytest.raises(ValueError, match="evaluator evidence does not match"):
        evaluate(relabeled_corpus, relabeled_run, _trusted_evidence=True)
    for name, digest in manifest["files"].items():
        assert hashlib.sha256((output / name).read_bytes()).hexdigest() == digest
    with pytest.raises(FileExistsError):
        validate_dataset(
            store,
            output,
            producer_paths,
            evaluator_paths,
            generator=generator,
            producer_manifests=PRODUCERS,
            evaluator_manifests=EVALUATORS,
        )

    original_invalidate = store.invalidate

    def force_probe_mismatch(identity, reason):
        original_invalidate(identity, reason)
        original_invalidate(next(iter(unaffected)), "forced probe mismatch")

    monkeypatch.setattr(store, "invalidate", force_probe_mismatch)
    failed_output = tmp_path / "failed-validation-bundle"
    with pytest.raises(ValueError, match="probe did not match judge dependencies"):
        validate_dataset(
            store,
            failed_output,
            producer_paths,
            evaluator_paths,
            generator=generator,
            producer_manifests=PRODUCERS,
            evaluator_manifests=EVALUATORS,
        )
    assert not failed_output.exists()
    assert all(
        store.get(identity)["current"] is True
        for identity in {invalidation["root_contract_id"], *exact_ids}
    )
    monkeypatch.setattr(store, "invalidate", original_invalidate)

    pair_labels = [
        row for row in store.list("consensus_label_set", True) if row["data"]["kind"] == "pair"
    ]
    assert pair_labels
    ballot_ids = [identity for row in pair_labels for identity in row["data"]["ballots"]]
    store.invalidate(contract_ids[0], "governing source revision")
    assert all(not store.get(row["id"])["current"] for row in pair_labels)
    assert all(not store.get(identity)["current"] for identity in ballot_ids)


def test_human_veto_blocks_consensus_publication(store, tmp_path):
    demo = load_demo(store)
    interpretation = store.get(demo["interpretations"][0])
    ir = SemanticIR.model_validate(interpretation["data"]["ir"])
    from oddsfox.consensus import canonicalize_contract_from_ir

    expected = cite_expected(store, ir.contract_version_id, canonicalize_contract_from_ir(ir))
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

        expected = cite_expected(store, ir.contract_version_id, canonicalize_contract_from_ir(ir))

        def gen(prompt, schema, expected=expected):
            return json.dumps(expected)

        approve_interpretation(store, identity, paths, generator=gen, manifests=PRODUCERS)
    published = Pipeline(store, allow_consensus=True).publish()
    assert published
    export = Pipeline(store).export()
    assert export["assertions"][0]["data"]["acceptance_basis"] == LOCAL_MODEL_CONSENSUS
    assert "REVIEWED" not in export["assertions"][0]["data"]["interpretation_assessments"]
    assert export["consensus_approvals"]
    Pipeline(store, allow_consensus=True).review(
        demo["interpretations"][0], "operator", "post-publication veto", False, False
    )
    assert Pipeline(store).export()["assertions"] == []
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
        artifacts=EVIDENCE,
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
        artifacts=EVIDENCE,
    )
    frozen = store.get(first["id"])
    assert len(calls) == 3
    assert second["id"] == first["id"] == frozen["id"]
    assert second["label"] == first["label"] == frozen["data"]["label"]
    assert second["ballots"] == first["ballots"] == frozen["data"]["ballots"]
    assert second["abstention"] == frozen["data"]["abstention"]


def test_interrupted_judge_store_recovers_and_resumes_deterministically(tmp_path):
    from oddsfox.store import Store

    dataset = tmp_path / "interrupted-judge"
    paths = [tmp_path / model["identity"] for model in EVALUATORS]
    for path in paths:
        path.mkdir()
    first = Store(dataset)
    config_id = persist_config(first, "evaluator-panel", panel_config("evaluator", EVALUATORS))
    artifact_id = first.put_artifact(b"captured evidence")
    evidence = {artifact_id: "captured evidence"}

    def interrupt(_prompt, _schema):
        raise KeyboardInterrupt

    try:
        with pytest.raises(KeyboardInterrupt):
            collect_unanimous(
                first,
                kind="contract",
                subject="interrupted",
                logical="interrupted",
                config_id=config_id,
                paths=paths,
                prompt="p",
                schema=ContractBallot.model_json_schema(),
                generator=interrupt,
                manifests=EVALUATORS,
                artifacts=evidence,
            )
        missing = run_ballot(
            first,
            kind="contract",
            subject="interrupted",
            model_path=paths[0],
            config_id=config_id,
            prompt="p",
            schema=ContractBallot.model_json_schema(),
            generator=lambda _prompt, _schema: contract_ballot(),
            manifest=EVALUATORS[0],
            artifacts=evidence,
        )
        assert missing[1:] == (None, "missing ballot", {})
    finally:
        first.close()

    reopened = Store(dataset)
    try:
        resumed = collect_unanimous(
            reopened,
            kind="contract",
            subject="interrupted",
            logical="interrupted",
            config_id=config_id,
            paths=paths,
            prompt="p",
            schema=ContractBallot.model_json_schema(),
            generator=lambda _prompt, _schema: contract_ballot(
                citations=[{"artifact_id": artifact_id, "start": 0, "end": 8}]
            ),
            manifests=EVALUATORS,
            artifacts=evidence,
        )
        assert resumed["label"]["threshold"] == "150000"
        attempts = reopened.status()["attempts"]
        assert {row["state"] for row in attempts} == {"interrupted", "done"}
        assert any(row["number"] == 2 and row["state"] == "done" for row in attempts)
        for ballot_id in resumed["ballots"]:
            ballot = reopened.get(ballot_id)
            dependencies = reopened._rows(
                "SELECT parent FROM dependencies WHERE child=?", [ballot_id]
            )
            assert config_id in {row["parent"] for row in dependencies}
            raw_id = ballot["data"]["raw_response_artifact"]
            assert reopened.artifact(raw_id)
        reopened.verify_integrity()
    finally:
        reopened.close()


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
    comparisons = [
        {"a": a, "b": b, "scope": "OBSERVED_EVENT", "conditions": []}
        for a, b in [("a", "b"), ("b", "c"), ("a", "c")]
    ]
    bench = {
        "benchmark_id": "synthetic-test",
        "revision": "1",
        "label_version": "local-unanimous-consensus/1",
        "labeling_guide": "exact-string-set/1",
        "split": "development",
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
        "comparisons": comparisons,
        "pair_labels": [],
        "pair_outcomes": [row | {"state": "abstained", "reason": "dissent"} for row in comparisons],
        "gold_claims": [],
        "stage_labels": [],
        "near_match_rejections": [],
        "evaluator_panel": panel_config("evaluator", EVALUATORS),
        "diagnostics": {
            "ballots": 3,
            "latency_seconds": 1.0,
            "stage_latency_seconds": {"contract": 1.0, "pair": 0.0},
            "peak_memory_bytes": 1,
            "failures": 3,
            "retries": 0,
        },
    }
    run = {
        "benchmark_hash": fingerprint(bench),
        "evidence_mode": "synthetic-fixture",
        "scored_before_case_review": True,
        "pipeline_configuration": {
            "compiler": "test",
            "protocol": "local-unanimous-consensus/1",
            "producer_panel_manifest": panel_config("producer", PRODUCERS),
            "schema": "1.0.0",
        },
        "acceptance_policy": {
            "version": "consensus-agreement/1",
            "allowed_scopes": ["OBSERVED_EVENT"],
            "allowed_settlement_states": ["CONDITIONAL"],
            "require_resolved": False,
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
        "pair_outcomes": [row | {"state": "abstained", "reason": "dissent"} for row in comparisons],
        "diagnostics": {
            "ballots": 3,
            "latency_seconds": 1.0,
            "stage_latency_seconds": {"contract": 1.0, "pair": 0.0},
            "peak_memory_bytes": 1,
            "failures": 3,
            "retries": 0,
        },
        "complete_zero_error_scans": True,
        "unanimous_cross_venue_relationship": False,
        "unanimous_near_match_rejection": True,
        "zero_accepted_known_false_equivalences": True,
        "provenance_invalidation": True,
    }
    run["pair_outcomes"][0] = comparisons[0] | {"state": "complete"}
    run["stage_outputs"] = [
        {
            "id": fingerprint(comparisons[0]),
            "operands": ["a", "b"],
            "stage": "settlement_compatibility",
            "mode": "pipeline",
            "values": {"state": "UNKNOWN", "conditions": []},
        }
    ]
    report = evaluate(bench, run)
    assert report["consensus"]["label_abstentions"] == 3
    assert report["consensus"]["label_coverage"] == 0
    assert report["release_gates"]["evaluator_label_coverage"] is False
    assert report["relationships"]["selected_precision"]["value"] is None
    assert report["relationships"]["selected_recall"]["value"] is None
    assert report["consensus"]["pair_label_coverage"]["value"] == 0
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

    expected = cite_expected(store, ir.contract_version_id, canonicalize_contract_from_ir(ir))
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
    other_ir = SemanticIR.model_validate(other["data"]["ir"])
    expected_b = cite_expected(
        store, other_ir.contract_version_id, canonicalize_contract_from_ir(other_ir)
    )

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

            expected = cite_expected(
                dataset, ir.contract_version_id, canonicalize_contract_from_ir(ir)
            )

            def gen(prompt, schema, expected=expected):
                return json.dumps(expected)

            approve_interpretation(dataset, identity, paths, generator=gen, manifests=PRODUCERS)
        pipeline = Pipeline(dataset, allow_consensus=True)
        pipeline.review(
            identities[0],
            "operator",
            "human approval of the exact IR",
            True,
            True,
        )
        published = pipeline.publish()
        assert published
        export = Pipeline(dataset).export()
        assert {row["data"]["acceptance_basis"] for row in export["assertions"]} == {
            LOCAL_MODEL_CONSENSUS
        }
        pipeline.review(
            identities[1],
            "operator",
            "human approval of the exact IR",
            True,
            True,
        )
        published = pipeline.publish()
        assert published
        export = Pipeline(dataset).export()
        assert {row["data"]["acceptance_basis"] for row in export["assertions"]} == {HUMAN_REVIEW}
    finally:
        dataset.close()


def test_consensus_approve_cli_exists_and_does_not_insert_review(tmp_path, monkeypatch, capsys):
    from oddsfox.cli import main, parser
    from oddsfox.store import Store

    parsed = parser().parse_args(
        ["consensus-approve", "interp", "--producer-model", str(tmp_path / "p0")]
    )
    assert parsed.command == "consensus-approve"
    assert parsed.interpretation_id == "interp"

    dataset_path = tmp_path / "cli-dataset"
    store = Store(dataset_path)
    interpretation_id = load_demo(store)["interpretations"][0]
    assert store.list("review") == []
    store.close()

    seen = []

    def stub(store, interpretation_id, producer_paths, **kwargs):
        seen.append((interpretation_id, [str(path) for path in producer_paths], kwargs))
        return "consensus-approval-id"

    monkeypatch.setattr("oddsfox.consensus.approve_interpretation", stub)
    producer = tmp_path / "p0"
    assert (
        main(
            [
                "--data",
                str(dataset_path),
                "consensus-approve",
                interpretation_id,
                "--producer-model",
                str(producer),
            ]
        )
        == 0
    )
    assert seen == [(interpretation_id, [str(producer)], {})]
    assert json.loads(capsys.readouterr().out) == {"id": "consensus-approval-id"}

    store = Store(dataset_path)
    try:
        assert store.list("review") == []
        assert store._rows("SELECT id FROM nodes WHERE kind=?", ["review"]) == []
        assert store.current("review", interpretation_id) is None
    finally:
        store.close()
