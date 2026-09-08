from copy import deepcopy

import pytest

from oddsfox.evaluation import claim_key, closure, comparison_key, evaluate
from oddsfox.ir import fingerprint


def claim(a, b, relation="IMPLIES", good=True):
    return {
        "a": a,
        "b": b,
        "relation": relation,
        "scope": "OBSERVED_EVENT",
        "conditions": [],
        "proof": {"state": "PROVEN_UNDER_PREMISES"},
        "settlement": {"state": "CONDITIONAL" if good else "UNKNOWN"},
        "interpretation_assessments": ["SUPPORTED", "SUPPORTED"],
    }


def benchmark_run():
    bench = {
        "benchmark_id": "synthetic-test",
        "revision": "1",
        "label_version": "1",
        "labeling_guide": "exact-string-set/1",
        "split": "development",
        "independent_human_labels": False,
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
            {k: v for k, v in claim(a, b).items() if k in {"a", "b", "scope", "conditions"}}
            for a, b in [("a", "b"), ("b", "c"), ("a", "c")]
        ],
        "gold_claims": [claim("a", "b"), claim("b", "c")],
        "stage_labels": [
            {
                "id": "a",
                "stage": "ir_fields",
                "mode": "pipeline",
                "expected": {"source": None, "threshold": "1"},
            }
        ],
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
        "proposals": [claim("a", "b"), claim("b", "c", good=False)],
        "stage_outputs": [],
        "outcomes": [],
    }
    return bench, run


def test_duplicates_and_unselected_paths_cannot_inflate_scores():
    bench, run = benchmark_run()
    original = evaluate(bench, run)
    repeated = evaluate(bench, run | {"proposals": run["proposals"] * 5})
    assert repeated["relationships"] == original["relationships"]
    assert original["relationships"]["selected_recall"]["correct"] == 1
    assert original["relationships"]["all_recall"]["correct"] == 3
    assert original["relationships"]["selected_recall"]["total"] == 3
    assert original["stages"]["ir_fields/pipeline"]["fields"]["correct"] == 0
    assert original["stages"]["ir_fields/pipeline"]["fields"]["total"] == 2
    assert original["release_gates"]["independent_human_labels"] is False


def test_exclusion_and_complement_do_not_chain():
    bench, _ = benchmark_run()
    universe = {comparison_key(c) for c in bench["comparisons"]}
    for relation in ("EXCLUDES", "COMPLEMENT"):
        keys = {claim_key(claim("a", "b", relation)), claim_key(claim("b", "c", relation))}
        assert closure(keys, universe) == keys


def test_conditions_and_direction_are_part_of_claim_identity():
    assert claim_key(claim("a", "b")) != claim_key(claim("b", "a"))
    assert claim_key(claim("a", "b", "EQUIVALENT")) == claim_key(claim("b", "a", "EQUIVALENT"))
    assert claim_key(claim("a", "b") | {"conditions": ["C"]}) != claim_key(claim("a", "b"))


def test_empty_denominator_not_perfect_and_changed_labels_rejected():
    bench, run = benchmark_run()
    result = evaluate(bench, run | {"proposals": []})
    assert result["relationships"]["selected_precision"]["value"] is None
    assert result["relationships"]["selected_recall"]["value"] == 0
    modified = deepcopy(bench)
    modified["revision"] = "2"
    with pytest.raises(ValueError, match="frozen"):
        evaluate(modified, run)
    with pytest.raises(ValueError, match="pre-review"):
        evaluate(bench, run | {"scored_before_case_review": False})


def test_selected_false_positive_counts_as_coverage_and_error():
    bench, run = benchmark_run()
    run["proposals"].append(claim("b", "a"))
    result = evaluate(bench, run)
    assert result["relationships"]["selected_precision"]["value"] < 1
    assert result["relationships"]["automatic_acceptance_coverage"]["value"] > 0


def test_missing_output_is_not_correct_unknown():
    bench, run = benchmark_run()
    run["stage_outputs"] = [
        {"id": "a", "stage": "ir_fields", "mode": "pipeline", "values": {"source": None}}
    ]
    result = evaluate(bench, run)
    assert result["stages"]["ir_fields/pipeline"]["fields"]["correct"] == 1
    assert result["stages"]["ir_fields/pipeline"]["whole_record"]["correct"] == 0


@pytest.mark.parametrize("conflicting", [False, True])
def test_duplicate_stage_labels_rejected(conflicting):
    bench, run = benchmark_run()
    duplicate = deepcopy(bench["stage_labels"][0])
    if conflicting:
        duplicate["expected"]["threshold"] = "2"
    bench["stage_labels"].append(duplicate)
    run["benchmark_hash"] = fingerprint(bench)
    with pytest.raises(ValueError, match="duplicate stage label"):
        evaluate(bench, run)


def test_ineligible_outcomes_counted_with_explicit_eligible_denominators():
    bench, run = benchmark_run()
    bench["contracts"][0]["eligible"] = False
    run["benchmark_hash"] = fingerprint(bench)
    run["outcomes"] = [
        {"id": "a", "stage": "interpret", "state": "unsupported"},
        {"id": "a", "stage": "resolve", "state": "failed"},
        {"id": "b", "stage": "interpret", "state": "abstained", "reason": "ambiguous"},
        {"id": "c", "stage": "interpret", "state": "complete"},
    ]
    result = evaluate(bench, run)
    assert result["abstention"]["interpret"]["unsupported"] == 1
    assert result["abstention"]["resolve"]["failures"] == 1
    assert result["abstention"]["interpret"]["rate"]["value"] == 0.5
    poly = result["breakdowns"]["venue:polymarket"]
    assert poly["coverage"]["eligible_per_sampled"]["value"] == 0
    assert poly["abstention"]["interpret"]["unsupported"] == 1
    assert poly["abstention"]["interpret"]["rate"]["value"] is None
    assert result["breakdowns"]["venue:kalshi"]["abstention"]["interpret"]["reasons"] == {
        "ambiguous": 1
    }
    assert result["breakdowns"]["template:test"]["coverage"] == result["coverage"]


def test_duplicate_ineligible_outcomes_rejected():
    bench, run = benchmark_run()
    bench["contracts"][0]["eligible"] = False
    run["benchmark_hash"] = fingerprint(bench)
    run["outcomes"] = [{"id": "a", "stage": "interpret", "state": "unsupported"}] * 2
    with pytest.raises(ValueError, match="duplicate stage outcome"):
        evaluate(bench, run)
