"""Frozen, pre-adjudication evaluation with complete-claim identities."""

import math
from collections import Counter, defaultdict
from statistics import median

from oddsfox.ir import fingerprint
from oddsfox.reasoning import RELATIONS

METRIC_VERSION = "oddsfox-metrics/3"
METRIC_V4 = "oddsfox-metrics/4"
CONSENSUS_AGREEMENT = "consensus-agreement/1"
STAGES = ("ir_fields", "canonical_resolution", "settlement_fields", "settlement_compatibility")


def ratio(n: int, d: int) -> dict:
    if not 0 <= n <= d:
        raise ValueError("invalid metric counts")
    if not d:
        return {"correct": n, "total": d, "value": None, "wilson_95": None}
    p, z = n / d, 1.959963984540054
    denominator = 1 + z * z / d
    center = (p + z * z / (2 * d)) / denominator
    radius = z * math.sqrt(p * (1 - p) / d + z * z / (4 * d * d)) / denominator
    return {
        "correct": n,
        "total": d,
        "value": p,
        "wilson_95": [max(0, center - radius), min(1, center + radius)],
    }


def conditions(values: list[str]) -> tuple[str, ...]:
    if not all(isinstance(x, str) and x.strip() for x in values):
        raise ValueError("conditions must be explicit nonempty strings")
    return tuple(sorted(set(" ".join(x.split()) for x in values)))


def claim_key(c: dict) -> tuple:
    a, b, relation, scope = c["a"], c["b"], c["relation"], c["scope"]
    if not isinstance(a, str) or not isinstance(b, str) or not a or not b or a == b:
        raise ValueError("claim needs distinct contract-version operands")
    if relation not in RELATIONS or scope not in {"OBSERVED_EVENT", "SETTLEMENT_OUTCOME"}:
        raise ValueError("unsupported claim type or scope")
    if relation != "IMPLIES":
        a, b = sorted((a, b))
    return a, b, relation, scope, conditions(c["conditions"])


def comparison_key(c: dict) -> tuple:
    return (*sorted((c["a"], c["b"])), c["scope"], conditions(c["conditions"]))


def in_universe(key: tuple, universe: set[tuple]) -> bool:
    a, b, _, scope, cond = key
    return (*sorted((a, b)), scope, cond) in universe


def closure(claims: set[tuple], universe: set[tuple]) -> set[tuple]:
    result = {c for c in claims if in_universe(c, universe)}
    contexts = defaultdict(set)
    for a, b, relation, scope, cond in result:
        if relation in {"IMPLIES", "EQUIVALENT"}:
            contexts[(scope, cond)].add((a, b))
            if relation == "EQUIVALENT":
                contexts[(scope, cond)].add((b, a))
    for (scope, cond), edges in contexts.items():
        vertices = {x for edge in edges for x in edge}
        reach = {x: {b for a, b in edges if a == x} for x in vertices}
        for k in sorted(vertices):
            for a in sorted(vertices):
                if k in reach[a]:
                    reach[a].update(reach[k])
        for a in vertices:
            for b in reach[a] - {a}:
                key = (a, b, "IMPLIES", scope, cond)
                if in_universe(key, universe):
                    result.add(key)
                if a in reach[b]:
                    key = (*sorted((a, b)), "EQUIVALENT", scope, cond)
                    if in_universe(key, universe):
                        result.add(key)
    return result


def selected(claim: dict, policy: dict) -> bool:
    if set(policy) != {
        "version",
        "allowed_scopes",
        "allowed_settlement_states",
        "require_resolved",
        "normalization",
    }:
        raise ValueError("acceptance policy must record its complete supported configuration")
    if policy["normalization"] != "exact-string-set/1" or not isinstance(
        policy["require_resolved"], bool
    ):
        raise ValueError("unsupported acceptance policy")
    if policy["version"] == CONSENSUS_AGREEMENT:
        if policy["require_resolved"] is not False:
            raise ValueError("consensus agreement cannot require solver resolution")
        return claim["scope"] in policy["allowed_scopes"]
    return (
        claim.get("proof", {}).get("state") == "PROVEN_UNDER_PREMISES"
        and claim["scope"] in policy["allowed_scopes"]
        and claim.get("settlement", {}).get("state") in policy["allowed_settlement_states"]
        and (
            not policy["require_resolved"]
            or all(
                x in {"SUPPORTED", "REVIEWED"} for x in claim.get("interpretation_assessments", [])
            )
            and len(claim.get("interpretation_assessments", [])) == 2
        )
    )


def _relationships(
    gold: set, proposed: set, chosen: set, universe: set, full_universe: set | None = None
) -> dict:
    gold_closed, all_closed, selected_closed = (
        {
            c
            for c in closure(group, full_universe if full_universe is not None else universe)
            if in_universe(c, universe)
        }
        for group in (gold, proposed, chosen)
    )
    proposed = {c for c in proposed if in_universe(c, universe)}
    chosen = {c for c in chosen if in_universe(c, universe)}
    chosen_comparisons = {
        (*sorted((a, b)), scope, cond)
        for a, b, _, scope, cond in chosen
        if in_universe((a, b, "IMPLIES", scope, cond), universe)
    }
    return {
        "all_precision": ratio(len(all_closed & gold_closed), len(all_closed)),
        "all_recall": ratio(len(all_closed & gold_closed), len(gold_closed)),
        "selected_precision": ratio(len(selected_closed & gold_closed), len(selected_closed)),
        "selected_recall": ratio(len(selected_closed & gold_closed), len(gold_closed)),
        "automatic_acceptance_coverage": ratio(len(chosen_comparisons), len(universe)),
        "selected_per_emitted": ratio(len(chosen & proposed), len(proposed)),
        "raw_unique_proposals": len(proposed),
        "raw_unique_selected": len(chosen),
        "closure_counts": {
            "gold": len(gold_closed),
            "all": len(all_closed),
            "selected": len(selected_closed),
        },
    }


def _stages(labels: list[dict], outputs: list[dict]) -> dict:
    label_keys = [(r["id"], r["stage"], r["mode"]) for r in labels]
    if len(set(label_keys)) != len(label_keys):
        raise ValueError("duplicate stage label")
    by_id = {}
    for output in outputs:
        key = (output["id"], output["stage"], output["mode"])
        if key in by_id and output != by_id[key]:
            raise ValueError("conflicting duplicate stage output")
        by_id[key] = output
    result = {}
    for stage in STAGES:
        for mode in ("pipeline", "gold-upstream"):
            rows = [r for r in labels if r["stage"] == stage and r["mode"] == mode]
            total = correct = complete = 0
            fields = defaultdict(lambda: [0, 0])
            confusion = Counter()
            resolved_total = resolved_correct = resolved_labels = 0
            for label in rows:
                output = by_id.get((label["id"], stage, mode))
                expected = label["expected"]
                actual = output.get("values", {}) if output else {}
                row_correct = True
                for key, value in expected.items():
                    equal = key in actual and actual[key] == value
                    if (
                        stage == "settlement_compatibility"
                        and key == "conditions"
                        and key in actual
                    ):
                        equal = (
                            isinstance(value, list)
                            and isinstance(actual[key], list)
                            and conditions(actual[key]) == conditions(value)
                        )
                    total += 1
                    correct += int(equal)
                    fields[key][1] += 1
                    fields[key][0] += int(equal)
                    row_correct &= equal
                complete += int(row_correct)
                if stage == "settlement_compatibility":
                    expected_class = expected.get("state", "UNKNOWN")
                    actual_class = actual.get("state", "MISSING")
                    confusion[f"{expected_class}->{actual_class}"] += 1
                if stage == "canonical_resolution":
                    resolved_labels += 1
                    if actual.get("canonical_observation_id") is not None:
                        resolved_total += 1
                        resolved_correct += int(row_correct)
            result[f"{stage}/{mode}"] = {
                "fields": ratio(correct, total),
                "whole_record": ratio(complete, len(rows)),
                "per_field": {key: ratio(*counts) for key, counts in fields.items()},
                "confusion_matrix": dict(confusion),
                "resolved_precision": ratio(resolved_correct, resolved_total),
                "resolution_coverage": ratio(resolved_total, resolved_labels),
            }
    return result


def _outcomes(contracts: list[dict], outcomes: list[dict]) -> dict:
    eligible = {c["id"] for c in contracts if c["eligible"]}
    sampled = {c["id"] for c in contracts}
    completed = {
        r["id"] for r in outcomes if r["stage"] == "interpret" and r["state"] == "complete"
    } & eligible
    abstentions = {}
    for stage in ("interpret", "resolve", "compare"):
        stage_outcomes = [r for r in outcomes if r["stage"] == stage and r["id"] in sampled]
        grouped = {r["id"]: r for r in stage_outcomes}
        if len(grouped) != len(stage_outcomes):
            raise ValueError("duplicate stage outcome")
        counts = Counter(r["state"] for r in grouped.values())
        abstentions[stage] = {
            "rate": ratio(
                sum(r["state"] == "abstained" and r["id"] in eligible for r in grouped.values()),
                len(eligible),
            ),
            "sampled": len(sampled),
            "abstained": counts["abstained"],
            "reasons": dict(
                Counter(r["reason"] for r in grouped.values() if r["state"] == "abstained")
            ),
            "failures": counts["failed"],
            "unsupported": counts["unsupported"],
            "missing": len(sampled - grouped.keys()),
            "eligible_missing": len(eligible - grouped.keys()),
        }
    return {
        "coverage": {
            "eligible_per_sampled": ratio(len(eligible), len(contracts)),
            "completed_per_eligible": ratio(len(completed), len(eligible)),
        },
        "abstention": abstentions,
    }


def evaluate(benchmark: dict, run: dict) -> dict:
    required = {
        "benchmark_id",
        "revision",
        "label_version",
        "labeling_guide",
        "split",
        "independent_human_labels",
        "contracts",
        "comparisons",
        "gold_claims",
        "stage_labels",
    }
    if not required <= benchmark.keys():
        raise ValueError("benchmark lacks frozen identifiers, labels or comparison universe")
    version = benchmark.get("metric_definition_version", METRIC_VERSION)
    label_source = benchmark.get("label_source")
    if version not in {METRIC_VERSION, METRIC_V4}:
        raise ValueError("unsupported metric definition version")
    if label_source == "local_unanimous_consensus":
        if version != METRIC_V4:
            raise ValueError("consensus corpora must use oddsfox-metrics/4")
        if benchmark["independent_human_labels"] is True:
            raise ValueError("consensus labels cannot set independent_human_labels")
        if run.get("cross_venue_human_validation") or run.get("near_match_human_validation"):
            raise ValueError("model ballots cannot set human-validation flags")
    elif version == METRIC_V4:
        raise ValueError("metrics v4 requires label_source local_unanimous_consensus")
    elif label_source not in {None}:
        raise ValueError("unsupported label_source")
    if run.get("scored_before_case_review") is not True or not run.get("pipeline_configuration"):
        raise ValueError(
            "evaluation needs original pre-review outputs and complete pipeline configuration"
        )
    if run.get("benchmark_hash") != fingerprint(benchmark):
        raise ValueError("benchmark content differs from frozen run hash")
    if len(benchmark["contracts"]) > 250:
        raise ValueError("V1 benchmark batch limited to 250 contracts")
    policy = run["acceptance_policy"]
    # Validate policy even when no claims were emitted.
    selected({"a": "a", "b": "b", "scope": "OBSERVED_EVENT"}, policy)
    universe = {comparison_key(c) for c in benchmark["comparisons"]}
    if len(universe) != len(benchmark["comparisons"]):
        raise ValueError("duplicate benchmark comparisons")
    gold = {claim_key(c) for c in benchmark["gold_claims"]}
    if any(not in_universe(c, universe) for c in gold):
        raise ValueError("gold claim lies outside the frozen comparison universe")
    proposed, chosen, outside = set(), set(), set()
    for claim in run["proposals"]:
        key = claim_key(claim)
        if not in_universe(key, universe):
            outside.add(key)
            continue
        proposed.add(key)
        if selected(claim, policy):
            chosen.add(key)
    contracts = benchmark["contracts"]
    ids = [c["id"] for c in contracts]
    if len(set(ids)) != len(ids):
        raise ValueError("duplicate benchmark contract IDs")
    known = set(ids)
    if any(c["a"] not in known or c["b"] not in known for c in benchmark["comparisons"]):
        raise ValueError("comparison has an unknown contract")
    stages = _stages(benchmark["stage_labels"], run.get("stage_outputs", []))
    outcomes = run.get("outcomes", [])
    if any(
        r["id"] not in known
        or r["stage"] not in {"interpret", "resolve", "compare"}
        or r["state"] not in {"complete", "abstained", "failed", "unsupported"}
        for r in outcomes
    ):
        raise ValueError("unknown outcome contract, stage or state")
    outcome_metrics = _outcomes(contracts, outcomes)
    breakdown = {}
    for field in ("venue", "template"):
        for value in sorted({c[field] for c in contracts}):
            subset = {c["id"] for c in contracts if c[field] == value}
            comparisons = {c for c in universe if c[0] in subset or c[1] in subset}
            breakdown[f"{field}:{value}"] = {
                **_outcomes([c for c in contracts if c["id"] in subset], outcomes),
                "relationships": _relationships(
                    gold,
                    proposed,
                    chosen,
                    comparisons,
                    universe,
                ),
                "stages": _stages(
                    [
                        r
                        for r in benchmark["stage_labels"]
                        if r["id"] in subset or any(x in subset for x in r.get("operands", []))
                    ],
                    run.get("stage_outputs", []),
                ),
            }
    relationships = _relationships(gold, proposed, chosen, universe)
    effort = run.get("paired_review_tasks", [])
    if any(t["manual_seconds"] < 0 or t["assisted_seconds"] < 0 for t in effort):
        raise ValueError("review duration cannot be negative")
    manual = [t["manual_seconds"] for t in effort]
    assisted = [t["assisted_seconds"] for t in effort]
    errors = sum(not t["manual_correct"] or not t["assisted_correct"] for t in effort)
    selected_precision = relationships["selected_precision"]["value"]
    selected_interval = relationships["selected_precision"]["wilson_95"]
    report = {
        "metric_definition_version": version,
        "benchmark": {
            k: benchmark[k]
            for k in ("benchmark_id", "revision", "label_version", "labeling_guide", "split")
        },
        "benchmark_hash": fingerprint(benchmark),
        "run_hash": fingerprint(run),
        "acceptance_policy_version": policy["version"],
        "acceptance_policy": policy,
        "acceptance_policy_hash": fingerprint(policy),
        "pipeline_configuration": run["pipeline_configuration"],
        "relationships": relationships,
        "stages": stages,
        "breakdowns": breakdown,
        **outcome_metrics,
        "unscored_unique_claims": len(outside),
        "review_effort": {
            "tasks": len(effort),
            "manual_median_seconds": median(manual) if manual else None,
            "assisted_median_seconds": median(assisted) if assisted else None,
            "remaining_errors": errors,
            "includes_corrections": run.get("review_times_include_corrections") is True,
        },
        "release_gates": {
            "independent_human_labels": benchmark["independent_human_labels"] is True,
            "selected_precision_target": selected_precision is not None
            and selected_precision >= 0.99,
            "review_effort": bool(
                effort
                and errors == 0
                and median(assisted) < median(manual)
                and run.get("review_times_include_corrections") is True
            ),
            "cross_venue_human_validation": run.get("cross_venue_human_validation") is True,
            "near_match_human_validation": run.get("near_match_human_validation") is True,
        },
        "caution": "Synthetic/development results and small samples do not establish release quality or population error rates.",
    }
    if version == METRIC_V4:
        sampled = len(benchmark["contracts"])
        contract_ids = set(ids)
        contract_abstentions = {
            row.get("id")
            for row in benchmark.get("label_abstentions", [])
            if row.get("id") in contract_ids
        }
        labeled = sampled - len(contract_abstentions)
        coverage = labeled / sampled if sampled else None
        wilson_lower = selected_interval[0] if selected_interval else None
        venues = {c["venue"] for c in benchmark["contracts"]}
        by_id = {c["id"]: c["venue"] for c in benchmark["contracts"]}
        report["label_source"] = "local_unanimous_consensus"
        report["consensus"] = {
            "protocol": benchmark.get("label_version"),
            "evaluator_panel": benchmark.get("evaluator_panel"),
            "label_abstentions": len(contract_abstentions),
            "label_coverage": coverage,
        }
        report["release_gates"] = {
            "independent_human_labels": False,
            "complete_zero_error_scans": run.get("complete_zero_error_scans") is True,
            "sample_size": sampled >= 100,
            "both_venues_represented": venues >= {"kalshi", "polymarket"},
            "evaluator_label_coverage": coverage is not None and coverage >= 0.80,
            "selected_agreement_target": selected_precision is not None
            and selected_precision >= 0.99,
            "selected_agreement_wilson_lower": wilson_lower is not None and wilson_lower >= 0.95,
            "unanimous_cross_venue_relationship": any(
                by_id.get(c["a"]) != by_id.get(c["b"]) for c in benchmark["gold_claims"]
            ),
            "unanimous_near_match_rejection": bool(benchmark.get("near_match_rejections")),
            "zero_accepted_known_false_equivalences": run.get(
                "zero_accepted_known_false_equivalences"
            )
            is True,
            "provenance_invalidation": run.get("provenance_invalidation") is True,
        }
        report["caution"] = (
            "Local unanimous consensus is reproducible panel agreement under a frozen "
            "protocol, not independent human review or proven natural-language correctness."
        )
    return report
