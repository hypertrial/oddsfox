"""Frozen, pre-adjudication evaluation with complete-claim identities."""

import base64
import hashlib
import math
from collections import Counter, defaultdict
from statistics import median

from oddsfox.ir import fingerprint
from oddsfox.reasoning import RELATIONS

METRIC_VERSION = "oddsfox-metrics/3"
METRIC_V4 = "oddsfox-metrics/4"
CONSENSUS_AGREEMENT = "consensus-agreement/1"
CONSENSUS_PROTOCOL = "local-unanimous-consensus/1"
STAGES = ("ir_fields", "canonical_resolution", "settlement_fields", "settlement_compatibility")
CONTRACT_STAGE_FIELDS = {
    "ir_fields": {
        "quantity",
        "source",
        "instrument_or_series",
        "unit",
        "comparator",
        "threshold",
        "measurement_method",
        "timestamp",
        "timezone",
        "precision",
        "revision_policy",
    },
    "canonical_resolution": {"canonical_observation_id", "canonical_observation_version"},
    "settlement_fields": {
        "resolution_source",
        "cutoff",
        "rounding",
        "missing_data_policy",
        "cancellation_policy",
        "exceptional_outcome_policy",
        "dispute_policy",
        "clarification_policy",
    },
}


def _validate_panel(panel: object, role: str) -> dict:
    if not isinstance(panel, dict) or panel.get("protocol") != CONSENSUS_PROTOCOL:
        raise ValueError("metrics v4 requires frozen consensus panel provenance")
    if (
        panel.get("role") != role
        or panel.get("unanimity") != "exact-canonical"
        or panel.get("prompt") != "judge-ballots/2"
        or panel.get("sampling") != {"temperature": 0}
    ):
        raise ValueError("metrics v4 panel role or unanimity is invalid")
    models = panel.get("models")
    if not isinstance(models, list) or len(models) < 3:
        raise ValueError("metrics v4 requires at least three panel models")
    for field in ("identity", "lineage", "weights_revision"):
        values = [model.get(field) for model in models if isinstance(model, dict)]
        if len(values) != len(models) or any(
            not isinstance(value, str) or not value for value in values
        ):
            raise ValueError(f"metrics v4 panel models require {field}")
        if len(set(values)) != len(values):
            raise ValueError(f"metrics v4 panel models require distinct {field}")
    return panel


def _validate_diagnostics(value: object, owner: str) -> dict:
    required = {
        "ballots",
        "latency_seconds",
        "stage_latency_seconds",
        "peak_memory_bytes",
        "failures",
        "retries",
    }
    if not isinstance(value, dict) or set(value) != required:
        raise ValueError(f"metrics v4 requires complete {owner} diagnostics")
    counts = {"ballots", "peak_memory_bytes", "failures", "retries"}
    if any(type(value[key]) is not int or value[key] < 0 for key in counts):
        raise ValueError(f"metrics v4 {owner} diagnostics must be nonnegative")
    latency = value["latency_seconds"]
    if (
        isinstance(latency, bool)
        or not isinstance(latency, int | float)
        or not math.isfinite(latency)
        or latency < 0
    ):
        raise ValueError(f"metrics v4 {owner} diagnostics must be nonnegative")
    stages = value["stage_latency_seconds"]
    if (
        not isinstance(stages, dict)
        or set(stages) != {"contract", "pair"}
        or any(
            isinstance(item, bool)
            or not isinstance(item, int | float)
            or not math.isfinite(item)
            or item < 0
            for item in stages.values()
        )
    ):
        raise ValueError(f"metrics v4 {owner} stage diagnostics must be nonnegative")
    return value


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


def comparison_identity(c: dict) -> str:
    return fingerprint(
        {
            "a": c["a"],
            "b": c["b"],
            "scope": c["scope"],
            "conditions": c["conditions"],
        }
    )


def _rows_match(left: list[dict], right: list[dict]) -> bool:
    return sorted(fingerprint(row) for row in left) == sorted(fingerprint(row) for row in right)


def _contract_stages(identity: str, label: dict, value_key: str) -> list[dict]:
    return [
        {
            "id": identity,
            "stage": stage,
            "mode": "pipeline",
            value_key: {key: label.get(key) for key in fields},
        }
        for stage, fields in CONTRACT_STAGE_FIELDS.items()
    ]


def _pair_stage(pair: dict, label: dict, value_key: str) -> dict:
    return {
        "id": comparison_identity(pair),
        "operands": [pair["a"], pair["b"]],
        "stage": "settlement_compatibility",
        "mode": "pipeline",
        value_key: {
            "state": label["settlement_compatibility"],
            "conditions": label["conditions"],
        },
    }


def _pair_claim(pair: dict, label: dict) -> dict | None:
    if label["relationship"] in {"NEAR_MATCH", "NONE"}:
        return None
    return {
        "a": pair["a"],
        "b": pair["b"],
        "relation": label["relationship"],
        "scope": label["scope"],
        "conditions": label["conditions"],
    }


def _evidence_diagnostics(measurements: list[dict]) -> dict:
    return {
        "ballots": len(measurements),
        "latency_seconds": sum(row.get("latency_seconds", 0) for row in measurements),
        "stage_latency_seconds": {
            stage: sum(
                row.get("latency_seconds", 0) for row in measurements if row.get("stage") == stage
            )
            for stage in ("contract", "pair")
        },
        "peak_memory_bytes": max(
            (row.get("peak_memory_bytes", 0) for row in measurements), default=0
        ),
        "failures": sum(row.get("failures", 0) for row in measurements),
        "retries": sum(row.get("retries", 0) for row in measurements),
    }


def _validate_evidence_projection(
    benchmark: dict,
    run: dict,
    label_sets: dict[str, dict],
    ballots: dict[str, dict],
    evaluator_ids: set[str],
    producer_ids: set[str],
) -> None:
    comparisons = {comparison_identity(row): row for row in benchmark["comparisons"]}
    contracts = {row["id"]: row for row in benchmark["contracts"]}

    def project(ids: set[str], value_key: str) -> dict:
        stages, outcomes, pair_outcomes, claims, abstentions, disagreements = [], [], [], [], [], []
        pair_labels, near_rejections, measurements = [], [], []
        eligible = {}
        for identity in ids:
            data = label_sets[identity]["data"]
            label = data["label"]
            reason = data.get("abstention")
            measurements.extend(
                ballots[ballot_id]["data"].get("measurement", {}) for ballot_id in data["ballots"]
            )
            if data["kind"] == "contract":
                subject = data["subject"]
                if label is None:
                    abstentions.append({"id": subject, "reason": reason})
                    outcomes.append(
                        {
                            "id": subject,
                            "stage": "interpret",
                            "state": "abstained",
                            "reason": reason,
                        }
                    )
                    if reason == "dissent":
                        disagreements.append({"id": subject, "kind": "contract"})
                else:
                    eligible[subject] = bool(label.get("eligible"))
                    stages.extend(_contract_stages(subject, label, value_key))
                    outcomes.append({"id": subject, "stage": "interpret", "state": "complete"})
                continue

            pair = comparisons[data["subject"]]
            pair_id = f"{pair['a']}/{pair['b']}"
            mismatch = label is not None and (
                label["scope"] != pair["scope"]
                or conditions(label["conditions"]) != conditions(pair["conditions"])
            )
            if label is None or mismatch:
                outcome_reason = "pair identity mismatch" if mismatch else reason
                abstentions.append({"id": pair_id, "reason": outcome_reason})
                pair_outcomes.append({**pair, "state": "abstained", "reason": outcome_reason})
                if reason == "dissent":
                    disagreements.append({"id": pair_id, "kind": "pair"})
                continue
            pair_outcomes.append({**pair, "state": "complete"})
            pair_labels.append({**pair, "relationship": label["relationship"]})
            stages.append(_pair_stage(pair, label, value_key))
            if label["relationship"] == "NEAR_MATCH":
                near_rejections.append(
                    {
                        **pair,
                        "differences": label.get("near_match_differences", []),
                    }
                )
            claim = _pair_claim(pair, label)
            if claim:
                claims.append(claim)
        return {
            "stages": stages,
            "outcomes": outcomes,
            "pair_outcomes": pair_outcomes,
            "claims": claims,
            "abstentions": abstentions,
            "disagreements": disagreements,
            "pair_labels": pair_labels,
            "near_rejections": near_rejections,
            "eligible": eligible,
            "diagnostics": _evidence_diagnostics(measurements),
        }

    evaluator = project(evaluator_ids, "expected")
    producer = project(producer_ids, "values")
    if (
        not _rows_match(evaluator["stages"], benchmark["stage_labels"])
        or not _rows_match(evaluator["pair_outcomes"], benchmark["pair_outcomes"])
        or not _rows_match(evaluator["claims"], benchmark["gold_claims"])
        or not _rows_match(evaluator["abstentions"], benchmark["label_abstentions"])
        or not _rows_match(evaluator["disagreements"], benchmark["disagreements"])
        or not _rows_match(evaluator["pair_labels"], benchmark["pair_labels"])
        or not _rows_match(evaluator["near_rejections"], benchmark["near_match_rejections"])
        or evaluator["diagnostics"] != benchmark["diagnostics"]
        or any(
            contracts[identity]["eligible"] != value
            for identity, value in evaluator["eligible"].items()
        )
    ):
        raise ValueError("metrics v4 evaluator evidence does not match the frozen corpus")
    if (
        not _rows_match(producer["stages"], run["stage_outputs"])
        or not _rows_match(producer["outcomes"], run["outcomes"])
        or not _rows_match(producer["pair_outcomes"], run["pair_outcomes"])
        or not _rows_match(producer["claims"], run["proposals"])
        or producer["diagnostics"] != run["diagnostics"]
    ):
        raise ValueError("metrics v4 producer evidence does not match the frozen run")


def _validate_judge_evidence(
    benchmark: dict,
    run: dict,
    evidence: dict,
    evaluator_panel: dict,
    producer_panel: dict,
) -> None:
    evaluator_ids = set(benchmark["evaluator_label_set_ids"])
    producer_ids = set(run["producer_label_set_ids"])
    if evaluator_ids & producer_ids:
        raise ValueError("metrics v4 judge evidence label sets overlap")
    label_sets = {row.get("id"): row for row in evidence.get("label_sets", [])}
    ballots = {row.get("id"): row for row in evidence.get("ballots", [])}
    configs = evidence.get("configs")
    evaluator_config = benchmark.get("evaluator_panel_id")
    producer_config = run.get("pipeline_configuration", {}).get("producer_panel")
    if (
        set(label_sets) != evaluator_ids | producer_ids
        or not isinstance(configs, dict)
        or set(configs) != {evaluator_config, producer_config}
        or configs[evaluator_config].get("data") != evaluator_panel
        or configs[producer_config].get("data") != producer_panel
        or any(configs[key].get("current") is not True for key in configs)
    ):
        raise ValueError("metrics v4 judge evidence configs or label sets are not linked")
    expected_subjects = {("contract", row["id"]) for row in benchmark["contracts"]} | {
        ("pair", comparison_identity(row)) for row in benchmark["comparisons"]
    }
    pair_operands = {
        comparison_identity(row): {row["a"], row["b"]} for row in benchmark["comparisons"]
    }
    edges = {(row.get("child"), row.get("parent")) for row in evidence.get("dependencies", [])}
    dependencies_by_child: dict[str, list[str]] = defaultdict(list)
    for child, parent in edges:
        dependencies_by_child[child].append(parent)
    for expected_kind, rows in (
        ("judge_config", configs.values()),
        ("consensus_label_set", label_sets.values()),
        ("judge_ballot", ballots.values()),
    ):
        for row in rows:
            identity = row.get("id")
            expected_identity = fingerprint(
                {
                    "kind": expected_kind,
                    "logical": row.get("logical"),
                    "data": row.get("data"),
                    "dependencies": sorted(set(dependencies_by_child[identity])),
                }
            )
            if (
                row.get("kind") != expected_kind
                or identity != expected_identity
                or row.get("current") is not True
            ):
                raise ValueError("metrics v4 judge evidence node identity is invalid")
    referenced_ballots = set()
    referenced_raw = set()
    referenced_sources = set()
    for owner_ids, config_id, panel in (
        (evaluator_ids, evaluator_config, evaluator_panel),
        (producer_ids, producer_config, producer_panel),
    ):
        rows = [label_sets[identity] for identity in owner_ids]
        if {
            (row.get("data", {}).get("kind"), row.get("data", {}).get("subject")) for row in rows
        } != expected_subjects:
            raise ValueError("metrics v4 judge evidence subjects are incomplete")
        expected_models = {model["identity"]: model for model in panel["models"]}
        for label_set in rows:
            data = label_set.get("data", {})
            ballot_ids = data.get("ballots")
            if (
                label_set.get("current") is not True
                or data.get("config_id") != config_id
                or not isinstance(ballot_ids, list)
                or len(ballot_ids) != len(expected_models)
                or len(set(ballot_ids)) != len(ballot_ids)
                or set(ballot_ids) - set(ballots)
                or (label_set["id"], config_id) not in edges
                or any((label_set["id"], identity) not in edges for identity in ballot_ids)
            ):
                raise ValueError("metrics v4 judge evidence label set is incomplete")
            referenced_ballots.update(ballot_ids)
            ballot_rows = [ballots[identity] for identity in ballot_ids]
            if {row.get("data", {}).get("model", {}).get("identity") for row in ballot_rows} != set(
                expected_models
            ):
                raise ValueError("metrics v4 judge evidence panel membership is invalid")
            for ballot in ballot_rows:
                ballot_data = ballot.get("data", {})
                model_id = ballot_data.get("model", {}).get("identity")
                if (
                    ballot.get("current") is not True
                    or ballot_data.get("protocol") != CONSENSUS_PROTOCOL
                    or ballot_data.get("prompt") != "judge-ballots/2"
                    or ballot_data.get("config_id") != config_id
                    or ballot_data.get("kind") != data.get("kind")
                    or ballot_data.get("subject") != data.get("subject")
                    or ballot_data.get("model") != expected_models[model_id]
                    or (ballot["id"], config_id) not in edges
                ):
                    raise ValueError("metrics v4 judge evidence ballot linkage is invalid")
                subject = data.get("subject")
                required_parents = (
                    pair_operands[subject] if data.get("kind") == "pair" else {subject}
                )
                if any((ballot["id"], parent) not in edges for parent in required_parents):
                    raise ValueError("metrics v4 judge evidence source dependency is missing")
                raw_id = ballot_data.get("raw_response_artifact")
                if ballot_data.get("payload") and not raw_id:
                    raise ValueError("metrics v4 judge evidence raw response is missing")
                if raw_id:
                    referenced_raw.add(raw_id)
                referenced_sources.update(
                    citation["artifact_id"]
                    for citation in ballot_data.get("payload", {}).get("citations", [])
                )
            label = data.get("label")
            if label is not None and any(
                row["data"].get("payload") != label for row in ballot_rows
            ):
                raise ValueError("metrics v4 judge evidence is not unanimous")
    if set(ballots) != referenced_ballots:
        raise ValueError("metrics v4 judge evidence contains unrelated ballots")
    if (
        set(evidence.get("raw_responses", {})) != referenced_raw
        or set(evidence.get("cited_sources", {})) != referenced_sources
    ):
        raise ValueError("metrics v4 judge evidence artifacts are not exact")
    _validate_evidence_projection(benchmark, run, label_sets, ballots, evaluator_ids, producer_ids)


def in_universe(key: tuple, universe: set[tuple]) -> bool:
    a, b, _, scope, cond = key
    return (*sorted((a, b)), scope, cond) in universe


def closure(
    claims: set[tuple], universe: set[tuple], blocked: set[tuple] | None = None
) -> set[tuple]:
    blocked = blocked or set()
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
                if in_universe(key, universe) and (*sorted((a, b)), scope, cond) not in blocked:
                    result.add(key)
                if a in reach[b]:
                    key = (*sorted((a, b)), "EQUIVALENT", scope, cond)
                    if in_universe(key, universe) and (*sorted((a, b)), scope, cond) not in blocked:
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
    gold: set,
    proposed: set,
    chosen: set,
    universe: set,
    full_universe: set | None = None,
    blocked: set[tuple] | None = None,
) -> dict:
    closure_universe = full_universe if full_universe is not None else universe
    gold_closed = {c for c in closure(gold, closure_universe, blocked) if in_universe(c, universe)}
    all_closed = {c for c in closure(proposed, closure_universe) if in_universe(c, universe)}
    selected_closed = {c for c in closure(chosen, closure_universe) if in_universe(c, universe)}
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
        if key in by_id:
            raise ValueError("duplicate stage output")
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


def evaluate(benchmark: dict, run: dict, *, _trusted_evidence: bool = False) -> dict:
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
    evidence_verified = False
    if label_source == "local_unanimous_consensus":
        if version != METRIC_V4:
            raise ValueError("consensus corpora must use oddsfox-metrics/4")
        if benchmark["independent_human_labels"] is not False:
            raise ValueError("consensus labels cannot set independent_human_labels")
        if run.get("cross_venue_human_validation") or run.get("near_match_human_validation"):
            raise ValueError("model ballots cannot set human-validation flags")
        if benchmark.get("label_version") != CONSENSUS_PROTOCOL:
            raise ValueError("metrics v4 requires the frozen consensus label protocol")
        evaluator_panel = _validate_panel(benchmark.get("evaluator_panel"), "evaluator")
        producer_panel = run.get("pipeline_configuration", {}).get("producer_panel_manifest")
        producer_panel = _validate_panel(producer_panel, "producer")
        for field in ("identity", "lineage", "weights_revision"):
            evaluator_values = {model[field] for model in evaluator_panel["models"]}
            producer_values = {model[field] for model in producer_panel["models"]}
            if evaluator_values & producer_values:
                raise ValueError(f"metrics v4 panels must have disjoint {field}")
        if run.get("pipeline_configuration", {}).get("protocol") != CONSENSUS_PROTOCOL:
            raise ValueError("metrics v4 requires the frozen producer protocol")
        _validate_diagnostics(benchmark.get("diagnostics"), "evaluator")
        _validate_diagnostics(run.get("diagnostics"), "producer")
        evidence_mode = run.get("evidence_mode")
        if evidence_mode == "immutable-local-store":
            evidence_hash = run.get("judge_evidence_hash")
            evidence = run.get("judge_evidence")
            if (
                not isinstance(evidence_hash, str)
                or len(evidence_hash) != 64
                or any(char not in "0123456789abcdef" for char in evidence_hash)
                or not benchmark.get("evaluator_label_set_ids")
                or not run.get("producer_label_set_ids")
                or not isinstance(evidence, dict)
                or fingerprint(evidence) != evidence_hash
            ):
                raise ValueError("metrics v4 requires immutable judge evidence provenance")
            _validate_judge_evidence(benchmark, run, evidence, evaluator_panel, producer_panel)
            for group in ("raw_responses", "cited_sources"):
                records = evidence.get(group)
                if not isinstance(records, dict) or any(
                    not isinstance(row, dict)
                    or row.get("sha256") != identity
                    or hashlib.sha256(base64.b64decode(row.get("base64", ""))).hexdigest()
                    != identity
                    for identity, row in records.items()
                ):
                    raise ValueError("metrics v4 judge evidence artifacts are invalid")
            evidence_verified = True
        elif evidence_mode != "synthetic-fixture" or benchmark.get("split") != "development":
            raise ValueError("metrics v4 requires immutable judge evidence provenance")
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
    if version == METRIC_V4 and policy.get("version") != CONSENSUS_AGREEMENT:
        raise ValueError("metrics v4 requires consensus-agreement/1 acceptance policy")
    if version != METRIC_V4 and policy.get("version") == CONSENSUS_AGREEMENT:
        raise ValueError("metrics v3 cannot select the consensus acceptance policy")
    # Validate policy even when no claims were emitted.
    selected({"a": "a", "b": "b", "scope": "OBSERVED_EVENT"}, policy)
    universe = {comparison_key(c) for c in benchmark["comparisons"]}
    if len(universe) != len(benchmark["comparisons"]):
        raise ValueError("duplicate benchmark comparisons")
    gold = {claim_key(c) for c in benchmark["gold_claims"]}
    scoring_universe = universe
    negative_universe = set()
    if version == METRIC_V4:
        pair_labels = benchmark.get("pair_labels")
        if not isinstance(pair_labels, list):
            raise ValueError("metrics v4 requires explicit pair labels")
        if any(
            row.get("relationship") not in {*RELATIONS, "NEAR_MATCH", "NONE"} for row in pair_labels
        ):
            raise ValueError("metrics v4 pair label relationship is invalid")
        scoring_universe = {comparison_key(row) for row in pair_labels}
        negative_universe = {
            comparison_key(row)
            for row in pair_labels
            if row.get("relationship") in {"NONE", "NEAR_MATCH"}
        }
        if len(scoring_universe) != len(pair_labels) or not scoring_universe <= universe:
            raise ValueError("metrics v4 pair labels must be unique members of the frozen universe")
        expected_gold = {
            claim_key(row | {"relation": row["relationship"]})
            for row in pair_labels
            if row.get("relationship") not in {"NONE", "NEAR_MATCH"}
        }
        if gold != expected_gold:
            raise ValueError("metrics v4 gold claims must match explicit evaluator pair labels")
        outcome_keys = {}
        for owner, rows in (
            ("evaluator", benchmark.get("pair_outcomes")),
            ("producer", run.get("pair_outcomes")),
        ):
            if not isinstance(rows, list) or len({comparison_key(row) for row in rows}) != len(
                rows
            ):
                raise ValueError(f"metrics v4 requires unique {owner} pair outcomes")
            if {comparison_key(row) for row in rows} != universe:
                raise ValueError(f"metrics v4 requires complete {owner} pair outcomes")
            if any(row.get("state") not in {"complete", "abstained"} for row in rows):
                raise ValueError(f"metrics v4 {owner} pair outcome state is invalid")
            if any(
                row.get("state") == "abstained"
                and (not isinstance(row.get("reason"), str) or not row["reason"].strip())
                for row in rows
            ):
                raise ValueError(f"metrics v4 {owner} pair abstention requires a reason")
            outcome_keys[owner] = {
                comparison_key(row) for row in rows if row.get("state") == "complete"
            }
        if outcome_keys["evaluator"] != scoring_universe:
            raise ValueError("metrics v4 evaluator labels and complete outcomes must match")
        contract_ids = {row["id"] for row in benchmark["contracts"]}
        contract_abstention_rows = [
            row for row in benchmark.get("label_abstentions", []) if row.get("id") in contract_ids
        ]
        if any(
            not isinstance(row.get("reason"), str) or not row["reason"].strip()
            for row in contract_abstention_rows
        ):
            raise ValueError("metrics v4 evaluator contract abstention requires a reason")
        contract_abstentions = {row.get("id") for row in contract_abstention_rows}
        active_contracts = contract_ids - contract_abstentions
        for stage, required_fields in CONTRACT_STAGE_FIELDS.items():
            rows = [
                row
                for row in benchmark["stage_labels"]
                if row.get("stage") == stage and row.get("mode") == "pipeline"
            ]
            if {row.get("id") for row in rows} != active_contracts or any(
                required_fields != set(row.get("expected", {})) for row in rows
            ):
                raise ValueError("metrics v4 requires complete labels for every mandatory stage")
        pair_stage_rows = [
            row
            for row in benchmark["stage_labels"]
            if row.get("stage") == "settlement_compatibility"
            and row.get("mode") == "pipeline"
            and {"state", "conditions"} == set(row.get("expected", {}))
        ]
        expected_pair_operands = {
            comparison_identity(row): sorted((row["a"], row["b"])) for row in pair_labels
        }
        if {row.get("id") for row in pair_stage_rows} != set(expected_pair_operands) or any(
            not isinstance(row.get("operands"), list)
            or sorted(row["operands"]) != expected_pair_operands.get(row.get("id"))
            for row in pair_stage_rows
        ):
            raise ValueError("metrics v4 requires complete labels for every mandatory stage")
        producer_complete_pairs = {
            comparison_identity(row)
            for row in run["pair_outcomes"]
            if row.get("state") == "complete"
        }
        producer_pair_outputs = [
            row
            for row in run.get("stage_outputs", [])
            if row.get("stage") == "settlement_compatibility" and row.get("mode") == "pipeline"
        ]
        comparison_operands = {
            comparison_identity(row): sorted((row["a"], row["b"]))
            for row in benchmark["comparisons"]
        }
        if {row.get("id") for row in producer_pair_outputs} != producer_complete_pairs or any(
            not isinstance(row.get("operands"), list)
            or sorted(row["operands"]) != comparison_operands.get(row.get("id"))
            or set(row.get("values", {})) != {"state", "conditions"}
            for row in producer_pair_outputs
        ):
            raise ValueError("metrics v4 producer pair stages must match complete outcomes")
        producer_complete_contracts = {
            row.get("id")
            for row in run.get("outcomes", [])
            if row.get("stage") == "interpret" and row.get("state") == "complete"
        }
        for stage in CONTRACT_STAGE_FIELDS:
            output_ids = {
                row.get("id")
                for row in run.get("stage_outputs", [])
                if row.get("stage") == stage and row.get("mode") == "pipeline"
            }
            output_rows = [
                row
                for row in run.get("stage_outputs", [])
                if row.get("stage") == stage and row.get("mode") == "pipeline"
            ]
            if output_ids != producer_complete_contracts or any(
                set(row.get("values", {})) != CONTRACT_STAGE_FIELDS[stage] for row in output_rows
            ):
                raise ValueError("metrics v4 producer stages must match complete outcomes")
        near_labels = {
            comparison_key(row) for row in pair_labels if row.get("relationship") == "NEAR_MATCH"
        }
        near_rejections = benchmark.get("near_match_rejections")
        if not isinstance(near_rejections, list) or any(
            not isinstance(row.get("differences"), list) or not row["differences"]
            for row in near_rejections
        ):
            raise ValueError("metrics v4 requires explicit near-match differences")
        if {comparison_key(row) for row in near_rejections} != near_labels:
            raise ValueError("metrics v4 near-match rejections must match pair labels")
    if any(not in_universe(c, scoring_universe) for c in gold):
        raise ValueError("gold claim lies outside the frozen comparison universe")
    proposed, chosen, outside = set(), set(), set()
    for claim in run["proposals"]:
        if version == METRIC_V4 and comparison_key(claim) not in outcome_keys["producer"]:
            raise ValueError("metrics v4 proposals require complete producer pair outcomes")
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
    if version == METRIC_V4 and any(
        row.get("state") == "abstained"
        and (not isinstance(row.get("reason"), str) or not row["reason"].strip())
        for row in outcomes
    ):
        raise ValueError("metrics v4 producer contract abstention requires a reason")
    outcome_metrics = _outcomes(contracts, outcomes)
    breakdown = {}
    for field in ("venue", "template"):
        for value in sorted({c[field] for c in contracts}):
            subset = {c["id"] for c in contracts if c[field] == value}
            comparisons = {c for c in scoring_universe if c[0] in subset or c[1] in subset}
            breakdown[f"{field}:{value}"] = {
                **_outcomes([c for c in contracts if c["id"] in subset], outcomes),
                "relationships": _relationships(
                    gold,
                    proposed,
                    chosen,
                    comparisons,
                    universe,
                    negative_universe,
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
    relationships = _relationships(
        gold, proposed, chosen, scoring_universe, universe, negative_universe
    )
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
            "pair_label_coverage": ratio(len(scoring_universe), len(universe)),
            "pair_label_abstentions": len(universe - scoring_universe),
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
            "unanimous_near_match_rejection": bool(near_labels),
            "zero_accepted_known_false_equivalences": run.get(
                "zero_accepted_known_false_equivalences"
            )
            is True,
            "provenance_invalidation": _trusted_evidence and evidence_verified,
        }
        report["caution"] = (
            "Local unanimous consensus is reproducible panel agreement under a frozen "
            "protocol, not independent human review or proven natural-language correctness."
        )
    return report
