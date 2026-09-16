"""Complete-scan freeze, stratified sampling, and write-once consensus evidence bundles."""

import base64
import ctypes
import errno
import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path

from oddsfox.catalog import candidates_for, sync_status
from oddsfox.discovery import VENUES
from oddsfox.evaluation import CONSENSUS_AGREEMENT, comparison_key, evaluate
from oddsfox.ir import fingerprint
from oddsfox.judge import (
    ContractBallot,
    PairBallot,
    collect_unanimous,
    contract_prompt,
    disjoint_panels,
    gold_claim_from_pair,
    pair_prompt,
    panel_config,
    persist_config,
)
from oddsfox.models import model_manifest
from oddsfox.store import Store

SAMPLE_CAP = 250
SAMPLE_ALGORITHM = "stratified-venue-template-lexical/1"


def scans_complete(store: Store) -> dict:
    status = sync_status(store)
    venues = {row["venue"]: row for row in status["venues"]}
    missing = [venue for venue in VENUES if venue not in venues]
    if missing:
        raise ValueError("both supported venues must have scan state before freeze")
    for _venue, row in venues.items():
        data = row["data"]
        if data.get("state") != "complete" or data.get("error_count", 0) or data.get("errors"):
            raise ValueError("partial or failed scans cannot produce release evidence")
        if not data.get("last_success"):
            raise ValueError("complete scans require a current successful timestamp")
    return status


def inventory(store: Store) -> list[dict]:
    rows = store._rows("SELECT * FROM events WHERE active ORDER BY id")
    contracts = []
    for event in rows:
        for identity in event["data"].get("contracts", []):
            record = store.get(identity)
            contracts.append(
                {
                    "id": record["id"],
                    "venue": record["data"]["platform"],
                    "template": record["data"]["metadata"].get("category")
                    or event.get("category")
                    or "unknown",
                    "eligible": True,
                    "event_id": event["id"],
                    "title": event["title"],
                    "lexical": event["title"].casefold(),
                }
            )
    return contracts


def stratified_sample(contracts: list[dict], *, limit: int = SAMPLE_CAP) -> tuple[list[dict], dict]:
    unique = []
    seen = set()
    for row in contracts:
        if row["id"] in seen:
            continue
        seen.add(row["id"])
        unique.append(row)
    unique.sort(key=lambda c: (c["venue"], c["template"], c["lexical"], c["id"]))
    strata: dict[tuple[str, str, str], list[dict]] = {}
    for row in unique:
        key = (row["venue"], row["template"], row["lexical"][:1] or "_")
        strata.setdefault(key, []).append(row)
    original_counts = {key: len(bucket) for key, bucket in strata.items()}
    selected = []
    while len(selected) < min(limit, len(unique)) and any(strata.values()):
        for key in sorted(strata):
            bucket = strata[key]
            if not bucket:
                continue
            selected.append(bucket.pop(0))
            if len(selected) >= limit:
                break
    selected.sort(key=lambda c: c["id"])
    selected_counts = Counter(
        (row["venue"], row["template"], row["lexical"][:1] or "_") for row in selected
    )
    return selected, {
        "algorithm": SAMPLE_ALGORITHM,
        "limit": limit,
        "inventory": len(unique),
        "sampled": len(selected),
        "strata": {
            f"{a}:{b}:{c}": {
                "inventory": original_counts[(a, b, c)],
                "selected": selected_counts[(a, b, c)],
            }
            for a, b, c in sorted(strata)
        },
    }


def comparison_universe(store: Store, contracts: list[dict]) -> list[dict]:
    known = {c["id"] for c in contracts}
    pairs = []
    seen = set()
    by_event: dict[str, list[str]] = {}
    for row in contracts:
        by_event.setdefault(row["event_id"], []).append(row["id"])
    for event_id in sorted(by_event):
        for candidate in candidates_for(store, event_id):
            other = store._rows("SELECT * FROM events WHERE id=?", [candidate["id"]])
            if not other:
                continue
            for left in by_event[event_id]:
                for right in other[0]["data"].get("contracts", []):
                    if right not in known or left == right:
                        continue
                    key = tuple(sorted((left, right)))
                    if key in seen:
                        continue
                    seen.add(key)
                    pairs.append(
                        {
                            "a": left,
                            "b": right,
                            "scope": "OBSERVED_EVENT",
                            "conditions": [],
                        }
                    )
    # Deterministic hard negatives: adjacent sampled IDs that were not candidates.
    ids = sorted(known)
    for left, right in zip(ids, ids[1:], strict=False):
        key = tuple(sorted((left, right)))
        if key in seen:
            continue
        seen.add(key)
        pairs.append({"a": left, "b": right, "scope": "OBSERVED_EVENT", "conditions": []})
        if len(pairs) >= SAMPLE_CAP:
            break
    pairs.sort(key=comparison_key)
    return pairs


def _rename_exclusive(source: Path, destination: Path) -> None:
    """Atomically publish a directory without replacing an existing path."""
    libc = ctypes.CDLL(None, use_errno=True)
    source_bytes = os.fsencode(source)
    destination_bytes = os.fsencode(destination)
    if sys.platform == "darwin":
        result = libc.renamex_np(source_bytes, destination_bytes, 0x00000004)
    elif hasattr(libc, "renameat2"):
        result = libc.renameat2(-100, source_bytes, -100, destination_bytes, 1)
    else:
        if destination.exists():
            raise FileExistsError(destination)
        source.rename(destination)
        return
    if result == 0:
        return
    error = ctypes.get_errno()
    if error == errno.EEXIST:
        raise FileExistsError(destination)
    raise OSError(error, os.strerror(error), destination)


def write_bundle(directory: Path, files: dict[str, dict]) -> dict:
    if directory.exists():
        raise FileExistsError(directory)
    directory.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{directory.name}.stage-", dir=directory.parent))
    try:
        hashes = {}
        for name, payload in files.items():
            raw = json.dumps(payload, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
            path = staging / name
            with path.open("x", encoding="utf-8") as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            hashes[name] = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        manifest = {"files": hashes, "bundle": fingerprint(files)}
        manifest_raw = json.dumps(manifest, indent=2, ensure_ascii=False) + "\n"
        with (staging / "manifest.json").open("x", encoding="utf-8") as handle:
            handle.write(manifest_raw)
            handle.flush()
            os.fsync(handle.fileno())
        for name, expected in hashes.items():
            if hashlib.sha256((staging / name).read_bytes()).hexdigest() != expected:
                raise ValueError("validation bundle hash mismatch before publication")
        if json.loads((staging / "manifest.json").read_text(encoding="utf-8")) != manifest:
            raise ValueError("validation bundle manifest mismatch before publication")
        staging_fd = os.open(staging, os.O_RDONLY)
        try:
            os.fsync(staging_fd)
        finally:
            os.close(staging_fd)
        _rename_exclusive(staging, directory)
        parent_fd = os.open(directory.parent, os.O_RDONLY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
        return manifest
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def contract_stages(identity: str, label: dict, value_key: str) -> list[dict]:
    groups = {
        "ir_fields": (
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
        ),
        "canonical_resolution": (
            "canonical_observation_id",
            "canonical_observation_version",
        ),
        "settlement_fields": (
            "resolution_source",
            "cutoff",
            "rounding",
            "missing_data_policy",
            "cancellation_policy",
            "exceptional_outcome_policy",
            "dispute_policy",
            "clarification_policy",
        ),
    }
    return [
        {
            "id": identity,
            "stage": stage,
            "mode": "pipeline",
            value_key: {key: label.get(key) for key in fields},
        }
        for stage, fields in groups.items()
    ]


def pair_stage(pair: dict, label: dict, value_key: str) -> dict:
    return {
        "id": fingerprint(pair),
        "operands": [pair["a"], pair["b"]],
        "stage": "settlement_compatibility",
        "mode": "pipeline",
        value_key: {
            "state": label["settlement_compatibility"],
            "conditions": label["conditions"],
        },
    }


def diagnostics(measurements: list[dict]) -> dict:
    stage_latency = {
        stage: sum(
            row.get("latency_seconds", 0) for row in measurements if row.get("stage") == stage
        )
        for stage in ("contract", "pair")
    }
    return {
        "ballots": len(measurements),
        "latency_seconds": sum(row.get("latency_seconds", 0) for row in measurements),
        "stage_latency_seconds": stage_latency,
        "peak_memory_bytes": max(
            (row.get("peak_memory_bytes", 0) for row in measurements), default=0
        ),
        "failures": sum(row.get("failures", 0) for row in measurements),
        "retries": sum(row.get("retries", 0) for row in measurements),
    }


def judge_evidence(store: Store, label_set_ids: list[str]) -> dict:
    label_sets = [store.get(identity) for identity in label_set_ids]
    config_ids = sorted({row["data"]["config_id"] for row in label_sets})
    configs = {identity: store.get(identity) for identity in config_ids}
    ballots = {}
    raw_responses = {}
    cited_sources = {}
    for label_set in label_sets:
        config = configs[label_set["data"]["config_id"]]["data"]
        ballot_ids = label_set["data"]["ballots"]
        rows = [store.get(identity) for identity in ballot_ids]
        expected_models = {model["identity"] for model in config["models"]}
        if (
            len(rows) != len(expected_models)
            or any(not row["current"] for row in rows)
            or {row["data"]["model"]["identity"] for row in rows} != expected_models
        ):
            raise ValueError("judge evidence does not match its frozen panel")
        label = label_set["data"]["label"]
        if label is not None and any(row["data"]["payload"] != label for row in rows):
            raise ValueError("judge evidence is not unanimous with its frozen label")
        for row in rows:
            ballots[row["id"]] = row
            for citation in row["data"].get("payload", {}).get("citations", []):
                artifact_id = citation["artifact_id"]
                if artifact_id not in cited_sources:
                    source = store.artifact(artifact_id)
                    cited_sources[artifact_id] = {
                        "sha256": hashlib.sha256(source).hexdigest(),
                        "base64": base64.b64encode(source).decode("ascii"),
                    }
            raw_id = row["data"].get("raw_response_artifact")
            if raw_id and raw_id not in raw_responses:
                raw = store.artifact(raw_id)
                raw_responses[raw_id] = {
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "base64": base64.b64encode(raw).decode("ascii"),
                }
    dependencies = store._rows(
        "SELECT child,parent FROM dependencies WHERE child IN (SELECT id FROM nodes WHERE kind IN ('judge_ballot','consensus_label_set')) ORDER BY child,parent"
    )
    return {
        "configs": configs,
        "label_sets": label_sets,
        "ballots": [ballots[identity] for identity in sorted(ballots)],
        "raw_responses": raw_responses,
        "cited_sources": cited_sources,
        "dependencies": [
            row
            for row in dependencies
            if row["child"] in ballots or any(label["id"] == row["child"] for label in label_sets)
        ],
    }


def freeze_corpus(
    store: Store,
    evaluator_paths: list[Path],
    *,
    generator=None,
    manifests: list[dict] | None = None,
) -> dict:
    scans_complete(store)
    contracts = inventory(store)
    sampled, sampling = stratified_sample(contracts)
    if not sampled:
        raise ValueError("corpus freeze requires captured contracts")
    manifests = manifests or [model_manifest(path) for path in evaluator_paths]
    config_id = persist_config(store, "evaluator-panel", panel_config("evaluator", manifests))
    comparisons = comparison_universe(store, sampled)
    gold = []
    stage_labels = []
    contract_abstentions = []
    pair_abstentions = []
    pair_labels = []
    pair_outcomes = []
    disagreements = []
    near_match_rejections = []
    evaluator_measurements = []
    evaluator_label_set_ids = []
    for row in sampled:
        contract = store.get(row["id"])
        texts = store.source_texts(row["id"])
        result = collect_unanimous(
            store,
            kind="contract",
            subject=row["id"],
            logical=f"evaluator:contract:{row['id']}",
            config_id=config_id,
            paths=evaluator_paths,
            prompt=contract_prompt(contract["data"], texts),
            schema=ContractBallot.model_json_schema(),
            generator=generator,
            manifests=manifests,
            artifacts=texts,
        )
        evaluator_measurements.extend(result["measurements"])
        evaluator_label_set_ids.append(result["id"])
        if result["label"] is None:
            contract_abstentions.append({"id": row["id"], "reason": result["abstention"]})
            if result["disagreement"]:
                disagreements.append({"id": row["id"], "kind": "contract"})
            continue
        label = result["label"]
        stage_labels.extend(contract_stages(row["id"], label, "expected"))
        row["eligible"] = bool(label.get("eligible"))
    for pair in comparisons:
        left = store.get(pair["a"])
        right = store.get(pair["b"])
        left_texts = store.source_texts(pair["a"])
        right_texts = store.source_texts(pair["b"])
        texts = left_texts | right_texts
        result = collect_unanimous(
            store,
            kind="pair",
            subject=fingerprint(pair),
            logical=f"evaluator:pair:{pair['a']}:{pair['b']}",
            config_id=config_id,
            paths=evaluator_paths,
            prompt=pair_prompt(left["data"], right["data"], texts),
            schema=PairBallot.model_json_schema(),
            generator=generator,
            manifests=manifests,
            artifacts=texts,
            artifact_groups=[set(left_texts), set(right_texts)],
            dependencies=[pair["a"], pair["b"]],
            expected_pair=pair,
        )
        evaluator_measurements.extend(result["measurements"])
        evaluator_label_set_ids.append(result["id"])
        if result["label"] is None:
            pair_abstentions.append(
                {"id": f"{pair['a']}/{pair['b']}", "reason": result["abstention"]}
            )
            if result["disagreement"]:
                disagreements.append({"id": f"{pair['a']}/{pair['b']}", "kind": "pair"})
            pair_outcomes.append({**pair, "state": "abstained", "reason": result["abstention"]})
            continue
        label = result["label"]
        if label["scope"] != pair["scope"] or sorted(label["conditions"]) != sorted(
            pair["conditions"]
        ):
            pair_abstentions.append(
                {"id": f"{pair['a']}/{pair['b']}", "reason": "pair identity mismatch"}
            )
            pair_outcomes.append({**pair, "state": "abstained", "reason": "pair identity mismatch"})
            continue
        pair_outcomes.append({**pair, "state": "complete"})
        pair_labels.append({**pair, "relationship": label["relationship"]})
        stage_labels.append(pair_stage(pair, label, "expected"))
        if label["relationship"] == "NEAR_MATCH":
            near_match_rejections.append(
                {
                    "a": pair["a"],
                    "b": pair["b"],
                    "scope": pair["scope"],
                    "conditions": pair["conditions"],
                    "differences": label.get("near_match_differences", []),
                }
            )
        claim = gold_claim_from_pair(pair["a"], pair["b"], label, pair=pair)
        if claim:
            gold.append(claim)
    venues = {c["venue"] for c in sampled}
    corpus = {
        "benchmark_id": "local-unanimous-consensus",
        "revision": "1",
        "label_version": PROTOCOL_LABEL,
        "labeling_guide": "exact-string-set/1",
        "split": "held-out-consensus",
        "independent_human_labels": False,
        "label_source": "local_unanimous_consensus",
        "metric_definition_version": "oddsfox-metrics/4",
        "contracts": [{k: c[k] for k in ("id", "venue", "template", "eligible")} for c in sampled],
        "comparisons": comparisons,
        "pair_labels": pair_labels,
        "pair_outcomes": pair_outcomes,
        "gold_claims": gold,
        "stage_labels": stage_labels,
        "sampling": sampling,
        "venues_represented": sorted(venues),
        "label_abstentions": contract_abstentions + pair_abstentions,
        "disagreements": disagreements,
        "near_match_rejections": near_match_rejections,
        "evaluator_panel": store.get(config_id)["data"],
        "evaluator_panel_id": config_id,
        "evaluator_label_set_ids": evaluator_label_set_ids,
        "diagnostics": diagnostics(evaluator_measurements),
    }
    return corpus


PROTOCOL_LABEL = "local-unanimous-consensus/1"


def producer_run(
    store: Store,
    corpus: dict,
    producer_paths: list[Path],
    *,
    generator=None,
    manifests: list[dict] | None = None,
) -> dict:
    manifests = manifests or [model_manifest(path) for path in producer_paths]
    evaluator_panel = corpus["evaluator_panel"]
    disjoint_panels(manifests, evaluator_panel["models"])
    scans_complete(store)
    config_id = persist_config(store, "producer-panel", panel_config("producer", manifests))
    proposals = []
    outcomes = []
    stage_outputs = []
    pair_outcomes = []
    producer_measurements = []
    producer_label_set_ids = []
    for contract in corpus["contracts"]:
        record = store.get(contract["id"])
        texts = store.source_texts(contract["id"])
        result = collect_unanimous(
            store,
            kind="contract",
            subject=contract["id"],
            logical=f"producer:contract:{contract['id']}",
            config_id=config_id,
            paths=producer_paths,
            prompt=contract_prompt(record["data"], texts),
            schema=ContractBallot.model_json_schema(),
            generator=generator,
            manifests=manifests,
            artifacts=texts,
        )
        producer_measurements.extend(result["measurements"])
        producer_label_set_ids.append(result["id"])
        if result["label"] is None:
            outcomes.append(
                {
                    "id": contract["id"],
                    "stage": "interpret",
                    "state": "abstained",
                    "reason": result["abstention"],
                }
            )
            continue
        outcomes.append({"id": contract["id"], "stage": "interpret", "state": "complete"})
        stage_outputs.extend(contract_stages(contract["id"], result["label"], "values"))
    for pair in corpus["comparisons"]:
        left = store.get(pair["a"])
        right = store.get(pair["b"])
        left_texts = store.source_texts(pair["a"])
        right_texts = store.source_texts(pair["b"])
        texts = left_texts | right_texts
        result = collect_unanimous(
            store,
            kind="pair",
            subject=fingerprint(pair),
            logical=f"producer:pair:{pair['a']}:{pair['b']}",
            config_id=config_id,
            paths=producer_paths,
            prompt=pair_prompt(left["data"], right["data"], texts),
            schema=PairBallot.model_json_schema(),
            generator=generator,
            manifests=manifests,
            artifacts=texts,
            artifact_groups=[set(left_texts), set(right_texts)],
            dependencies=[pair["a"], pair["b"]],
            expected_pair=pair,
        )
        producer_measurements.extend(result["measurements"])
        producer_label_set_ids.append(result["id"])
        if result["label"] is None:
            pair_outcomes.append({**pair, "state": "abstained", "reason": result["abstention"]})
            continue
        if result["label"]["scope"] != pair["scope"] or sorted(
            result["label"]["conditions"]
        ) != sorted(pair["conditions"]):
            pair_outcomes.append({**pair, "state": "abstained", "reason": "pair identity mismatch"})
            continue
        pair_outcomes.append({**pair, "state": "complete"})
        stage_outputs.append(pair_stage(pair, result["label"], "values"))
        claim = gold_claim_from_pair(pair["a"], pair["b"], result["label"], pair=pair)
        if claim:
            proposals.append(claim)
    venues = {c["id"]: c["venue"] for c in corpus["contracts"]}
    return {
        "benchmark_hash": fingerprint(corpus),
        "scored_before_case_review": True,
        "pipeline_configuration": {
            "compiler": "consensus-producer",
            "protocol": PROTOCOL_LABEL,
            "producer_panel": config_id,
            "producer_panel_manifest": store.get(config_id)["data"],
            "schema": "1.0.0",
        },
        "acceptance_policy": {
            "version": CONSENSUS_AGREEMENT,
            "allowed_scopes": ["OBSERVED_EVENT"],
            "allowed_settlement_states": ["CONDITIONAL", "COMPATIBLE"],
            "require_resolved": False,
            "normalization": "exact-string-set/1",
        },
        "proposals": proposals,
        "stage_outputs": stage_outputs,
        "outcomes": outcomes,
        "pair_outcomes": pair_outcomes,
        "diagnostics": diagnostics(producer_measurements),
        "producer_label_set_ids": producer_label_set_ids,
        "complete_zero_error_scans": True,
        "unanimous_cross_venue_relationship": any(
            venues.get(c["a"]) != venues.get(c["b"]) for c in corpus["gold_claims"]
        ),
        "unanimous_near_match_rejection": bool(corpus.get("near_match_rejections")),
        "zero_accepted_known_false_equivalences": False,
        "provenance_invalidation": False,
    }


def validate_dataset(
    store: Store,
    output: Path,
    producer_paths: list[Path],
    evaluator_paths: list[Path],
    *,
    generator=None,
    producer_manifests=None,
    evaluator_manifests=None,
    skip_sync: bool = True,
) -> dict:
    started = time.monotonic()
    if not skip_sync:
        from oddsfox.sync import SyncRunner

        runner = SyncRunner(store)
        try:
            for venue in VENUES:
                runner.run_venue(venue, runner.request(venue))
        finally:
            runner.close()
    producer_manifests = producer_manifests or [model_manifest(p) for p in producer_paths]
    evaluator_manifests = evaluator_manifests or [model_manifest(p) for p in evaluator_paths]
    disjoint_panels(producer_manifests, evaluator_manifests)
    corpus = freeze_corpus(
        store, evaluator_paths, generator=generator, manifests=evaluator_manifests
    )
    run = producer_run(
        store, corpus, producer_paths, generator=generator, manifests=producer_manifests
    )
    evidence = judge_evidence(
        store, corpus["evaluator_label_set_ids"] + run["producer_label_set_ids"]
    )
    run["evidence_mode"] = "immutable-local-store"
    run["judge_evidence_hash"] = fingerprint(evidence)
    run["judge_evidence"] = evidence
    metrics = evaluate(corpus, run, _trusted_evidence=True)
    metrics["operational"] = {
        "validation_wall_seconds": time.monotonic() - started,
        "evaluator": corpus["diagnostics"],
        "producer": run["diagnostics"],
    }
    bundle = {
        "corpus.json": corpus,
        "benchmark.json": corpus,
        "run.json": run,
        "metrics.json": metrics,
        "judge-evidence.json": evidence,
        "disagreements.json": {
            "disagreements": corpus["disagreements"],
            "label_abstentions": corpus["label_abstentions"],
        },
    }
    write_bundle(output, bundle)
    return metrics
