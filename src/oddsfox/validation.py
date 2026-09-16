"""Complete-scan freeze, stratified sampling, and write-once consensus evidence bundles."""

import hashlib
import json
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
    return selected, {
        "algorithm": SAMPLE_ALGORITHM,
        "limit": limit,
        "inventory": len(unique),
        "sampled": len(selected),
        "strata": {f"{a}:{b}:{c}": 1 for a, b, c in sorted(strata)},
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


def write_bundle(directory: Path, files: dict[str, dict]) -> dict:
    directory.mkdir(parents=True, exist_ok=True)
    hashes = {}
    for name, payload in files.items():
        path = directory / name
        raw = json.dumps(payload, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
        with path.open("x", encoding="utf-8") as handle:
            handle.write(raw)
        hashes[name] = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    manifest = {
        "files": hashes,
        "bundle": fingerprint(files),
    }
    with (directory / "manifest.json").open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
    return manifest


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
    disagreements = []
    near_match_rejections = []
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
        )
        if result["label"] is None:
            contract_abstentions.append({"id": row["id"], "reason": result["abstention"]})
            if result["disagreement"]:
                disagreements.append({"id": row["id"], "kind": "contract"})
            continue
        label = result["label"]
        stage_labels.append(
            {
                "id": row["id"],
                "stage": "ir_fields",
                "mode": "pipeline",
                "expected": {
                    "source": label.get("source"),
                    "threshold": label.get("threshold"),
                    "measurement_method": label.get("measurement_method"),
                },
            }
        )
        row["eligible"] = bool(label.get("eligible"))
    for pair in comparisons:
        left = store.get(pair["a"])
        right = store.get(pair["b"])
        texts = store.source_texts(pair["a"]) | store.source_texts(pair["b"])
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
        )
        if result["label"] is None:
            pair_abstentions.append(
                {"id": f"{pair['a']}/{pair['b']}", "reason": result["abstention"]}
            )
            if result["disagreement"]:
                disagreements.append({"id": f"{pair['a']}/{pair['b']}", "kind": "pair"})
            continue
        if result["label"]["relationship"] == "NEAR_MATCH":
            near_match_rejections.append(
                {
                    "a": pair["a"],
                    "b": pair["b"],
                    "differences": result["label"].get("near_match_differences", []),
                }
            )
        claim = gold_claim_from_pair(pair["a"], pair["b"], result["label"], pair=pair)
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
        "gold_claims": gold,
        "stage_labels": stage_labels,
        "sampling": sampling,
        "venues_represented": sorted(venues),
        "label_abstentions": contract_abstentions + pair_abstentions,
        "disagreements": disagreements,
        "near_match_rejections": near_match_rejections,
        "evaluator_panel": config_id,
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
    disjoint_panels(manifests, store.get(corpus["evaluator_panel"])["data"]["models"])
    scans_complete(store)
    config_id = persist_config(store, "producer-panel", panel_config("producer", manifests))
    proposals = []
    outcomes = []
    stage_outputs = []
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
        )
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
        stage_outputs.append(
            {
                "id": contract["id"],
                "stage": "ir_fields",
                "mode": "pipeline",
                "values": {
                    "source": result["label"].get("source"),
                    "threshold": result["label"].get("threshold"),
                    "measurement_method": result["label"].get("measurement_method"),
                },
            }
        )
    for pair in corpus["comparisons"]:
        left = store.get(pair["a"])
        right = store.get(pair["b"])
        texts = store.source_texts(pair["a"]) | store.source_texts(pair["b"])
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
        )
        if result["label"] is None:
            continue
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
    metrics = evaluate(corpus, run)
    bundle = {
        "corpus.json": corpus,
        "benchmark.json": corpus,
        "run.json": run,
        "metrics.json": metrics,
        "disagreements.json": {
            "disagreements": corpus["disagreements"],
            "label_abstentions": corpus["label_abstentions"],
        },
    }
    write_bundle(output, bundle)
    return metrics
