"""Producer-panel consensus approvals. Never written into human review records."""

from pathlib import Path

from oddsfox.ir import SemanticIR, fingerprint
from oddsfox.judge import (
    PROTOCOL,
    canonicalize_contract,
    collect_unanimous,
    disjoint_panels,
    panel_config,
    persist_config,
)
from oddsfox.models import model_manifest
from oddsfox.reasoning import eligible
from oddsfox.store import Store

HUMAN_REVIEW = "HUMAN_REVIEW"
LOCAL_MODEL_CONSENSUS = "LOCAL_MODEL_CONSENSUS"


def load_manifests(paths: list[Path]) -> list[dict]:
    return [model_manifest(path) for path in paths]


def configure_panels(
    store: Store, producer_paths: list[Path], evaluator_paths: list[Path]
) -> tuple[str, str]:
    producer = load_manifests(producer_paths)
    evaluator = load_manifests(evaluator_paths)
    disjoint_panels(producer, evaluator)
    producer_id = persist_config(store, "producer-panel", panel_config("producer", producer))
    evaluator_id = persist_config(store, "evaluator-panel", panel_config("evaluator", evaluator))
    return producer_id, evaluator_id


def human_review(store: Store, interpretation: dict) -> dict | None:
    review = store.current("review", interpretation["id"])
    if not review:
        return None
    if review["data"].get("ir_digest") != interpretation["data"]["digest"]:
        return None
    return review


def human_rejected(store: Store, interpretation: dict) -> bool:
    review = human_review(store, interpretation)
    return bool(review and not review["data"]["approved"])


def human_approved(store: Store, interpretation: dict) -> bool:
    review = human_review(store, interpretation)
    return bool(review and review["data"]["approved"])


def consensus_record(store: Store, interpretation: dict) -> dict | None:
    approval = store.current("consensus_approval", interpretation["id"])
    if not approval:
        return None
    if approval["data"].get("ir_digest") != interpretation["data"]["digest"]:
        return None
    return approval


def accepted(store: Store, interpretation: dict, *, allow_consensus: bool) -> bool:
    if human_rejected(store, interpretation):
        return False
    if human_approved(store, interpretation):
        return True
    return allow_consensus and consensus_record(store, interpretation) is not None


def acceptance_basis(store: Store, interpretation: dict, *, allow_consensus: bool) -> str | None:
    if not accepted(store, interpretation, allow_consensus=allow_consensus):
        return None
    if human_approved(store, interpretation):
        return HUMAN_REVIEW
    return LOCAL_MODEL_CONSENSUS


def approve_interpretation(
    store: Store,
    interpretation_id: str,
    producer_paths: list[Path],
    *,
    generator=None,
    manifests: list[dict] | None = None,
) -> str:
    interpretation = store.get(interpretation_id)
    if interpretation["kind"] != "interpretation":
        raise ValueError("consensus target must be an interpretation")
    ir = SemanticIR.model_validate(interpretation["data"]["ir"])
    if not eligible(ir):
        raise ValueError("consensus cannot broaden the formal family")
    if human_rejected(store, interpretation):
        raise ValueError("current human rejection vetoes model consensus")
    contract = store.get(ir.contract_version_id)
    if contract["data"]["metadata"]["capture_status"] == "missing_rules" or any(
        r["status"] != "captured" for r in contract["data"]["references"]
    ):
        raise ValueError("uncaptured governing material prevents consensus approval")
    ir.validate_evidence(
        store.source_texts(contract["id"]),
        contract["data"]["metadata"]["outcome_ids"],
        interpretation["data"]["derivations"],
    )
    manifests = manifests or load_manifests(producer_paths)
    config_id = persist_config(store, "producer-panel", panel_config("producer", manifests))
    expected = canonicalize_contract_from_ir(ir)
    texts = store.source_texts(ir.contract_version_id)
    from oddsfox.judge import ContractBallot, contract_prompt

    result = collect_unanimous(
        store,
        kind="contract",
        subject=interpretation_id,
        logical=f"producer:{interpretation_id}:{interpretation['data']['digest']}",
        config_id=config_id,
        paths=producer_paths,
        prompt=contract_prompt(contract["data"], texts),
        schema=ContractBallot.model_json_schema(),
        generator=generator,
        manifests=manifests,
        artifacts=texts,
    )
    if result["label"] is None:
        raise ValueError(result["abstention"] or "producer panel abstained")
    if fingerprint(ballot_semantics(result["label"])) != fingerprint(ballot_semantics(expected)):
        raise ValueError("producer ballots are not unanimous with the compiled IR")
    data = {
        "protocol": PROTOCOL,
        "ir_digest": interpretation["data"]["digest"],
        "panel": config_id,
        "ballots": result["ballots"],
        "acceptance_basis": LOCAL_MODEL_CONSENSUS,
    }
    sources = [ir.contract_version_id, interpretation_id, config_id, *result["ballots"]]
    if result.get("id"):
        sources.append(result["id"])
    with store.transaction():
        return store.insert(
            "consensus_approval",
            interpretation_id,
            data,
            sources,
            "ACCEPTED",
        )


def canonicalize_contract_from_ir(ir: SemanticIR) -> dict:
    observation = ir.observation.model_dump()
    predicate = ir.predicate.model_dump()
    settlement = ir.settlement_semantics.model_dump()
    from oddsfox.judge import ContractBallot

    ballot = ContractBallot.model_validate(
        {
            "eligible": True,
            "family": "instantaneous_threshold",
            "quantity": observation.get("quantity"),
            "source": observation.get("source"),
            "instrument_or_series": observation.get("instrument_or_series"),
            "unit": observation.get("unit"),
            "timestamp": observation.get("timestamp"),
            "timezone": observation.get("timezone"),
            "measurement_method": observation.get("measurement_method"),
            "precision": observation.get("precision"),
            "revision_policy": observation.get("revision_policy"),
            "canonical_observation_id": observation.get("canonical_observation_id"),
            "canonical_observation_version": observation.get("canonical_observation_version"),
            "comparator": predicate.get("comparator"),
            "threshold": predicate.get("threshold"),
            "resolution_source": settlement.get("resolution_source"),
            "cutoff": settlement.get("cutoff"),
            "rounding": settlement.get("rounding"),
            "missing_data_policy": settlement.get("missing_data_policy"),
            "cancellation_policy": settlement.get("cancellation_policy"),
            "exceptional_outcome_policy": settlement.get("exceptional_outcome_policy"),
            "dispute_policy": settlement.get("dispute_policy"),
            "clarification_policy": settlement.get("clarification_policy"),
            "unknowns": [],
            "citations": [],
        }
    )
    return canonicalize_contract(ballot)


def ballot_semantics(payload: dict) -> dict:
    return {key: value for key, value in payload.items() if key != "citations"}
