"""Explicitly synthetic examples, never a substitute for human release labels."""

import json

from oddsfox.compiler import blank_ir
from oddsfox.ingest import import_capture
from oddsfox.ir import SemanticIR, leaves
from oddsfox.pipeline import Pipeline
from oddsfox.store import Store


def sample(
    store: Store,
    native_id: str,
    threshold: str = "100000",
    platform: str = "polymarket",
    comparator: str = "GTE",
    source: str = "Example reference feed",
    timestamp: str = "2027-01-01T00:00:00Z",
) -> dict:
    ir = blank_ir("pending")
    ir["observation"].update(
        {
            "quantity": "BTC price",
            "source": source,
            "instrument_or_series": "BTC/USD",
            "unit": "USD",
            "timestamp": timestamp,
            "timezone": "UTC",
            "measurement_method": "instantaneous",
            "precision": "0.01",
            "revision_policy": "first published value",
        }
    )
    ir["predicate"].update(comparator=comparator, threshold=threshold)
    ir["settlement_semantics"].update(
        {
            "payout_mapping": {
                "unit": "USD",
                "outcomes": [
                    {"outcome_id": "no", "if_true": "0", "if_false": "1"},
                    {"outcome_id": "yes", "if_true": "1", "if_false": "0"},
                ],
            },
            "resolution_source": source,
            "cutoff": "2027-01-02T00:00:00Z",
            "rounding": "none",
            "missing_data_policy": "cancel if absent",
            "cancellation_policy": "return stake",
            "exceptional_outcome_policy": "cancel",
            "dispute_policy": "manual adjudication",
            "clarification_policy": "recompile before settlement",
        }
    )
    semantic = {k: ir[k] for k in ("observation", "predicate", "settlement_semantics")}
    text = "SYNTHETIC OddsFox demonstration only; not a listed market.\n"
    positions = {}
    for pointer, value in leaves(semantic):
        if value is None or pointer == "/predicate/observation_ref":
            continue
        line = f"{pointer}: {value}\n"
        positions[pointer] = (len(text), len(text) + len(line) - 1)
        text += line
    title = f"[Synthetic] BTC {comparator} {threshold} USD at 2027-01-01 UTC"
    payload = (
        {"id": native_id, "question": title, "description": text, "outcomes": '["no","yes"]'}
        if platform == "polymarket"
        else {
            "market": {
                "ticker": native_id,
                "title": title,
                "market_type": "binary",
                "rules_primary": text,
            }
        }
    )
    contract_id = import_capture(store, platform, json.dumps(payload).encode())
    key = "description" if platform == "polymarket" else "rules_primary"
    artifact = store.get(contract_id)["data"]["text_artifacts"][key]
    ir["contract_version_id"] = contract_id
    ir["field_evidence"] = {
        p: {
            "source_spans": [{"artifact_id": artifact, "start": start, "end": end}],
            "derivation_ref": None,
        }
        for p, (start, end) in positions.items()
    }
    return ir


def load_demo(store: Store, approve: bool = False) -> dict:
    if store.list("contract"):
        raise ValueError("use a new empty dataset for the synthetic demo")
    pipeline = Pipeline(store)
    a = sample(store, "example-high", "150000")
    b = sample(store, "EXAMPLE-LOW", "100000", "kalshi")
    near = sample(store, "example-other-feed", "100000", source="Different example feed")
    pipeline.register(
        "example-btc-2027",
        a["observation"],
        "synthetic fixture",
        "Illustrative exact observation; no real contract or human label",
    )
    pipeline.register(
        "different-feed-2027",
        near["observation"],
        "synthetic fixture",
        "Different source deliberately remains a separate observation",
    )
    identities = [
        pipeline.interpret(SemanticIR.model_validate(ir).canonical()) for ir in (a, b, near)
    ]
    if approve:
        for identity in identities:
            pipeline.review(
                identity,
                "synthetic fixture",
                "Explicit synthetic-demo approval; not independent human evidence",
                True,
                True,
            )
        pipeline.publish()
    return {
        "synthetic": True,
        "interpretations": identities,
        "accepted": len(store.list("assertion")),
        "release_quality_established": False,
    }
