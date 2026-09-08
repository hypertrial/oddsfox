import copy
import json

import pytest
from pydantic import ValidationError

from oddsfox.demo import sample
from oddsfox.ir import SemanticIR, parse_ir, strict_json


def test_canonical_roundtrip_and_evidence(store):
    raw = sample(store, "one", "100000.00000000000000000000000000000000001")
    ir = SemanticIR.model_validate(raw)
    assert parse_ir(ir.canonical()).canonical() == ir.canonical()
    assert json.loads(ir.canonical())["predicate"]["threshold"].endswith("00001")
    assert ir.observation.canonical_observation_id is None
    ir.validate_evidence(store.source_texts(ir.contract_version_id), ["no", "yes"])
    assert len(ir.digest()) == 64


@pytest.mark.parametrize("value", ["1.0", "1e3", "01", "+1", "-0", "NaN", 1.5, True])
def test_decimal_rejection(store, value):
    raw = sample(store, "one")
    raw["predicate"]["threshold"] = value
    with pytest.raises(ValidationError):
        SemanticIR.model_validate(raw)


@pytest.mark.parametrize(
    "change",
    [
        lambda r: r.update(schema_version="2.0.0"),
        lambda r: r.update(unexpected=True),
        lambda r: r["observation"].pop("source"),
        lambda r: r["observation"].update(precision="0"),
        lambda r: r["observation"].update(timestamp="2027-02-30T00:00:00Z"),
        lambda r: r["observation"].update(timestamp="2027-01-01T00:00:00.10Z"),
        lambda r: r["observation"].update(canonical_observation_id="forged"),
        lambda r: r["settlement_semantics"]["payout_mapping"]["outcomes"].reverse(),
        lambda r: r["settlement_semantics"]["payout_mapping"]["outcomes"][1].update(
            outcome_id="no"
        ),
        lambda r: r["field_evidence"].update(
            {"/compiler_version": {"source_spans": [], "derivation_ref": None}}
        ),
        lambda r: r["field_evidence"].update(
            {"/predicate/missing": {"source_spans": [], "derivation_ref": None}}
        ),
    ],
)
def test_schema_rejects_nonconforming_consumers(store, change):
    raw = sample(store, "one")
    change(raw)
    with pytest.raises(ValidationError):
        SemanticIR.model_validate(raw)


def test_missing_foreign_and_dangling_evidence(store):
    raw = sample(store, "one")
    other = sample(store, "two", "200000")
    text = store.source_texts(raw["contract_version_id"])
    absent = copy.deepcopy(raw)
    absent["field_evidence"].pop("/predicate/threshold")
    with pytest.raises(ValueError, match="missing evidence"):
        SemanticIR.model_validate(absent).validate_evidence(text, ["no", "yes"])
    raw["field_evidence"]["/predicate/threshold"] = other["field_evidence"]["/predicate/threshold"]
    with pytest.raises(ValueError, match="outside"):
        SemanticIR.model_validate(raw).validate_evidence(text, ["no", "yes"])
    raw["field_evidence"]["/predicate/threshold"] = {
        "source_spans": [],
        "derivation_ref": "missing",
    }
    with pytest.raises(ValueError, match="derivation"):
        SemanticIR.model_validate(raw).validate_evidence(text, ["no", "yes"])


def test_derivation_binds_pointer_value_and_source(store):
    raw = sample(store, "one")
    evidence = raw["field_evidence"]["/predicate/threshold"]
    record = {
        "pointer": "/predicate/threshold",
        "value": "100000",
        "source_spans": evidence["source_spans"],
    }
    raw["field_evidence"]["/predicate/threshold"] = {
        "source_spans": [],
        "derivation_ref": "normalization/1",
    }
    ir = SemanticIR.model_validate(raw)
    texts = store.source_texts(ir.contract_version_id)
    ir.validate_evidence(texts, ["no", "yes"], {"normalization/1": record})
    with pytest.raises(ValueError, match="mismatched"):
        ir.validate_evidence(
            texts, ["no", "yes"], {"normalization/1": record | {"pointer": "/predicate/comparator"}}
        )


def test_duplicate_json_keys_and_nonjson_numbers():
    with pytest.raises(ValueError, match="duplicate"):
        strict_json('{"a":1,"a":2}')
    with pytest.raises(ValueError, match="non-JSON"):
        strict_json('{"a":NaN}')


def test_unicode_payout_order_and_span_offsets(store):
    raw = sample(store, "one")
    ids = ["\ue000", "\U00010000"]
    rows = raw["settlement_semantics"]["payout_mapping"]["outcomes"]
    for row, identity in zip(rows, ids, strict=True):
        row["outcome_id"] = identity
    ir = SemanticIR.model_validate(raw)
    assert [
        p.outcome_id for p in parse_ir(ir.canonical()).settlement_semantics.payout_mapping.outcomes
    ] == ids
    rows.reverse()
    with pytest.raises(ValidationError, match="Unicode"):
        SemanticIR.model_validate(raw)
