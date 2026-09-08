import json
from copy import deepcopy

import pytest

from oddsfox.cli import main
from oddsfox.demo import load_demo, sample
from oddsfox.evaluation import evaluate
from oddsfox.ir import SemanticIR, fingerprint
from oddsfox.normalization import scale_decimal
from oddsfox.pipeline import Pipeline
from oddsfox.reasoning import differences
from oddsfox.store import Store


def test_generation_grammar_keeps_structure_without_expanding_large_size_bounds():
    from oddsfox.compiler import generation_schema

    public = SemanticIR.model_json_schema()
    grammar = generation_schema()
    assert grammar["required"] == public["required"]
    assert grammar["additionalProperties"] is False
    field = grammar["$defs"]["Observation"]["properties"]["quantity"]
    assert field["anyOf"][1] == {"type": "null"}
    assert "maxLength" not in field["anyOf"][0]
    assert (
        public["$defs"]["Observation"]["properties"]["quantity"]["anyOf"][0]["maxLength"] == 20000
    )
    assert grammar["$defs"]["Predicate"]["properties"]["threshold"]["anyOf"][0]["pattern"]


def test_prompt_line_offsets_reference_original_unicode_text():
    from oddsfox.compiler import source_lines

    text = "é🦊\r\nsecond\nlast"
    lines = source_lines({"artifact": text})["artifact"]
    assert "".join(line["text"] for line in lines) == text
    for line in lines:
        assert text[line["start"] : line["end"]] == line["text"]


def test_decoder_citations_choose_valid_fields_and_captured_spans():
    from oddsfox.compiler import generation_schema

    schema = generation_schema("contract", {"artifact": "é🦊\nsecond"})
    assert schema["properties"]["contract_version_id"]["const"] == "contract"
    fields = schema["properties"]["field_evidence"]
    assert fields["additionalProperties"] is False
    assert "/predicate/threshold" in fields["properties"]
    assert "/settlement_semantics/resolution_source" in fields["required"]
    assert "/settlement_semantics/payout_mapping/unit" in fields["required"]
    assert all(path.startswith("/") for path in fields["properties"])
    assert schema["$defs"]["SourceSpan"]["enum"] == [
        {"artifact_id": "artifact", "start": 0, "end": 3},
        {"artifact_id": "artifact", "start": 3, "end": 9},
    ]
    with pytest.raises(ValueError, match="1..256"):
        generation_schema("contract", {"artifact": "x\n" * 257})


def test_null_payout_omits_only_empty_decoder_evidence_slots():
    from oddsfox.compiler import blank_ir, public_model_response

    raw = blank_ir("contract")
    pointer = "/settlement_semantics/payout_mapping/unit"
    raw["field_evidence"][pointer] = {"source_spans": [], "derivation_ref": None}
    ir = SemanticIR.model_validate_json(public_model_response(json.dumps(raw)))
    ir.validate_evidence({}, [])
    assert pointer not in ir.field_evidence
    raw["field_evidence"][pointer]["source_spans"] = [{"artifact_id": "a", "start": 0, "end": 1}]
    with pytest.raises(ValueError, match="dangling evidence"):
        SemanticIR.model_validate_json(public_model_response(json.dumps(raw)))
    with pytest.raises(ValueError):
        public_model_response('{"field_evidence": {}, "field_evidence": {}}')
    raw["field_evidence"] = {
        "/settlement_semantics/payout_mapping/invented": {
            "source_spans": [],
            "derivation_ref": None,
        }
    }
    with pytest.raises(ValueError, match="dangling evidence"):
        SemanticIR.model_validate_json(public_model_response(json.dumps(raw)))


def test_model_response_replay_uses_recorded_normalization_and_preserves_raw(store):
    from oddsfox.compiler import blank_ir, replay

    contract = sample(store, "replay")["contract_version_id"]
    raw = blank_ir(contract)
    pointer = "/settlement_semantics/payout_mapping/unit"
    raw["field_evidence"][pointer] = {"source_spans": [], "derivation_ref": None}
    original = json.dumps(raw).encode()
    artifact = store.put_artifact(original)
    pipeline = Pipeline(store)
    config = pipeline.configure({"decoding": {"empty_payout_evidence": "omit-if-parent-null/1"}})
    identity = replay(store, contract, artifact, config)
    assert store.get(identity)["data"]["ir"]["field_evidence"] == {}
    assert store.get(identity)["data"]["raw_response_artifact"] == artifact
    assert store.get(identity)["data"]["validated_response_artifact"] != artifact
    assert store.artifact(artifact) == original
    assert store.list("assertion") == []
    with pytest.raises(ValueError, match="different contract"):
        replay(store, "other-contract", artifact, config)
    config = pipeline.configure({"decoding": {"empty_payout_evidence": "unknown/2"}})
    with pytest.raises(ValueError, match="unsupported model-response"):
        replay(store, contract, artifact, config)


def test_successful_job_links_original_and_processed_responses(store):
    from oddsfox.compiler import blank_ir, public_model_response

    contract = sample(store, "job-raw")["contract_version_id"]
    raw = blank_ir(contract)
    raw["field_evidence"]["/settlement_semantics/payout_mapping/unit"] = {
        "source_spans": [],
        "derivation_ref": None,
    }
    original = store.put_artifact(json.dumps(raw).encode())
    pipeline = Pipeline(store)
    config = pipeline.configure()
    job = store.enqueue("interpret", [contract, config], {})
    store.claim_job(job)
    identity = pipeline.interpret(
        public_model_response(store.artifact(original)),
        config,
        job,
        original_response_artifact=original,
    )
    data = store.get(identity)["data"]
    assert data["raw_response_artifact"] == original
    assert data["validated_response_artifact"] != original
    assert store.status()["attempts"][0]["artifact"] == original


@pytest.mark.parametrize("version", ["prompt", "grammar", "empty_payout_evidence"])
def test_incompatible_persisted_compiler_settings_fail_before_loading_model(
    store, tmp_path, version
):
    from oddsfox.compiler import GRAMMAR_VERSION, PROMPT_VERSION, run_compile_job

    contract = sample(store, "old-job")["contract_version_id"]
    settings = {
        "prompt": PROMPT_VERSION,
        "decoding": {
            "constrained": True,
            "grammar": GRAMMAR_VERSION,
            "empty_payout_evidence": "omit-if-parent-null/1",
        },
    }
    if version == "prompt":
        settings[version] = "old/0"
    else:
        settings["decoding"][version] = "old/0"
    config = Pipeline(store).configure(settings)
    job = store.enqueue("interpret", [contract, config], settings)
    for _ in range(3):
        with pytest.raises(ValueError, match="unsupported persisted compiler"):
            run_compile_job(store, job, tmp_path / "missing-model")
    assert store._rows("SELECT state FROM jobs WHERE id=?", [job])[0]["state"] == "failed"


def test_backup_rejects_missing_quarantined_proof(store, tmp_path):
    proof = store.put_artifact(b"quarantined solver evidence")
    with store.transaction():
        store.insert(
            "quarantine", "failed-set", {"proof_artifacts": {"claim": proof}}, [], "QUARANTINED"
        )
    (store.artifacts / proof).unlink()
    destination = tmp_path / "backup"
    with pytest.raises(ValueError, match="missing referenced artifact"):
        store.backup(destination)
    assert not destination.exists()


@pytest.mark.parametrize("populated", [False, True])
def test_parquet_manifest_survives_empty_exports(store, tmp_path, populated):
    import duckdb

    if populated:
        load_demo(store, True)
    pipeline = Pipeline(store)
    output = tmp_path / "export.parquet"
    pipeline.export_parquet(output)
    connection = duckdb.connect()
    try:
        entries = connection.execute(
            "SELECT key,value FROM parquet_kv_metadata(?)", [str(output)]
        ).fetchall()
        metadata = json.loads(dict(entries)[b"oddsfox"])
        assert metadata["export_schema_version"] == "1.0.0"
        assert metadata["ir_schema_version"] == "1.0.0"
        expected = pipeline.export()
        for key in ("freshness", "contracts", "interpretations", "reviews", "registry"):
            assert metadata[key] == expected[key]
        assert metadata["manifest"]["configuration"] == expected["manifest"]["configuration"]
        assert "assertions" not in metadata
        assert connection.execute("SELECT count(*) FROM read_parquet(?)", [str(output)]).fetchone()[
            0
        ] == (2 if populated else 0)
    finally:
        connection.close()


def test_job_cannot_complete_with_another_contract(store):
    a, b = sample(store, "a"), sample(store, "b")
    pipeline = Pipeline(store)
    config = pipeline.configure()
    job = store.enqueue("interpret", [a["contract_version_id"], config], {})
    store.claim_job(job)
    with pytest.raises(ValueError, match="job's contract"):
        pipeline.interpret(json.dumps(b), config, job)
    assert store.list("interpretation", True) == []
    assert store._rows("SELECT state FROM jobs WHERE id=?", [job])[0]["state"] == "running"


def test_config_rollback_is_new_revision_and_invalid_manual_input_does_not_invalidate(store):
    pipeline = Pipeline(store)
    original = pipeline.configure()
    next_ = pipeline.configure({"prompt": "second/2"})
    with pytest.raises(ValueError):
        pipeline.interpret("{}")
    assert store.current("configuration", "active")["id"] == next_
    assert pipeline.configure() == next_
    reverted = pipeline.configure({"prompt": "evidence-json/1"})
    assert reverted != original
    assert not store.get(original)["current"]


def test_mixed_polarity_does_not_hide_implication_and_near_matches_explained(store):
    pipeline = Pipeline(store)
    raws = [
        sample(store, "a", "1"),
        sample(store, "middle", "2", comparator="LT"),
        sample(store, "b", "3"),
    ]
    pipeline.register("btc", raws[0]["observation"], "test", "reviewed definition")
    for raw in raws:
        pipeline.interpret(json.dumps(raw))
    assert any(
        c["a"] == raws[2]["contract_version_id"]
        and c["b"] == raws[0]["contract_version_id"]
        and c["relation"] == "IMPLIES"
        for c in pipeline.compare()
    )
    other = sample(store, "near", "1", source="Different source")
    irs = [
        pipeline.resolve(SemanticIR.model_validate(raws[0]))[0],
        SemanticIR.model_validate(other),
    ]
    result = differences(irs)
    assert len(result) == 1 and result[0]["state"] == "NOT_COMPARABLE"
    assert result[0]["differences"]["source"]["b"] == "Different source"


def test_restore_rejects_missing_referenced_evidence_and_leaves_no_destination(store, tmp_path):
    raw = sample(store, "one")
    backup = tmp_path / "backup"
    store.backup(backup)
    artifact = next(iter(store.source_texts(raw["contract_version_id"])))
    (backup / "artifacts" / artifact).unlink()
    output = tmp_path / "restored"
    assert main(["--data", str(output), "restore", str(backup)]) == 1
    assert not output.exists()
    assert main(["--data", str(output), "restore", str(tmp_path / "absent")]) == 1
    assert not (tmp_path / "absent").exists()


def test_third_interruption_is_failed_with_attempt_evidence(tmp_path):
    store = Store(tmp_path / "jobs")
    job = store.enqueue("test", [], {})
    for _ in range(2):
        store.claim_job(job)
        store.fail_job(job, "failure")
    store.claim_job(job)
    store.close()
    store = Store(tmp_path / "jobs")
    try:
        record = store._rows("SELECT state,attempts FROM jobs WHERE id=?", [job])[0]
        assert record == {"state": "failed", "attempts": 3}
        assert store.status()["attempts"][0]["state"] == "interrupted"
    finally:
        store.close()


def test_exact_unit_conversion_and_reviewed_alias_provenance(store):
    assert (
        scale_decimal("123456789012345678901234567890.000000001", "1000")
        == "123456789012345678901234567890000.000001"
    )
    with pytest.raises(ValueError):
        scale_decimal("1", "-1")
    raw = sample(store, "one", "100")
    target = deepcopy(raw["observation"])
    target.update(unit="millidollars", precision="10")
    pipeline = Pipeline(store)
    registry = pipeline.register(
        "scaled",
        target,
        "test",
        "explicit positive exact unit conversion",
        [{"definition": raw["observation"], "unit_factor": "1000"}],
    )
    identity = pipeline.interpret(json.dumps(raw))
    record = store.get(identity)
    assert record["data"]["ir"]["predicate"]["threshold"] == "100000"
    assert record["data"]["ir"]["observation"]["unit"] == "millidollars"
    assert record["data"]["ir"]["observation"]["canonical_observation_version"] == registry
    assert record["data"]["derivations"]
    pipeline.review(identity, "test", "checked exact source and conversion rule", True, True)


def test_unknown_referenced_material_blocks_semantic_approval(store):
    result = load_demo(store)
    pipeline = Pipeline(store)
    identity = result["interpretations"][0]
    with pytest.raises(ValueError, match="attestation"):
        pipeline.review(identity, "test", "no attestation", True, False)


def test_public_schema_exposes_lexical_restrictions():
    schema = SemanticIR.model_json_schema()
    exact = schema["$defs"]["Predicate"]["properties"]["threshold"]["anyOf"][0]
    instant = schema["$defs"]["Observation"]["properties"]["timestamp"]["anyOf"][0]
    assert "pattern" in exact and exact["maxLength"] == 256
    assert "pattern" in instant


def test_exact_registry_match_precedes_alias(store):
    pipeline = Pipeline(store)
    raw = sample(store, "one", "100")
    direct = pipeline.register("direct", raw["observation"], "test", "direct observation")
    target = deepcopy(raw["observation"])
    target.update(unit="scaled", precision="10")
    pipeline.register(
        "alias",
        target,
        "test",
        "scaled alias",
        [{"definition": raw["observation"], "unit_factor": "1000"}],
    )
    identity = pipeline.interpret(json.dumps(raw))
    assert (
        store.get(identity)["data"]["ir"]["observation"]["canonical_observation_version"] == direct
    )


def test_revision_race_before_feasibility_is_stale_input(store, monkeypatch):
    load_demo(store, True)
    pipeline = Pipeline(store)
    original = pipeline.compare

    def race():
        proposals = original()
        sample(store, "example-high", "160000")
        return proposals

    monkeypatch.setattr(pipeline, "compare", race)
    from oddsfox.store import StaleInput

    with pytest.raises(StaleInput):
        pipeline.publish()
    assert pipeline.export()["assertions"] == []


def test_equal_threshold_equivalents_remain_adjacent(store):
    from oddsfox.reasoning import candidates

    pipeline = Pipeline(store)
    raw = sample(store, "seed", "1")
    pipeline.register("btc", raw["observation"], "test", "exact definition")
    base = pipeline.resolve(SemanticIR.model_validate(raw))[0].model_dump()
    irs = []
    for identity, comparator in [("a", "GTE"), ("b", "GT"), ("c", "GTE")]:
        data = deepcopy(base)
        data["contract_version_id"] = identity
        data["predicate"]["comparator"] = comparator
        irs.append(SemanticIR.model_validate(data))
    assert any(
        {a.contract_version_id, b.contract_version_id} == {"a", "c"} for a, b in candidates(irs)
    )


def test_breakdown_closure_is_storage_independent_and_completion_stage_specific():
    def c(a, b):
        return {
            "a": a,
            "b": b,
            "relation": "IMPLIES",
            "scope": "OBSERVED_EVENT",
            "conditions": [],
            "proof": {"state": "PROVEN_UNDER_PREMISES"},
            "settlement": {"state": "CONDITIONAL"},
            "interpretation_assessments": ["SUPPORTED", "SUPPORTED"],
        }

    b = {
        "benchmark_id": "regression",
        "revision": "1",
        "label_version": "1",
        "labeling_guide": "exact-string-set/1",
        "split": "development",
        "independent_human_labels": False,
        "contracts": [
            {"id": i, "venue": "X" if i == "b" else "Y", "template": "t", "eligible": True}
            for i in "bca"
        ],
        "comparisons": [c("b", "c"), c("c", "a"), c("b", "a")],
        "gold_claims": [c("b", "c"), c("c", "a")],
        "stage_labels": [],
    }
    r = {
        "benchmark_hash": fingerprint(b),
        "scored_before_case_review": True,
        "pipeline_configuration": {"compiler": "test"},
        "acceptance_policy": {
            "version": "1",
            "allowed_scopes": ["OBSERVED_EVENT"],
            "allowed_settlement_states": ["CONDITIONAL"],
            "require_resolved": True,
            "normalization": "exact-string-set/1",
        },
        "proposals": [c("b", "c"), c("c", "a")],
        "outcomes": [
            {"id": "b", "stage": "interpret", "state": "failed"},
            {"id": "b", "stage": "resolve", "state": "complete"},
        ],
    }
    sparse = evaluate(b, r)
    materialized = evaluate(b, r | {"proposals": r["proposals"] + [c("b", "a")]})
    assert (
        sparse["breakdowns"]["venue:X"]["relationships"]["selected_recall"]
        == materialized["breakdowns"]["venue:X"]["relationships"]["selected_recall"]
    )
    assert sparse["coverage"]["completed_per_eligible"]["correct"] == 0
