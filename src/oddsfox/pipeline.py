"""Capture-to-publication operations over one dependency-aware coordinator."""

import hashlib
import importlib.metadata
import json
import time
from pathlib import Path
from typing import Any

from oddsfox import __version__
from oddsfox.ir import Observation, SemanticIR, fingerprint, parse_ir
from oddsfox.reasoning import (
    RELATIONS,
    RULE_VERSION,
    SETTLEMENT_CLASSIFIER,
    candidate_pairs,
    constraints_feasible,
    eligible,
    logical_feasible,
    probability_constraint,
    settlement,
    verify,
)
from oddsfox.store import StaleInput, Store, now

DEFAULT_CONFIG = {
    "compiler": __version__,
    "schema": "1.0.0",
    "rules": RULE_VERSION,
    "ontology": "reviewed-exact-observations/1",
    "prompt": "evidence-json/1",
    "model": None,
    "quantization": None,
    "decoding": None,
    "implementation_hash": fingerprint(
        {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(Path(__file__).parent.glob("*.py"))
        }
    ),
    "dependencies": {
        name: importlib.metadata.version(name) for name in ("cvc5", "duckdb", "pydantic", "rfc8785")
    },
}


class Pipeline:
    def __init__(self, store: Store):
        self.store = store
        active = store.current("configuration", "active")
        if active and any(
            active["data"].get(key) != DEFAULT_CONFIG[key]
            for key in ("compiler", "schema", "rules", "implementation_hash", "dependencies")
        ):
            updated = {k: v for k, v in active["data"].items() if k != "configuration_revision"}
            updated.update(
                {
                    key: DEFAULT_CONFIG[key]
                    for key in (
                        "compiler",
                        "schema",
                        "rules",
                        "implementation_hash",
                        "dependencies",
                    )
                }
            )
            self.configure(updated)

    def configure(self, config: dict | None = None) -> str:
        complete = dict(DEFAULT_CONFIG) | (config or {})
        with self.store.transaction():
            current = self.store.current("configuration", "active")
            if current and (
                config is None
                or {k: v for k, v in current["data"].items() if k != "configuration_revision"}
                == complete
            ):
                return current["id"]
            complete["configuration_revision"] = len(self.store.list("configuration", True)) + 1
            return self.store.insert("configuration", "active", complete, [], "CURRENT")

    def register(
        self,
        canonical_id: str,
        observation: dict,
        reviewer: str,
        rationale: str,
        aliases: list[dict] | None = None,
    ) -> str:
        self._reviewer(reviewer, rationale)
        definition = Observation.model_validate(observation)
        if definition.measurement_method != "instantaneous":
            raise ValueError("V1 registry only supports instantaneous observations")
        if (
            definition.canonical_observation_id is not None
            or definition.canonical_observation_version is not None
        ):
            raise ValueError("registry definition must leave identity fields null")
        if any(
            v is None for k, v in definition.model_dump().items() if not k.startswith("canonical_")
        ):
            raise ValueError("registry definition must be complete")
        from oddsfox.normalization import scale_decimal

        aliases = aliases or []
        for alias in aliases:
            if set(alias) != {"definition", "unit_factor"}:
                raise ValueError("reviewed alias needs exact definition and unit_factor")
            source = Observation.model_validate(alias["definition"])
            if (
                any(
                    v is None
                    for k, v in source.model_dump().items()
                    if not k.startswith("canonical_")
                )
                or source.canonical_observation_id is not None
                or source.canonical_observation_version is not None
            ):
                raise ValueError(
                    "alias definition must be complete with unresolved canonical identity"
                )
            if (
                source.measurement_method != "instantaneous"
                or source.timestamp != definition.timestamp
            ):
                raise ValueError("aliases cannot merge different instants or unsupported methods")
            assert source.precision is not None
            if scale_decimal(source.precision, alias["unit_factor"]) != definition.precision:
                raise ValueError("unit conversion must exactly preserve measurement increment")
        with self.store.transaction():
            return self.store.insert(
                "registry",
                canonical_id,
                {
                    "definition": definition.model_dump(),
                    "reviewer": reviewer,
                    "rationale": rationale,
                    "reviewed_at": now(),
                    "alias_policy": "exact reviewed definitions and positive unit scales",
                    "aliases": aliases,
                },
                [],
                "REVIEWED",
            )

    @staticmethod
    def _reviewer(reviewer: str, rationale: str):
        if not reviewer.strip() or not rationale.strip():
            raise ValueError("reviewer and rationale are required")

    def resolve(self, ir: SemanticIR) -> tuple[SemanticIR, list[str]]:
        observation = ir.observation.model_dump()
        named = observation["canonical_observation_id"]
        version = observation["canonical_observation_version"]
        observation["canonical_observation_id"] = None
        observation["canonical_observation_version"] = None
        matches = [r for r in self.store.list("registry") if r["data"]["definition"] == observation]
        if named is not None:
            matches = [r for r in matches if r["logical"] == named and r["id"] == version]
            if not matches:
                raise ValueError("canonical identity does not match a current reviewed definition")
        if len(matches) != 1:
            if named is not None:
                raise ValueError("ambiguous registry definition")
            return ir, []
        registry = matches[0]
        value = ir.model_dump()
        value["observation"]["canonical_observation_id"] = registry["logical"]
        value["observation"]["canonical_observation_version"] = registry["id"]
        return SemanticIR.model_validate(value), [registry["id"]]

    def interpret(
        self,
        raw: str | bytes,
        config_id: str | None = None,
        job_id: str | None = None,
        derivations: dict | None = None,
        mode: str = "manual-candidate",
        original_response_artifact: str | None = None,
    ) -> str:
        raw_artifact = self.store.put_artifact(raw.encode() if isinstance(raw, str) else raw)
        original_response_artifact = original_response_artifact or raw_artifact
        self.store.artifact(original_response_artifact)
        ir = parse_ir(raw)
        contract = self.store.get(ir.contract_version_id)
        if contract["kind"] != "contract":
            raise ValueError("IR requires a captured contract version")
        config_id = config_id or self.configure()
        config = self.store.get(config_id)
        if config["kind"] != "configuration":
            raise ValueError("expected pipeline configuration")
        if ir.compiler_version != config["data"]["compiler"]:
            raise ValueError("IR compiler version differs from run configuration")
        derivations = dict(derivations or {})
        ir.validate_evidence(
            self.store.source_texts(contract["id"]),
            contract["data"]["metadata"]["outcome_ids"],
            derivations,
        )
        ir, registry = self.resolve(ir)
        exact_exists = any(
            r["data"]["definition"] == ir.observation.model_dump()
            for r in self.store.list("registry")
        )
        if ir.observation.canonical_observation_id is None and not exact_exists:
            from oddsfox.normalization import normalize_alias

            matches = [
                (r, a)
                for r in self.store.list("registry")
                for a in r["data"].get("aliases", [])
                if a["definition"] == ir.observation.model_dump()
            ]
            if len(matches) == 1:
                ir = normalize_alias(ir, matches[0][0], matches[0][1], derivations)
        ir, registry = self.resolve(ir)
        ir.validate_evidence(
            self.store.source_texts(contract["id"]),
            contract["data"]["metadata"]["outcome_ids"],
            derivations,
        )
        rule_dependencies = set()
        for evidence in ir.field_evidence.values():
            if evidence.derivation_ref is None:
                continue
            rule_id = derivations[evidence.derivation_ref].get("reviewed_rule")
            if rule_id is not None:
                rule = self.store.get(rule_id)
                if rule["kind"] != "registry" or rule["status"] != "REVIEWED":
                    raise ValueError("derivation rule must be a current reviewed registry version")
                rule_dependencies.add(rule_id)
        ir_artifact = self.store.put_artifact(ir.canonical())
        assessment = (
            "UNSUPPORTED"
            if ir.observation.measurement_method not in {None, "instantaneous"}
            else "SUPPORTED"
            if eligible(ir)
            else "AMBIGUOUS"
        )
        data = {
            "ir": ir.model_dump(),
            "digest": ir.digest(),
            "ir_artifact": ir_artifact,
            "raw_response_artifact": original_response_artifact,
            "validated_response_artifact": raw_artifact,
            "assessment": assessment,
            "reason": "pending semantic review"
            if assessment == "SUPPORTED"
            else "unresolved fields or canonical observation",
            "derivations": derivations or {},
            "mode": mode,
            "configuration": config_id,
        }
        with self.store.transaction():
            if job_id:
                jobs = self.store._rows("SELECT * FROM jobs WHERE id=?", [job_id])
                if (
                    not jobs
                    or jobs[0]["stage"] != "interpret"
                    or set(jobs[0]["inputs"]) != {contract["id"], config_id}
                ):
                    raise ValueError(
                        "model response does not match the job's contract and configuration"
                    )
            if not job_id and mode == "manual-candidate":
                current = self.store.current("interpretation", contract["id"])
                if (
                    current
                    and {k: v for k, v in current["data"].items() if k != "interpretation_revision"}
                    == data
                ):
                    data["interpretation_revision"] = current["data"].get(
                        "interpretation_revision", 0
                    )
                else:
                    data["interpretation_revision"] = (
                        len(self.store.list("interpretation", True)) + 1
                    )
            identity = self.store.insert(
                "interpretation",
                contract["id"],
                data,
                [contract["id"], config_id, *registry, *sorted(rule_dependencies)],
            )
            if job_id:
                self.store.complete_job(job_id, identity, original_response_artifact)
        return identity

    def review(
        self,
        interpretation_id: str,
        reviewer: str,
        rationale: str,
        approve: bool,
        governing_material_complete: bool = False,
    ) -> str:
        self._reviewer(reviewer, rationale)
        with self.store.transaction():
            interpretation = self.store.get(interpretation_id)
            if interpretation["kind"] != "interpretation":
                raise ValueError("review target must be an interpretation")
            self.store.require_current([interpretation_id])
            ir = SemanticIR.model_validate(interpretation["data"]["ir"])
            contract = self.store.get(ir.contract_version_id)
            if approve:
                if not eligible(ir) or not governing_material_complete:
                    raise ValueError(
                        "approval requires complete supported semantics and explicit governing-material attestation"
                    )
                if contract["data"]["metadata"]["capture_status"] == "missing_rules" or any(
                    r["status"] != "captured" for r in contract["data"]["references"]
                ):
                    raise ValueError("uncaptured governing material prevents approval")
                ir.validate_evidence(
                    self.store.source_texts(contract["id"]),
                    contract["data"]["metadata"]["outcome_ids"],
                    interpretation["data"]["derivations"],
                )
                self.resolve(ir)
            identity = self.store.insert(
                "review",
                interpretation_id,
                {
                    "approved": approve,
                    "reviewer": reviewer,
                    "rationale": rationale,
                    "time": now(),
                    "scope": "exact IR digest and dependency versions",
                    "ir_digest": ir.digest(),
                    "governing_material_complete": governing_material_complete,
                },
                [interpretation_id],
                "REVIEWED" if approve else "WITHDRAWN",
            )
            if not approve:
                for row in self.store._rows(
                    "SELECT child FROM dependencies WHERE parent=?", [interpretation_id]
                ):
                    child = self.store.get(row["child"])
                    if child["kind"] == "assertion":
                        self.store.invalidate(child["id"], "interpretation rejected")
            return identity

    def compare(self) -> list[dict]:
        return [
            claim
            for context in self.comparison_contexts().values()
            for claim in self.iter_comparisons(records=context["records"])
        ]

    def iter_comparisons(self, *, pair_start=0, pair_limit=None, on_pair=None, records=None):
        with self.store.lock:
            records = self.store.list("interpretation") if records is None else records
            approved_ids = {r["id"] for r in records if self._approved(r)}
        irs = [SemanticIR.model_validate(r["data"]["ir"]) for r in records]
        by_contract = {r["data"]["ir"]["contract_version_id"]: r for r in records}
        observed_pairs = {
            frozenset((a.contract_version_id, b.contract_version_id))
            for subset in (
                irs,
                [
                    SemanticIR.model_validate(r["data"]["ir"])
                    for r in records
                    if r["id"] in approved_ids
                ],
            )
            for a, b in candidate_pairs(subset, observed_edges_only=True)
        }
        for pair_index, (a, b) in enumerate(candidate_pairs(irs, settlement_pairs=True)):
            if pair_index < pair_start:
                continue
            if pair_limit is not None and pair_index >= pair_start + pair_limit:
                return
            cross_polarity = (a.predicate.comparator in {"GT", "GTE"}) != (
                b.predicate.comparator in {"GT", "GTE"}
            )
            compatibility = settlement(a, b)
            for left, right in ((a, b), (b, a)):
                for relation in RELATIONS:
                    if (
                        relation != "IMPLIES"
                        and left.contract_version_id > right.contract_version_id
                    ):
                        continue
                    proof = verify(left, right, relation)
                    if proof["state"] != "PROVEN_UNDER_PREMISES":
                        continue
                    for scope in ("OBSERVED_EVENT", "SETTLEMENT_OUTCOME"):
                        if (
                            scope == "OBSERVED_EVENT"
                            and not cross_polarity
                            and frozenset((a.contract_version_id, b.contract_version_id))
                            not in observed_pairs
                        ):
                            continue
                        if scope == "SETTLEMENT_OUTCOME" and compatibility["state"] not in {
                            "COMPATIBLE",
                            "CONDITIONAL",
                        }:
                            continue
                        conditions = (
                            compatibility["conditions"] if scope == "SETTLEMENT_OUTCOME" else []
                        )
                        operands = [
                            by_contract[left.contract_version_id],
                            by_contract[right.contract_version_id],
                        ]
                        claim = {
                            "a": left.contract_version_id,
                            "b": right.contract_version_id,
                            "relation": relation,
                            "scope": scope,
                            "conditions": conditions,
                            "interpretations": [r["id"] for r in operands],
                            "ir_digests": [left.digest(), right.digest()],
                            "interpretation_assessments": [
                                "REVIEWED" if r["id"] in approved_ids else r["data"]["assessment"]
                                for r in operands
                            ],
                            "settlement": compatibility,
                            "proof": proof,
                            "premises": {
                                "domain": "real",
                                "common_observation": left.observation.model_dump(),
                            },
                            "configuration": [r["data"]["configuration"] for r in operands],
                            "evidence": [
                                left.model_dump()["field_evidence"],
                                right.model_dump()["field_evidence"],
                            ],
                            "constraint": probability_constraint(
                                relation,
                                left.contract_version_id,
                                right.contract_version_id,
                                conditions,
                            ),
                        }
                        claim["claim_id"] = fingerprint(
                            {k: claim[k] for k in ("a", "b", "relation", "scope", "conditions")}
                        )
                        yield claim
            if on_pair:
                on_pair(pair_index + 1)

    def comparison_contexts(self):
        with self.store.lock:
            groups = {}
            for record in self.store.list("interpretation"):
                ir = SemanticIR.model_validate(record["data"]["ir"])
                contract = self.store.get(ir.contract_version_id)["data"]
                fields = contract["metadata"].get("governing_fields", {})
                if (
                    not eligible(ir)
                    or fields.get("mve_selected_legs")
                    or fields.get("mve_collection_ticker")
                    or fields.get("is_combination")
                ):
                    continue
                key = fingerprint(ir.observation.model_dump())
                groups.setdefault(key, []).append(record)
            contexts = {}
            for key, records in groups.items():
                reviews = [self.store.current("review", r["id"]) for r in records]
                signature = fingerprint(
                    {
                        "records": sorted(r["id"] for r in records),
                        "reviews": sorted(r["id"] for r in reviews if r),
                        "settlement_classifier": SETTLEMENT_CLASSIFIER,
                    }
                )
                contexts[key] = {"signature": signature, "records": records}
            return contexts

    def comparison_signature(self):
        return fingerprint({k: v["signature"] for k, v in self.comparison_contexts().items()})

    def cached_comparisons(self):
        contexts = self.comparison_contexts()
        states = []
        for key, context in contexts.items():
            rows = self.store._rows(
                "SELECT * FROM comparison_groups WHERE group_id=? AND signature=?",
                [key, context["signature"]],
            )
            states.append(rows[0] if rows else {"state": "pending", "processed": 0})
        return {
            "state": "complete"
            if all(s["state"] == "complete" for s in states)
            else "running"
            if any(s["state"] == "running" for s in states)
            else "pending",
            "processed": sum(s["processed"] for s in states),
            "groups": len(contexts),
            "signature": fingerprint({k: v["signature"] for k, v in contexts.items()}),
        }

    def cached_rows(self, offset=0, limit=250):
        signatures = [v["signature"] for v in self.comparison_contexts().values()]
        if not signatures:
            return []
        return [
            r["data"]
            for r in self.store._rows(
                "SELECT data FROM comparison_rows WHERE signature IN ("
                + ",".join("?" for _ in signatures)
                + ") ORDER BY id LIMIT ? OFFSET ?",
                [*signatures, limit, offset],
            )
        ]

    def cached_row_coverage(self, limit=250):
        signatures = [v["signature"] for v in self.comparison_contexts().values()]
        total = 0
        if signatures:
            total = int(
                self.store._rows(
                    "SELECT COUNT(*) AS n FROM comparison_rows WHERE signature IN ("
                    + ",".join("?" for _ in signatures)
                    + ")",
                    signatures,
                )[0]["n"]
            )
        return {
            "processed": min(limit, total),
            "total": total,
            "complete": total <= limit,
        }

    def refresh_comparisons(self, max_pairs=250):
        pending = []
        for key, context in self.comparison_contexts().items():
            rows = self.store._rows(
                "SELECT * FROM comparison_groups WHERE group_id=? AND signature=?",
                [key, context["signature"]],
            )
            if not rows or rows[0]["state"] != "complete":
                pending.append((rows[0]["updated"] if rows else 0, key, context, rows))
        if not pending:
            return
        _, key, context, rows = min(pending, key=lambda x: (x[0], x[1]))
        signature = context["signature"]
        cursor = rows[0]["pair_cursor"] if rows else 0
        count = rows[0]["processed"] if rows else 0
        with self.store.transaction():
            self.store.db.execute(
                "INSERT INTO comparison_groups VALUES (?,?,'running',?,?,?) ON CONFLICT(group_id) DO UPDATE SET signature=excluded.signature,state='running',processed=excluded.processed,pair_cursor=excluded.pair_cursor,updated=excluded.updated",
                [key, signature, count, cursor, time.time()],
            )
        advanced = cursor

        def checkpoint(value):
            nonlocal advanced
            with self.store.transaction():
                current = self.comparison_contexts().get(key)
                if not current or current["signature"] != signature:
                    raise StaleInput("comparison observation changed")
                self.store.db.execute(
                    "UPDATE comparison_groups SET pair_cursor=?,processed=?,updated=? WHERE group_id=? AND signature=?",
                    [value, count, time.time(), key, signature],
                )
            advanced = value

        try:
            for claim in self.iter_comparisons(
                pair_start=cursor,
                pair_limit=max_pairs,
                on_pair=checkpoint,
                records=context["records"],
            ):
                with self.store.transaction():
                    self.store.db.execute(
                        "INSERT INTO comparison_rows VALUES (?,?,?) ON CONFLICT DO NOTHING",
                        [signature, claim["claim_id"], json.dumps(claim)],
                    )
                count += 1
        except StaleInput:
            return
        with self.store.transaction():
            current = self.comparison_contexts().get(key)
            if current and current["signature"] == signature and advanced < cursor + max_pairs:
                self.store.db.execute(
                    "UPDATE comparison_groups SET state='complete',processed=? WHERE group_id=? AND signature=?",
                    [count, key, signature],
                )

    def _approved(self, interpretation: dict) -> bool:
        review = self.store.current("review", interpretation["id"])
        return bool(
            review
            and review["data"]["approved"]
            and review["data"]["ir_digest"] == interpretation["data"]["digest"]
        )

    def publish(self, proposals: list[dict] | None = None) -> list[str]:
        # Re-derive under the current snapshot; never trust caller-provided proof fields.
        expected = self.compare()
        if proposals is not None and proposals != expected:
            raise StaleInput("proposal set changed; recompute from current dependencies")
        proposals = expected
        proof_artifacts = {
            c["claim_id"]: self.store.put_artifact(json.dumps(c["proof"], sort_keys=True).encode())
            for c in proposals
        }
        with self.store.transaction():
            self.store.require_current(
                [identity for claim in proposals for identity in claim["interpretations"]]
            )
            current_irs = {
                r["data"]["ir"]["contract_version_id"]: SemanticIR.model_validate(r["data"]["ir"])
                for r in self.store.list("interpretation")
            }
            if not constraints_feasible(proposals) or not logical_feasible(proposals, current_irs):
                self.store.insert(
                    "quarantine",
                    fingerprint(proposals),
                    {
                        "reason": "candidate premises or probability constraints are inconsistent or undecided",
                        "proposals": proposals,
                        "proof_artifacts": proof_artifacts,
                    },
                    [],
                    "QUARANTINED",
                )
                return []
            published = []
            for claim in proposals:
                operands = [self.store.get(i) for i in claim["interpretations"]]
                self.store.require_current([r["id"] for r in operands])
                approvals = [self.store.current("review", r["id"]) for r in operands]
                if not all(self._approved(r) for r in operands):
                    continue
                dependencies = claim["interpretations"] + [a["id"] for a in approvals if a]
                data = claim | {
                    "proof_artifact": proof_artifacts[claim["claim_id"]],
                    "approvals": [a["id"] for a in approvals if a],
                }
                published.append(
                    self.store.insert(
                        "assertion", claim["claim_id"], data, dependencies, "ACCEPTED"
                    )
                )
            return published

    def export_parquet(self, destination: Path):
        with self.store.lock:
            metadata = self.export()
            # Assertions are Parquet rows; supporting records remain in its footer,
            # including when there are no accepted assertions.
            del metadata["assertions"]
            self.store.export_parquet(destination, metadata)

    def export(self, history: bool = False) -> dict[str, Any]:
        with self.store.lock:
            assertions = self.store.list("assertion", history)
            if not history:
                assertions = [r for r in assertions if r["status"] == "ACCEPTED"]
            interpretations = self.store.list("interpretation", history)
            contracts = self.store.list("contract", history)
            return {
                "export_schema_version": "1.0.0",
                "ir_schema_version": "1.0.0",
                "manifest": {
                    "compiler": __version__,
                    "exported_at": now(),
                    "history": history,
                    "contracts": [r["id"] for r in contracts],
                    "interpretations": [r["id"] for r in interpretations],
                    "configuration": self.store.list("configuration", history),
                },
                "contracts": contracts,
                "interpretations": interpretations,
                "assertions": assertions,
                "reviews": self.store.list("review", history),
                "registry": self.store.list("registry", history),
                "freshness": self.store.status()["refreshes"],
            }
