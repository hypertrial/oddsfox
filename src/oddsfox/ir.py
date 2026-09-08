"""The public, language-independent 1.0.0 semantic boundary.

Structural validation cannot establish whether a quotation supports a meaning.
That judgement belongs to the separately recorded semantic review.
"""

import hashlib
import json
import re
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal, Self
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import rfc8785
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_validator,
)

Text = Annotated[str, StringConstraints(min_length=1, max_length=20000)]


def decimal_string(value: str) -> str:
    if not re.fullmatch(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?", value) or value == "-0":
        raise ValueError("expected canonical exact decimal string")
    if len(value) > 256:
        raise ValueError("decimal exceeds the bounded V1 precision")
    return value


def instant(value: str) -> str:
    if not re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]*[1-9])?Z", value
    ):
        raise ValueError("expected canonical UTC instant")
    datetime.fromisoformat(value)
    return value


Exact = Annotated[
    str,
    StringConstraints(
        pattern=r"^(?:0|[1-9][0-9]*|-[1-9][0-9]*|-?(?:0|[1-9][0-9]*)\.[0-9]*[1-9])$", max_length=256
    ),
    AfterValidator(decimal_string),
]
Instant = Annotated[
    str,
    StringConstraints(
        pattern=r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]*[1-9])?Z$",
        max_length=256,
    ),
    AfterValidator(instant),
]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    @model_validator(mode="before")
    @classmethod
    def unicode_scalars(cls, value):
        def check(v):
            if isinstance(v, str) and any(0xD800 <= ord(c) <= 0xDFFF for c in v):
                raise ValueError("unpaired Unicode surrogate is not valid JCS text")
            if isinstance(v, dict):
                for k, child in v.items():
                    check(k)
                    check(child)
            elif isinstance(v, (list, tuple)):
                for child in v:
                    check(child)

        check(value)
        return value


class Observation(StrictModel):
    canonical_observation_id: Text | None
    canonical_observation_version: Text | None
    quantity: Text | None
    source: Text | None
    instrument_or_series: Text | None
    unit: Text | None
    timestamp: Instant | None
    timezone: Text | None
    measurement_method: Text | None
    precision: Exact | None
    revision_policy: Text | None

    @model_validator(mode="after")
    def valid(self) -> Self:
        if (self.canonical_observation_id is None) != (self.canonical_observation_version is None):
            raise ValueError("canonical identity and version must resolve together")
        if self.precision is not None and Decimal(self.precision) <= 0:
            raise ValueError("precision must be a positive measurement increment")
        if self.timezone is not None:
            if re.fullmatch(r"[+-](?:0[0-9]|1[0-4]):[0-5][0-9]", self.timezone):
                if self.timezone[1:3] == "14" and self.timezone[4:] != "00":
                    raise ValueError("invalid UTC offset")
            else:
                try:
                    ZoneInfo(self.timezone)
                except ZoneInfoNotFoundError as exc:
                    raise ValueError("unknown timezone") from exc
        return self


class Predicate(StrictModel):
    observation_ref: Literal["/observation"]
    comparator: Literal["GT", "GTE", "LT", "LTE"] | None
    threshold: Exact | None


class Payout(StrictModel):
    outcome_id: Text
    if_true: Exact | None
    if_false: Exact | None


class PayoutMapping(StrictModel):
    unit: Text | None
    outcomes: Annotated[list[Payout], Field(min_length=2, max_length=2)]

    @model_validator(mode="after")
    def ordered(self) -> Self:
        ids = [row.outcome_id for row in self.outcomes]
        if ids != sorted(set(ids)):
            raise ValueError("outcome IDs must be distinct and sorted by Unicode code point")
        return self


class SettlementSemantics(StrictModel):
    payout_mapping: PayoutMapping | None
    resolution_source: Text | None
    cutoff: Instant | None
    rounding: Text | None
    missing_data_policy: Text | None
    cancellation_policy: Text | None
    exceptional_outcome_policy: Text | None
    dispute_policy: Text | None
    clarification_policy: Text | None


class SourceSpan(StrictModel):
    artifact_id: Text
    start: Annotated[int, Field(ge=0, le=2**53 - 1)]
    end: Annotated[int, Field(ge=1, le=2**53 - 1)]

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if self.start >= self.end:
            raise ValueError("empty or reversed source span")
        return self


class FieldEvidence(StrictModel):
    source_spans: list[SourceSpan]
    derivation_ref: Text | None

    @model_validator(mode="after")
    def ordered(self) -> Self:
        keys = [(s.artifact_id, s.start, s.end) for s in self.source_spans]
        if keys != sorted(set(keys)):
            raise ValueError("source spans must be sorted and deduplicated")
        return self


def pointer_value(document: Any, pointer: str) -> Any:
    if not pointer.startswith("/") or re.search(r"~(?![01])", pointer):
        raise ValueError("invalid JSON Pointer")
    parts = pointer[1:].split("/")
    if parts[0] not in {"observation", "predicate", "settlement_semantics"}:
        raise ValueError("evidence must point into semantic objects")
    value = document
    try:
        for part in parts:
            part = part.replace("~1", "/").replace("~0", "~")
            if isinstance(value, list):
                if not re.fullmatch(r"0|[1-9][0-9]*", part):
                    raise ValueError("invalid array index")
                value = value[int(part)]
            else:
                value = value[part]
    except (KeyError, IndexError, TypeError) as exc:
        raise ValueError("dangling evidence pointer") from exc
    return value


def leaves(value: Any, path: str = ""):
    if isinstance(value, dict):
        for key, child in value.items():
            yield from leaves(child, path + "/" + key.replace("~", "~0").replace("/", "~1"))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from leaves(child, path + "/" + str(index))
    else:
        yield path, value


class SemanticIR(StrictModel):
    schema_version: Literal["1.0.0"]
    compiler_version: Text
    contract_version_id: Text
    observation: Observation
    predicate: Predicate
    settlement_semantics: SettlementSemantics
    field_evidence: dict[str, FieldEvidence]

    @model_validator(mode="after")
    def pointers(self) -> Self:
        document = self.model_dump()
        for pointer in self.field_evidence:
            pointer_value(document, pointer)
        return self

    def canonical(self) -> bytes:
        return rfc8785.dumps(self.model_dump())

    def digest(self) -> str:
        return hashlib.sha256(self.canonical()).hexdigest()

    def validate_evidence(
        self,
        artifacts: dict[str, str],
        outcome_ids: list[str],
        derivations: dict[str, dict] | None = None,
    ) -> None:
        """Artifacts must already be restricted to this captured contract version."""
        derivations = derivations or {}
        payout = self.settlement_semantics.payout_mapping
        if payout is not None and [o.outcome_id for o in payout.outcomes] != sorted(outcome_ids):
            raise ValueError("payout IDs do not match captured native outcomes")
        for pointer, evidence in self.field_evidence.items():
            for span in evidence.source_spans:
                if span.artifact_id not in artifacts or span.end > len(artifacts[span.artifact_id]):
                    raise ValueError("source span is outside this contract's evidence")
            if evidence.derivation_ref is not None:
                record = derivations.get(evidence.derivation_ref)
                if (
                    not record
                    or record.get("pointer") != pointer
                    or record.get("value") != pointer_value(self.model_dump(), pointer)
                ):
                    raise ValueError("unresolved or mismatched derivation record")
                for span in record.get("source_spans", []):
                    s = SourceSpan.model_validate(span)
                    if s.artifact_id not in artifacts or s.end > len(artifacts[s.artifact_id]):
                        raise ValueError("invalid derivation source span")
                if not record.get("source_spans"):
                    raise ValueError("derivation must retain underlying source evidence")
        semantic = {
            k: self.model_dump()[k] for k in ("observation", "predicate", "settlement_semantics")
        }
        structural = {
            "/predicate/observation_ref",
            "/observation/canonical_observation_id",
            "/observation/canonical_observation_version",
        }
        for pointer, value in leaves(semantic):
            if value is not None and pointer not in structural:
                evidence = self.field_evidence.get(pointer)
                if evidence is None or not (evidence.source_spans or evidence.derivation_ref):
                    raise ValueError(f"missing evidence for {pointer}")


def strict_json(raw: str | bytes) -> Any:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    def invalid(value):
        raise ValueError(f"non-JSON number: {value}")

    if len(raw) > 4 * 1024 * 1024:
        raise ValueError("input exceeds 4 MiB limit")
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid)


def parse_ir(raw: str | bytes) -> SemanticIR:
    return SemanticIR.model_validate(strict_json(raw))


def fingerprint(value: Any) -> str:
    return hashlib.sha256(rfc8785.dumps(value)).hexdigest()
