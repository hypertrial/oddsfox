"""Exact positive unit scaling authorized by a reviewed registry alias."""

from fractions import Fraction

from oddsfox.ir import SemanticIR, decimal_string


def scale_decimal(value: str, factor: str) -> str:
    decimal_string(value)
    decimal_string(factor)
    multiplier = Fraction(factor)
    if multiplier <= 0:
        raise ValueError("unit scale must be positive to preserve comparator direction")
    scaled = Fraction(value) * multiplier
    denominator = scaled.denominator
    twos = fives = 0
    while denominator % 2 == 0:
        twos += 1
        denominator //= 2
    while denominator % 5 == 0:
        fives += 1
        denominator //= 5
    if denominator != 1:
        raise ValueError("normalization does not have a finite exact decimal")
    digits = max(twos, fives)
    number = abs(scaled.numerator) * (10**digits // scaled.denominator)
    text = str(number).zfill(digits + 1)
    if digits:
        text = (text[:-digits] + "." + text[-digits:]).rstrip("0").rstrip(".")
    if scaled < 0:
        text = "-" + text
    return decimal_string(text)


def normalize_alias(ir: SemanticIR, registry: dict, alias: dict, derivations: dict) -> SemanticIR:
    data = ir.model_dump()
    target = registry["data"]["definition"]
    original = ir.observation.model_dump()
    original["canonical_observation_id"] = original["canonical_observation_version"] = None
    if original != alias["definition"]:
        raise ValueError("alias applicability mismatch")
    changes = {f"/observation/{k}": v for k, v in target.items() if original[k] != v}
    if ir.predicate.threshold is not None:
        scaled = scale_decimal(ir.predicate.threshold, alias["unit_factor"])
        if scaled != ir.predicate.threshold:
            changes["/predicate/threshold"] = scaled
    for pointer, value in changes.items():
        evidence = data["field_evidence"].get(pointer)
        if not evidence:
            raise ValueError("alias normalization requires original field evidence")
        spans = evidence["source_spans"]
        if not spans and evidence["derivation_ref"]:
            spans = derivations[evidence["derivation_ref"]]["source_spans"]
        reference = f"{registry['id']}:{pointer}"
        derivations[reference] = {
            "pointer": pointer,
            "value": value,
            "source_spans": spans,
            "reviewed_rule": registry["id"],
            "unit_factor": alias["unit_factor"],
        }
        data["field_evidence"][pointer] = {"source_spans": [], "derivation_ref": reference}
        root, key = pointer[1:].split("/")
        data[root][key] = value
    data["observation"]["canonical_observation_id"] = registry["logical"]
    data["observation"]["canonical_observation_version"] = registry["id"]
    return SemanticIR.model_validate(data)
