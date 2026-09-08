"""Exact threshold reasoning. All claims refer to an explicit real-valued domain.

A measurement increment is descriptive until a reviewed rule supplies a grid;
using the larger real domain is conservative and does not invent such a rule.
"""

from fractions import Fraction
from itertools import combinations, pairwise
from typing import Literal

import cvc5
from cvc5 import Kind  # ty: ignore[unresolved-import] -- binary extension exports no stubs

from oddsfox.ir import SemanticIR, fingerprint

Relation = Literal["EQUIVALENT", "IMPLIES", "EXCLUDES", "COMPLEMENT"]
RELATIONS: tuple[Relation, ...] = ("EQUIVALENT", "IMPLIES", "EXCLUDES", "COMPLEMENT")
RULE_VERSION = "real-threshold-cells/1"


def truth(comparator: str, threshold: Fraction, value: Fraction) -> bool:
    match comparator:
        case "GT":
            return value > threshold
        case "GTE":
            return value >= threshold
        case "LT":
            return value < threshold
        case "LTE":
            return value <= threshold
        case _:
            raise ValueError("unsupported comparator")


def relation_holds(relation: Relation, a: bool, b: bool) -> bool:
    match relation:
        case "EQUIVALENT":
            return a == b
        case "IMPLIES":
            return not a or b
        case "EXCLUDES":
            return not (a and b)
        case "COMPLEMENT":
            return a != b
        case _:
            raise ValueError("unsupported relation")


def eligible(ir: SemanticIR) -> bool:
    return (
        all(v is not None for v in ir.observation.model_dump().values())
        and ir.observation.measurement_method == "instantaneous"
        and ir.predicate.threshold is not None
        and ir.predicate.comparator is not None
        and all(v is not None for v in ir.settlement_semantics.model_dump().values())
        and ir.settlement_semantics.payout_mapping is not None
        and ir.settlement_semantics.payout_mapping.unit is not None
        and all(
            p.if_true is not None and p.if_false is not None
            for p in ir.settlement_semantics.payout_mapping.outcomes
        )
    )


def same_observation(a: SemanticIR, b: SemanticIR) -> bool:
    return a.observation.canonical_observation_id is not None and a.observation == b.observation


def verify(
    a: SemanticIR,
    b: SemanticIR,
    relation: Relation,
    lower: str | None = None,
    upper: str | None = None,
) -> dict:
    if not same_observation(a, b) or not eligible(a) or not eligible(b):
        return {"state": "UNKNOWN", "reason": "incomplete or different observations"}
    assert a.predicate.threshold is not None and b.predicate.threshold is not None
    assert a.predicate.comparator is not None and b.predicate.comparator is not None
    x, y = Fraction(a.predicate.threshold), Fraction(b.predicate.threshold)
    lo, hi = (
        Fraction(lower) if lower is not None else None,
        Fraction(upper) if upper is not None else None,
    )
    if lo is not None and hi is not None and lo > hi:
        return {"state": "UNKNOWN", "reason": "inconsistent domain premises"}
    boundaries = sorted(
        {x, y} | ({lo} if lo is not None else set()) | ({hi} if hi is not None else set())
    )
    points = boundaries + [(p + q) / 2 for p, q in pairwise(boundaries)]
    points += [boundaries[0] - 1, boundaries[-1] + 1]
    points = sorted(p for p in points if (lo is None or p >= lo) and (hi is None or p <= hi))
    values = [
        (truth(a.predicate.comparator, x, p), truth(b.predicate.comparator, y, p)) for p in points
    ]
    if not points or not any(va for va, _ in values) or not any(vb for _, vb in values):
        return {"state": "UNKNOWN", "reason": "infeasible operand; vacuous relation quarantined"}
    witnesses = [
        str(p)
        for p, (va, vb) in zip(points, values, strict=True)
        if not relation_holds(relation, va, vb)
    ]
    return {
        "state": "DISPROVEN" if witnesses else "PROVEN_UNDER_PREMISES",
        "rule": RULE_VERSION,
        "relation": relation,
        "operands": [a.digest(), b.digest()],
        "domain": {"kind": "real", "lower": lower, "upper": upper},
        "substitutions": [a.predicate.model_dump(), b.predicate.model_dump()],
        "cell_representatives": [str(p) for p in points],
        "counterexamples": witnesses,
    }


def solver_verify(a: SemanticIR, b: SemanticIR, relation: Relation, timeout_ms: int = 1000) -> dict:
    """Independent encoding check with bounded solver effort and retained SMT-LIB."""
    if not same_observation(a, b) or not eligible(a) or not eligible(b):
        return {"state": "UNKNOWN", "reason": "ineligible operands"}
    if not 1 <= timeout_ms <= 10000:
        raise ValueError("solver timeout outside 1..10000 ms")
    solver = cvc5.Solver()  # ty: ignore[unresolved-attribute] -- binary extension
    solver.setLogic("QF_LRA")
    solver.setOption("tlimit-per", str(timeout_ms))
    solver.setOption("produce-models", "true")
    x = solver.mkConst(solver.getRealSort(), "x")
    kinds = {"GT": Kind.GT, "GTE": Kind.GEQ, "LT": Kind.LT, "LTE": Kind.LEQ}

    def predicate(ir):
        return solver.mkTerm(
            kinds[ir.predicate.comparator], x, solver.mkReal(ir.predicate.threshold)
        )

    aa, bb = predicate(a), predicate(b)

    def not_(t):
        return solver.mkTerm(Kind.NOT, t)

    match relation:
        case "EQUIVALENT":
            bad = solver.mkTerm(Kind.XOR, aa, bb)
        case "IMPLIES":
            bad = solver.mkTerm(Kind.AND, aa, not_(bb))
        case "EXCLUDES":
            bad = solver.mkTerm(Kind.AND, aa, bb)
        case "COMPLEMENT":
            bad = solver.mkTerm(Kind.EQUAL, aa, bb)
        case _:
            raise ValueError("unsupported relation")
    # The domain is R, and each finite threshold predicate is feasible in R.
    solver.assertFormula(bad)
    result = solver.checkSat()
    return {
        "state": "PROVEN_UNDER_PREMISES"
        if result.isUnsat()
        else "DISPROVEN"
        if result.isSat()
        else "UNKNOWN",
        "solver": "cvc5",
        "version": solver.getVersion(),
        "options": {"tlimit-per": str(timeout_ms)},
        "smtlib": f"(set-logic QF_LRA)\n(declare-const x Real)\n(assert {bad})\n(check-sat)\n",
        "result": str(result),
        "certificate": None,
        "operands": [a.digest(), b.digest()],
    }


def candidates(
    irs: list[SemanticIR], *, settlement_pairs: bool = False
) -> list[tuple[SemanticIR, SemanticIR]]:
    """Sparse observed-event edges or explicit pairs for conditional settlement."""
    if len(irs) > 250:
        raise ValueError("V1 comparison batch is limited to 250 interpretations")
    result = []
    for pair in candidate_pairs(irs, settlement_pairs=settlement_pairs):
        result.append(pair)
        if len(result) > 16000:
            raise ValueError("comparison component exceeds the bounded V1 candidate limit")
    return result


def candidate_pairs(irs: list[SemanticIR], *, settlement_pairs=False, observed_edges_only=False):
    """Stream whole-observation pairs; never partition away cross-batch relations."""
    groups: dict[str, list[SemanticIR]] = {}
    for ir in irs:
        if eligible(ir):
            groups.setdefault(fingerprint(ir.observation.model_dump()), []).append(ir)
    for group in groups.values():

        def order(ir: SemanticIR):
            assert ir.predicate.threshold is not None and ir.predicate.comparator is not None
            strictness = {"GTE": 0, "GT": 1, "LT": 0, "LTE": 1}
            return (
                Fraction(ir.predicate.threshold),
                strictness[ir.predicate.comparator],
                ir.contract_version_id,
            )

        increasing = sorted(
            [ir for ir in group if ir.predicate.comparator in {"GT", "GTE"}], key=order
        )
        decreasing = sorted(
            [ir for ir in group if ir.predicate.comparator in {"LT", "LTE"}], key=order
        )
        # Pair-specific settlement conditions do not support transitive closure.
        yield from (combinations(increasing, 2) if settlement_pairs else pairwise(increasing))
        yield from (combinations(decreasing, 2) if settlement_pairs else pairwise(decreasing))
        # Nontransitive exclusion/complement needs explicit cross-polarity candidates.
        if not observed_edges_only:
            yield from ((a, b) for a in increasing for b in decreasing)


def differences(irs: list[SemanticIR]) -> list[dict]:
    if len(irs) > 250:
        raise ValueError("V1 comparison batch is limited to 250 interpretations")
    result = []
    for index, a in enumerate(irs):
        for b in irs[index + 1 :]:
            if (a.observation.quantity, a.observation.instrument_or_series) != (
                b.observation.quantity,
                b.observation.instrument_or_series,
            ):
                continue
            if same_observation(a, b):
                continue
            changed = {
                key: {"a": value, "b": b.observation.model_dump()[key]}
                for key, value in a.observation.model_dump().items()
                if value != b.observation.model_dump()[key]
            }
            result.append(
                {
                    "a": a.contract_version_id,
                    "b": b.contract_version_id,
                    "state": "NOT_COMPARABLE",
                    "reason": "different or unresolved canonical observations; no shared proof context",
                    "differences": changed,
                }
            )
    return result


def settlement(a: SemanticIR, b: SemanticIR) -> dict:
    """Never execute prose policy strings or infer that matching strings are equivalent."""
    sa, sb = a.settlement_semantics, b.settlement_semantics
    differences = [key for key in sa.model_dump() if sa.model_dump()[key] != sb.model_dump()[key]]
    if not eligible(a) or not eligible(b):
        return {
            "state": "UNKNOWN",
            "conditions": [],
            "differences": differences,
            "reason": "unresolved governing semantics",
        }

    def binary(mapping):
        return mapping is not None and sorted(
            (p.if_true, p.if_false) for p in mapping.outcomes
        ) == [("0", "1"), ("1", "0")]

    if not binary(sa.payout_mapping) or not binary(sb.payout_mapping):
        return {
            "state": "DIFFERENT",
            "conditions": [],
            "differences": differences,
            "reason": "ordinary payout mapping is not Boolean",
        }
    condition = (
        f"Contract versions {', '.join(sorted((a.contract_version_id, b.contract_version_id)))} "
        "settle according to their captured ordinary binary payout mappings, "
        "using the reviewed observation; neither uses missing-data, cancellation, exceptional, "
        "dispute or clarification overrides."
    )
    return {
        "state": "CONDITIONAL",
        "conditions": [condition],
        "differences": differences,
        "reason": "ordinary-binary/1; exceptional policy prose has no executable model",
    }


def probability_constraint(relation: Relation, a: str, b: str, conditions: list[str]) -> dict:
    suffix = " | C" if conditions else ""
    pa, pb = f"P({a}{suffix})", f"P({b}{suffix})"
    expressions = {
        "EQUIVALENT": f"{pa} = {pb}",
        "IMPLIES": f"{pa} <= {pb}",
        "EXCLUDES": f"{pa} + {pb} <= 1",
        "COMPLEMENT": f"{pa} + {pb} = 1",
    }
    return {
        "expression": expressions[relation],
        "bounds": [f"0 <= {pa} <= 1", f"0 <= {pb} <= 1"],
        "conditions": conditions,
        "conditioning_requirement": "P(C) > 0" if conditions else None,
        "probability_space": "common reviewed observation",
        "kind": "symbolic; not market prices",
    }


def constraints_feasible(claims: list[dict], timeout_ms: int = 1000) -> bool:
    solver = cvc5.Solver()  # ty: ignore[unresolved-attribute] -- binary extension
    solver.setLogic("QF_LRA")
    solver.setOption("tlimit-per", str(timeout_ms))
    events = {}
    for claim in claims:
        for operand in (claim["a"], claim["b"]):
            # Scope and conditions must share the same context before inequalities combine.
            key = (operand, claim["scope"], tuple(claim["conditions"]))
            if key not in events:
                p = solver.mkConst(solver.getRealSort(), f"p{len(events)}")
                events[key] = p
                solver.assertFormula(solver.mkTerm(Kind.GEQ, p, solver.mkReal(0)))
                solver.assertFormula(solver.mkTerm(Kind.LEQ, p, solver.mkReal(1)))
        ctx = (claim["scope"], tuple(claim["conditions"]))
        a, b = events[(claim["a"], *ctx)], events[(claim["b"], *ctx)]
        match claim["relation"]:
            case "IMPLIES":
                formula = solver.mkTerm(Kind.LEQ, a, b)
            case "EQUIVALENT":
                formula = solver.mkTerm(Kind.EQUAL, a, b)
            case "EXCLUDES":
                formula = solver.mkTerm(Kind.LEQ, solver.mkTerm(Kind.ADD, a, b), solver.mkReal(1))
            case "COMPLEMENT":
                formula = solver.mkTerm(Kind.EQUAL, solver.mkTerm(Kind.ADD, a, b), solver.mkReal(1))
            case _:
                raise ValueError("unsupported relation")
        solver.assertFormula(formula)
    return solver.checkSat().isSat()


def logical_feasible(
    claims: list[dict], irs: dict[str, SemanticIR], timeout_ms: int = 1000
) -> bool:
    solver = cvc5.Solver()  # ty: ignore[unresolved-attribute] -- binary extension
    solver.setLogic("QF_LRA")
    solver.setOption("tlimit-per", str(timeout_ms))
    variables = {}
    kinds = {"GT": Kind.GT, "GTE": Kind.GEQ, "LT": Kind.LT, "LTE": Kind.LEQ}
    for claim in claims:
        operands = [irs[identity] for identity in (claim["a"], claim["b"])]
        if not same_observation(*operands) or not all(eligible(ir) for ir in operands):
            return False
        context = (
            fingerprint(operands[0].observation.model_dump()),
            claim["scope"],
            tuple(claim["conditions"]),
        )
        if context not in variables:
            variables[context] = solver.mkConst(solver.getRealSort(), f"x{len(variables)}")
        terms = []
        for ir in operands:
            assert ir.predicate.comparator is not None and ir.predicate.threshold is not None
            terms.append(
                solver.mkTerm(
                    kinds[ir.predicate.comparator],
                    variables[context],
                    solver.mkReal(ir.predicate.threshold),
                )
            )
        a, b = terms
        match claim["relation"]:
            case "IMPLIES":
                formula = solver.mkTerm(Kind.IMPLIES, a, b)
            case "EQUIVALENT":
                formula = solver.mkTerm(Kind.EQUAL, a, b)
            case "EXCLUDES":
                formula = solver.mkTerm(Kind.NOT, solver.mkTerm(Kind.AND, a, b))
            case "COMPLEMENT":
                formula = solver.mkTerm(Kind.XOR, a, b)
            case _:
                raise ValueError("unsupported relation")
        solver.assertFormula(formula)
    return solver.checkSat().isSat()
