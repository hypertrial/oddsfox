import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from oddsfox.compiler import blank_ir
from oddsfox.demo import sample
from oddsfox.ir import SemanticIR
from oddsfox.pipeline import Pipeline
from oddsfox.reasoning import RELATIONS, probability_constraint, settlement, solver_verify, verify


def pair(store, comparator_a="GTE", comparator_b="GTE", a="150000", b="100000"):
    raw_a = sample(store, "a", a, comparator=comparator_a)
    raw_b = sample(store, "b", b, comparator=comparator_b)
    pipe = Pipeline(store)
    pipe.register("btc", raw_a["observation"], "test", "reviewed exact observation")
    return pipe.resolve(SemanticIR.model_validate(raw_a))[0], pipe.resolve(
        SemanticIR.model_validate(raw_b)
    )[0]


@pytest.mark.parametrize(
    "ca,cb,a,b,relation,proven",
    [
        ("GTE", "GTE", "150000", "100000", "IMPLIES", True),
        ("GTE", "GT", "100000", "100000", "IMPLIES", False),
        ("GT", "GTE", "100000", "100000", "IMPLIES", True),
        ("GT", "LTE", "1", "1", "COMPLEMENT", True),
        ("GTE", "LTE", "1", "1", "EXCLUDES", False),
        ("GT", "LT", "1", "1", "COMPLEMENT", False),
        ("GTE", "LT", "1.00000000000000000000001", "1", "EXCLUDES", True),
    ],
)
def test_rules_and_solver_boundaries(store, ca, cb, a, b, relation, proven):
    left, right = pair(store, ca, cb, a, b)
    result = verify(left, right, relation)
    assert (result["state"] == "PROVEN_UNDER_PREMISES") == proven
    solver = solver_verify(left, right, relation)
    assert solver["state"] == result["state"]
    assert "check-sat" in solver["smtlib"]


def test_nonvacuity_and_inconsistent_domain(store):
    a, b = pair(store)
    assert verify(a, b, "IMPLIES", lower="2", upper="1")["state"] == "UNKNOWN"
    assert "infeasible" in verify(a, b, "IMPLIES", upper="10")["reason"]


def test_settlement_is_conditioned_and_probability_bounds_present(store):
    a, b = pair(store)
    result = settlement(a, b)
    assert result["state"] == "CONDITIONAL" and result["conditions"]
    constraint = probability_constraint("IMPLIES", "A", "B", result["conditions"])
    assert constraint["bounds"] == ["0 <= P(A | C) <= 1", "0 <= P(B | C) <= 1"]
    assert constraint["conditioning_requirement"] == "P(C) > 0"


@settings(max_examples=70, deadline=None)
@given(
    st.integers(-1000000, 1000000),
    st.integers(-1000000, 1000000),
    st.sampled_from(["GT", "GTE", "LT", "LTE"]),
    st.sampled_from(["GT", "GTE", "LT", "LTE"]),
    st.sampled_from(RELATIONS),
)
def test_exact_threshold_rules_match_cvc5(a, b, ca, cb, relation):
    base = blank_ir("a")
    base["observation"] = {k: "value" for k in base["observation"]}
    base["observation"].update(
        timestamp="2027-01-01T00:00:00Z",
        timezone="UTC",
        precision="0.01",
        measurement_method="instantaneous",
    )
    base["settlement_semantics"] = {k: "value" for k in base["settlement_semantics"]}
    base["settlement_semantics"].update(
        cutoff="2027-01-01T00:00:00Z",
        payout_mapping={
            "unit": "USD",
            "outcomes": [
                {"outcome_id": "no", "if_true": "0", "if_false": "1"},
                {"outcome_id": "yes", "if_true": "1", "if_false": "0"},
            ],
        },
    )
    base["predicate"].update(comparator=ca, threshold=str(a))
    left = SemanticIR.model_validate(base)
    base["predicate"].update(comparator=cb, threshold=str(b))
    base["contract_version_id"] = "b"
    right = SemanticIR.model_validate(base)
    assert verify(left, right, relation)["state"] == solver_verify(left, right, relation)["state"]


def test_settlement_conditions_bound_to_exact_pair(store):
    from oddsfox.evaluation import claim_key, closure, comparison_key

    a, b = pair(store)
    c = b.model_copy(update={"contract_version_id": "c" * 64})
    ab, bc = settlement(a, b)["conditions"], settlement(b, c)["conditions"]
    assert ab != bc
    assert settlement(b, a)["conditions"] == ab
    claims = [
        {
            "a": x.contract_version_id,
            "b": y.contract_version_id,
            "relation": "IMPLIES",
            "scope": "SETTLEMENT_OUTCOME",
            "conditions": cond,
        }
        for x, y, cond in [(a, b, ab), (b, c, bc), (a, c, ab)]
    ]
    keys = {claim_key(x) for x in claims[:2]}
    assert closure(keys, {comparison_key(x) for x in claims}) == keys


def test_dense_settlement_candidate_limit_is_explicit(store):
    from oddsfox.reasoning import candidates

    a, _ = pair(store)
    batch = [a.model_copy(update={"contract_version_id": str(i)}) for i in range(180)]
    assert len(candidates(batch[:179], settlement_pairs=True)) == 15931
    assert len(candidates(batch)) == 179
    with pytest.raises(ValueError, match="bounded V1 candidate limit"):
        candidates(batch, settlement_pairs=True)
