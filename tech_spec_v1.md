# OddsFox — Technical Specification V1

This specification is part of the MIT-licensed OddsFox FOSS project; see the
[repository licensing scope](README.md#license).

## Ownership and processing flow

This document defines the behavior required by the
[product specification](product_spec_v1.md). Concrete libraries, persistence,
and process topology are specified only in the [technology stack](tech_stack_v1.md).

```text
Ingest → Version → Interpret → Resolve → Derive and verify → Review → Publish
                         ↑                                      |
                         └──────── Corrections and revisions ───┘
```

Persist stage inputs and outputs so an interrupted run can resume. Replay of a
stored model response must be distinguishable from a new inference run; recording
a seed does not make model generation inherently reproducible.

## Source contracts and revisions

Adapters capture title, description, complete available resolution rules,
referenced rule documents and clarifications, outcomes, observation and resolution
dates, source/oracle, platform grouping, lifecycle state, and native identifiers.
Preserve exact retrieved payloads and provenance; a normalized record alone is
insufficient evidence. Record missing or inaccessible governing material explicitly.

Each immutable `ContractVersion` records platform, native contract ID, optional
native version, content hash, retrieval timestamp, source URLs, and capture status.
Use a local version identity when the venue supplies no revision number. Record
source effective time separately when available; retrieval time is not effective
time. An unchanged payload adds a refresh observation without mutating the version
or creating duplicate content.

Version referenced material as well as the market payload. A clarification can
invalidate an interpretation even when the title is unchanged. Store the last
successful refresh and retrieval failures so consumers can assess freshness.

## Semantic representation and canonical identity

Represent the supported observation and predicate explicitly:

| Object | Required meaning |
| --- | --- |
| Observation | Quantity/entity, source and series or instrument, unit, instant with timezone, observation method, precision, and revision/vintage policy. |
| Predicate | Canonical observation ID, comparator (`GT`, `GTE`, `LT`, `LTE`), and exact decimal threshold. |
| Settlement rule | Outcome-to-payout mapping, resolution source, cutoff, rounding, missing-data behavior, cancellation, exceptional resolution, and dispute/clarification policy. |
| Interpretation | Contract version, observation, predicate, settlement rule, field-level source spans, unresolved fields, and derivation/review metadata. |

Observation time and resolution cutoff are distinct fields. V1 has no implicit
aggregation: an interval maximum is unsupported. Unknown fields remain unknown;
do not supply convenient defaults for dates, sources, or exceptional outcomes.
Raw model responses may be stored as attempts, but only schema-valid objects can
become interpretations.

Canonical observation IDs are stable and their definitions versioned. Match with
exact identifiers and reviewed aliases first. Model suggestions cannot silently
merge observations. Unit conversions must be exact and preserve comparator and
rounding meaning. A source, observation method, or vintage difference prevents a
merge unless a reviewed rule establishes equivalence.

World predicates and settlement rules are separate objects, but both participate
in any claim about contract settlement. An observed event is not automatically a
Boolean payout under cancellation or exceptional resolution.

## Independent assessment states

| Dimension | States and meaning |
| --- | --- |
| Interpretation | `SUPPORTED`: complete within the supported schema, pending semantic review; `REVIEWED`: approved for these exact dependencies; `AMBIGUOUS`: multiple plausible readings or missing governing details; `UNSUPPORTED`: outside V1. |
| Formal verification | `PROVEN_UNDER_PREMISES`: a rule or solver establishes the encoded claim; `DISPROVEN`: a valid counterexample exists; `UNKNOWN`: timeout, unsupported encoding, or undecided result; `NOT_RUN`. |
| Settlement compatibility | `COMPATIBLE`: the relation survives every branch of the reviewed governing policy, with no known omitted branch; `CONDITIONAL`: it holds only under named restrictions; `DIFFERENT`: a material rule difference prevents the proposed settlement relation; `UNKNOWN`: insufficient evidence. |
| Publication lifecycle | `PROVISIONAL`: awaiting acceptance; `ACCEPTED`: passed publication gates; `STALE`: a dependency changed and reassessment is pending; `WITHDRAWN`: rejected or superseded following reassessment. History remains queryable. |

Semantic review can approve an individual interpretation or a versioned template
with explicit applicability checks. Template approval does not cover new wording
or exceptions outside those checks. Record reviewer, time, rationale, and scope.
No standalone confidence score replaces these states.

## Relation derivation and verification

Generate candidates within compatible canonical observation groups, using sorted
thresholds and comparator rules. Do not classify every arbitrary market pair
with an LLM or materialize all transitive implications. Near-matches can appear
in the report as differences without entering a shared proof context.

Let `D` contain explicit domain and interpretation premises. For eligible
predicates A and B:

```text
EQUIVALENT(A,B): UNSAT(D ∧ (A XOR B))
IMPLIES(A,B):    UNSAT(D ∧ A ∧ ¬B)
EXCLUDES(A,B):   UNSAT(D ∧ A ∧ B)
COMPLEMENT(A,B): UNSAT(D ∧ (A ↔ B))
```

Check that `D` is satisfiable before using it. Check operand feasibility and flag
impossible predicates for review so vacuous implications are not presented as
useful market relationships. A solver's `UNKNOWN` response is never a proof.

Use deterministic threshold rules before solver calls. Both paths retain a
verification artifact: rule identity and substitutions, or solver input, result,
version, options, and certificate when available. Solver agreement on the same
encoding does not validate the natural-language interpretation.

Each assertion records relation, direction, scope (`OBSERVED_EVENT` or
`SETTLEMENT_OUTCOME`), premise set, source spans, interpretation assessments,
settlement assessment, verification artifact, and dependency versions. Include
compiler, model/quantization, prompt, schema, ontology, and rules versions.

Settlement-outcome assertions require an explicit mapping through resolution
branches. Matching ordinary observations alone is insufficient. If cancellation,
missing data, or exceptional payouts cannot be modeled, report conditional
compatibility or abstain; never export an unconditional settlement implication.

## Publication and probability constraints

Accepted assertions require reviewed interpretations, a proof under recorded
premises, and current dependencies. Observed-event assertions can be accepted
despite settlement differences, but must display those differences and must not
be relabeled as settlement assertions. Settlement assertions additionally require
compatible rules, or explicit conditions on every displayed and exported claim.

For events in a common probability space and under a common conditioning context:

```text
Every event A:     0 <= P(A) <= 1
A implies B:       P(A) <= P(B)
A and B exclude:   P(A) + P(B) <= 1
A complements B:   P(A) + P(B) = 1
A is equivalent B: P(A) = P(B)
```

Mutual exclusion means `A ∩ B = ∅`; it is not independence. Conditional claims
must use `P(A | C)` and `P(B | C)` with the same explicit C and `P(C) > 0`.
Apply the same zero-to-one bounds to conditional probabilities. Include these
bounds in feasibility checks, not only the derived relationship inequalities.
These are constraints on event probabilities, not on quoted market prices.

Export scope and premises with each constraint. Do not mix differently
conditioned claims or silently drop settlement exceptions. Check the combined
logical premises and constraint feasibility for each affected compatible
component before publication. On inconsistency, quarantine the affected candidate
set and expose the conflicting evidence for review. Passing these checks does
not establish completeness or consistency across unsupported semantics.

## Incremental processing and recovery

Job identity includes stage, input versions/hashes, and the full configuration
fingerprint relevant to the stage. A content hash alone must not suppress work
after a compiler, prompt, model, ontology, or rule change.

Persist dependencies between source versions, interpretations, canonical
definitions, review approvals, proofs, and constraints. On a relevant change,
atomically mark dependent current assertions stale before making the new source
version current. Recompute the dependency closure, not just immediate graph
neighbors. Unaffected accepted results remain available.

Commit stage output and job completion together. Use bounded retries and retain
failed attempts; an exhausted or interrupted job must not expose partial accepted
output. At publication commit, recheck that every dependency version and review
approval is still current in the same transaction. A job that finishes after a
revision or approval withdrawal may retain historical output but cannot restore
the obsolete assertion to accepted status. Recover interrupted jobs on restart.
Refresh failure preserves historical
evidence but must surface stale freshness rather than claim a successful refresh.

## Consumer contract

The local report and API expose contract versions, interpretations, comparisons,
review decisions, accepted assertions, symbolic constraints, and revision history.
Review actions are explicit writes; querying or exporting never implies approval.
Treat imported contract text and model output as untrusted data. Escape them in
HTML reports, allow only safe external-link schemes, and never interpret their
contents as executable markup or instructions authorizing a review action.

Default accepted queries return only current accepted results from a consistent
database snapshot. Historical and provisional results require explicit selection.
Every result includes scope, conditions, assessment states, provenance references,
source freshness, and reasons for abstention or withdrawal. Exports carry a schema
version and a manifest identifying the exact source and processing versions.

## Evaluation and observability

Implement the product acceptance criteria with a labeled corpus covering positive
relations, hard negatives, ambiguity, and unsupported cases. Keep held-out contract
templates or event groups separate from development examples to reduce leakage.
Record independent labels and disagreement resolution. Score automatic proposals
before case-specific review so adjudication cannot inflate automatic precision.

Regression cases include threshold equality boundaries, unit conversion, different
sources/times/vintages, missing rules, exceptional payouts, inconsistent premises,
solver timeouts, source/template revisions, interrupted jobs, and idempotent replay.
Include a job completing after its input or approval was invalidated, probability
constraints feasible only without zero-to-one bounds, and markup in source text.
Compare incremental output with a full rebuild under the same stored inputs and
configuration. Report syntax and semantic accuracy separately from relation quality.

Track abstention reasons, pending review, refresh age/failures, stale assertions,
job retries, proof failures, contradictions, latency, peak memory, and cost per
contract. The local status report must make incomplete processing visible.

## External semantic references

- [Z3 validity and satisfiability](https://microsoft.github.io/z3guide/docs/logic/propositional-logic/)
  explains the scope of proofs over encoded formulas.
- [Polymarket resolution](https://docs.polymarket.com/concepts/resolution) documents
  governing rules, clarifications, and exceptional resolution. Venue documentation
  informs adapters; captured contract-specific rules remain part of each claim's evidence.
