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

## Public V1 semantic IR

The primary output of contract compilation is `SemanticIR`, a public serialized
contract containing `Observation`, `Predicate`, and `SettlementSemantics`.
Reasoning, evaluation, and exports consume this versioned boundary rather than
depending on internal Python objects. Interpretation records wrap the IR with
assessment states, review history, and processing metadata; an IR export by itself
does not imply semantic approval or a proof.

The exact V1 field inventory follows. Every listed key is required; `T?` means
`T` or JSON `null`, not an omitted key. `string` means a nonempty string.

```text
SemanticIR
  schema_version: "1.0.0"
  compiler_version: string
  contract_version_id: string
  observation: Observation
  predicate: Predicate
  settlement_semantics: SettlementSemantics
  field_evidence: map<JSONPointer, FieldEvidence>

Observation
  canonical_observation_id: string?
  canonical_observation_version: string?
  quantity: string?
  source: string?
  instrument_or_series: string?
  unit: string?
  timestamp: Instant?
  timezone: string?
  measurement_method: string?
  precision: Decimal?
  revision_policy: string?

Predicate
  observation_ref: "/observation"
  comparator: ("GT" | "GTE" | "LT" | "LTE")?
  threshold: Decimal?

SettlementSemantics
  payout_mapping: {unit: string?, outcomes: Payout[]}?
  resolution_source: string?
  cutoff: Instant?
  rounding: string?
  missing_data_policy: string?
  cancellation_policy: string?
  exceptional_outcome_policy: string?
  dispute_policy: string?
  clarification_policy: string?

Payout
  outcome_id: string
  if_true: Decimal?
  if_false: Decimal?

FieldEvidence
  source_spans: SourceSpan[]
  derivation_ref: string?

SourceSpan
  artifact_id: string
  start: integer
  end: integer
```

`Decimal` is an exact decimal JSON string, never a binary floating-point number.
Use plain base-10 notation without exponent, plus sign, redundant leading zeros,
or trailing fractional zeros; zero is `"0"`, never negative zero. `precision`
expresses a positive measurement increment in the observation unit. `Instant` is
a UTC timestamp of the form `YYYY-MM-DDTHH:MM:SS[.fraction]Z`, with redundant
fractional zeros removed. Preserve the source timezone separately as an IANA name
or explicit numeric offset; unresolved timezone information must not be guessed.

`payout_mapping` describes the two native outcome IDs and ordinary true/false
predicate branches in its stated payout unit. A non-null mapping contains exactly
two rows with distinct IDs matching the captured contract's outcomes. Sort outcomes
by Unicode-code-point order of those IDs, independently of locale.
Exceptional branches remain in the separate policy fields. Policy and method
strings are evidence-backed descriptions, not an executable language: only
reviewed rules/templates can translate supported meanings into solver premises.
A known description outside the reasoner's support remains unsupported there.

Use [JSON Pointer](https://www.rfc-editor.org/rfc/rfc6901) keys in `field_evidence`
to attach provenance to any semantic field or nested payout value. Each source
span references an immutable UTF-8 text artifact, with zero-based, half-open
Unicode-code-point offsets satisfying `0 <= start < end <= text length` and
bounded by `2^53 - 1`. Artifact metadata links to the captured contract/source
version. A derivation reference identifies the versioned normalization rule or
review record; it does not replace the underlying source evidence. Sort and
deduplicate spans by `(artifact_id, start, end)`. Explicitly unknown fields can
have empty evidence; populated semantic values must have source spans, directly
or through a resolvable derivation record. Structural references and schema/compiler
IDs use record-level provenance rather than invented contract quotations.

Finalize payout-array order before assigning evidence pointers. A producer that
reorders existing rows must remap their pointers in the same transformation and
validate that each span still supports its target field. Readers reject unsorted
IR arrays rather than silently sorting them: JCS preserves array order. Evidence
pointers must resolve within the semantic objects of this IR, never into metadata
or `field_evidence` itself. For span ordering, compare artifact IDs by Unicode
code points and offsets numerically.

Canonical JSON export uses UTF-8 [JCS (RFC 8785)](https://www.rfc-editor.org/rfc/rfc8785)
after the field normalizations above. Reject duplicate keys, unknown fields for
the declared schema version, invalid values, and dangling references. Persist
schema and compiler versions with every interpretation. Store the SHA-256 digest
of the canonical IR bytes in interpretation metadata and reference it from proofs
and exports; the digest is outside the hashed payload. Publish the corresponding
JSON Schema; serialized consumers must not require Pydantic or Python.

Schema versions are explicit: incompatible structure or meaning changes require
a new major version and a documented migration/recompilation transition. Preserve
old artifacts and invalidate affected approvals and proofs; never reinterpret an
old payload under a new schema. Consumers reject unsupported schema versions
rather than silently dropping fields. Conformance fixtures must cover canonical
round trips, exact decimals, explicit nulls, evidence references, and transitions.
Include reordered payout rows with remapped evidence, non-ASCII IDs, duplicate
outcome IDs, and evidence pointers that resolve to the wrong field.

## Semantic invariants and canonical identity

Observation time and resolution cutoff are distinct fields. V1 has no implicit
aggregation: an interval maximum is unsupported. Unknown fields remain unknown;
do not supply convenient defaults for dates, sources, or exceptional outcomes.
Raw model responses may be stored as attempts, but only schema-valid objects can
become interpretations.

V1 carries exactly one observation and one threshold predicate, with no intervals,
categorical partitions, or multi-market expressions. The local observation
reference remains valid before canonical resolution; canonical ID and version are
both null until resolved, then identify the exact registry definition. Partial IR
can be serialized for review, but unresolved required semantics cannot pass the
existing interpretation or publication gates.

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
They are derived outputs of contract compilation and verification. Global
probability coherence and larger constraint graphs are downstream applications;
V1's bounded consistency checks do not promise either capability.

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
Refresh failure preserves historical evidence but must surface stale freshness
rather than claim a successful refresh.

## Consumer contract

The local report and API expose contract versions, interpretations containing
versioned semantic IR, comparisons, review decisions, accepted assertions,
symbolic constraints, and revision history.
Review actions are explicit writes; querying or exporting never implies approval.
Treat imported contract text and model output as untrusted data. Escape them in
HTML reports, allow only safe external-link schemes, and never interpret their
contents as executable markup or instructions authorizing a review action.

Default accepted queries return only current accepted results from a consistent
database snapshot. Historical and provisional results require explicit selection.
Every result includes scope, conditions, assessment states, provenance references,
source freshness, and reasons for abstention or withdrawal. Exports carry a schema
version and a manifest identifying the exact source and processing versions.
The IR's own schema version is independent of the export envelope version.

## Evaluation and observability

Implement the product acceptance criteria with a labeled corpus covering positive
relations, hard negatives, ambiguity, and unsupported cases. Keep held-out contract
templates or event groups separate from development examples to reduce leakage.
Record independent labels and disagreement resolution. Score automatic proposals
before case-specific review so adjudication cannot inflate automatic precision.
Evaluate both the full pipeline and each stage with gold upstream inputs to
distinguish propagated errors from errors introduced by that stage. Keep these
results separate; component scores cannot replace the end-to-end release gate.

### Benchmark metric contract

Freeze the eligible corpus, field labels, observation identities, candidate
comparisons (including hard negatives), and reference relationships before scoring.
Identify each claim by its contract-version operands, relation type, direction,
scope, and conditions normalized under the frozen labeling guide. Deduplicate
claims before calculating precision, recall, or acceptance coverage; repeated
jobs, proofs, and derivation paths do not create additional observations. Use a
canonical operand order for symmetric relations and preserve implication direction.
Score both precision and recall over the same fixed comparison set and closure
rules, within a common scope and conditioning context, so storage choices do not
change either score. Compute closure separately
for all proposals and policy-selected proposals; do not credit the selected set
with paths that use unselected claims. Only equivalence and implication are
transitive; exclusion and complement must not be transitively chained. Report
outputs outside the scoring set separately as unscored. Missing predictions on
known labels
are errors or false negatives, not removals from the denominator. Report a zero
denominator as not applicable with its count, never as perfect accuracy.

| Metric | Definition and required breakdown |
| --- | --- |
| IR field extraction accuracy | Correct observation/predicate field values divided by labeled fields, after frozen field-specific normalization. Report per-field and whole-record accuracy, including explicit unknown labels; score canonical IDs separately below. |
| Canonical observation resolution accuracy | Correct canonical ID/version assignments or correctly unresolved decisions divided by labeled observations. Also report precision among resolved assignments and resolution coverage to expose false merges and excessive abstention. |
| Settlement-semantics interpretation accuracy | Correct payout and policy meanings divided by labeled settlement fields under the frozen labeling guide, not literal wording equality; report each field and whole-record accuracy, including missing-data, cancellation, and exceptional policies. |
| Settlement-compatibility classification accuracy | Correct compatibility class and required conditions divided by labeled comparisons. Report a confusion matrix across `COMPATIBLE`, `CONDITIONAL`, `DIFFERENT`, and `UNKNOWN`. |
| Relationship precision | Correct complete claims divided by emitted claims; report both all proposals and the subset selected by the frozen automatic acceptance policy. The existing at-least-99% end-to-end target applies to the selected subset before case-specific human intervention. |
| Relationship recall | Correct claims recovered divided by gold positive claims in the fixed comparison set; report both all-proposal and acceptance-selected recall, including missed candidates. |
| Automatic acceptance coverage | Fixed candidate comparisons with at least one policy-selected claim divided by all fixed candidate comparisons; also report selected claims / emitted claims. Count incorrect selections in coverage and as errors in precision. |
| Abstention rate and reasons | Explicit abstentions divided by eligible inputs at each stage, split by reason and venue/template. Report failures, missing outputs, and unsupported inputs separately; they remain in applicable accuracy/recall denominators. |

Persist benchmark ID/revision, label and metric-definition versions, full pipeline
configuration, and `acceptance_policy_version` plus its complete configuration/hash
with every run. Freeze policy filters and thresholds before held-out evaluation;
changing them creates a distinct run, never an overwrite. This evaluation selector
does not bypass the semantic-review requirement for actual accepted publication.
Retain raw proposals, policy selections, stage outcomes, and later review decisions
so corrections cannot retrospectively improve the automatic score. Apply the
product specification's counts, uncertainty intervals, and venue/template
breakdowns to these metrics as well as to the end-to-end results.

Regression cases include threshold equality boundaries, unit conversion, different
sources/times/vintages, missing rules, exceptional payouts, inconsistent premises,
solver timeouts, source/template revisions, interrupted jobs, and idempotent replay.
Include a job completing after its input or approval was invalidated, probability
constraints feasible only without zero-to-one bounds, and markup in source text.
Benchmark fixtures must show that duplicate claims leave metrics unchanged and
unselected or non-transitive relation paths cannot inflate selected-set recall.
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
