# OddsFox — Product Specification V1

OddsFox is free and open-source software (FOSS) under the [MIT License](LICENSE).
Repository-wide licensing scope is documented in the [README](README.md#license).

## Purpose and document ownership

OddsFox is a local, open-source compiler and verifier for prediction-market
contracts. It produces traceable formal interpretations and verified relationships
so researchers can compare outcomes and settlement rules with inspectable evidence.

This document owns the user, workflow, scope, and acceptance criteria.
[Technical specification](tech_spec_v1.md) owns semantics and system behavior.
[Technology stack](tech_stack_v1.md) owns implementation and deployment choices.
These documents describe the same V1; none describes a separate hosted edition.

## First user and recurring task

The first user is a researcher maintaining a comparable set of prediction-market
contracts. Today, the task requires reading individual resolution rules, checking
whether superficially similar contracts actually refer to the same observation,
and repeating the review when rules change.

OddsFox should reduce that review effort while making consequential differences
harder to miss. Developers consuming its structured results are a secondary
audience. The semantic intermediate representation (IR) is a first-class public
product artifact: the primary compiler output and stable interface between
interpretation, reasoning, evaluation, and downstream consumers. Its versioned,
language-independent canonical JSON contract is owned by the
[technical specification](tech_spec_v1.md#public-v1-semantic-ir).

## User workflow and deliverable

1. Browse automatically discovered qualifying events across Kalshi and Polymarket International; retain manual capture/import for research.
2. Inspect a local comparison report containing candidate relationships,
   settlement differences, unresolved questions, and source evidence.
3. Review uncertain interpretations and record corrections or approval.
4. Export versioned semantic IR and accepted relationships with their explicit
   assumptions for downstream research, or retrieve them through the local API.
5. Refresh the collection and inspect which prior conclusions were withdrawn or
   replaced following source changes.

Every comparison explains what is related, why, which contract versions it uses,
which assumptions apply, and what remains uncertain. Declining to establish a
relationship is a useful result, not a processing failure to hide.

## Worked example

Consider two hypothetical contracts using the same named BTC/USD reference value
at the same instant:

- A: the value is at least USD 150,000.
- B: the value is at least USD 100,000.

OddsFox can establish that A's observation predicate implies B's. The report must
also assess whether their settlement rules preserve that relationship.

A third contract using a different reference source, or the day's maximum instead
of the single observation, must not inherit the relationship merely because its
title looks similar. The report should explain the mismatch or request review.
All examples here are illustrative, not claims about listed markets.

## V1 scope

Formally verify one contract family: binary numeric threshold questions about a single
scalar observation at a specified instant, with an identifiable source, unit,
comparison operator, and resolution policy. Discover equivalence, implication,
mutual exclusion, and complement relationships within that family.

Use a curated, independently labeled corpus for formal-verification evaluation across venues. Source availability and actual
contract rules determine eligibility; do not force contracts into the supported
family to meet a coverage target. Include near-matches and unsupported contracts
in evaluation so that abstention and mismatch detection are measured.

V1 provides a local report, adjudication workflow, structured export, and local
API. It publishes symbolic probability constraints derived from verified semantics;
these are not market-price estimates. It does not ingest prices or adjust numerical
probability estimates.

Defer formal reasoning over elections as a general domain, categorical partitions, n-ary relationships,
interval maxima, temporal containment, causal inference, forecasting, trade
execution, graph visualization, and a hosted multi-user service. Extend scope
only after the first family's interpretation and revision behavior are validated.
Global probability coherence, larger logical hypergraphs, categorical partitions,
temporal containment, and trading applications are possible downstream extensions
enabled by the IR, not V1 deliverables.

## Default discovery and explanation experience

The home view includes each venue's events with at least one market open for
trading and lifetime volume strictly above USD 100,000. Qualification is per venue
event, not pooled across exchanges. Include every active child market after event
qualification. Lifetime event totals include closed children that contributed volume.
Label venue volume conventions; face-value contract notional and cash turnover are
not interchangeable measures. Unknown totals belong in a separate visible queue.

Cover all publicly discoverable categories and listed combinations. Explain rules,
outcomes, timing, sources, exceptions and combination legs using cited local-model
outputs. Suggest cross-venue matches without automatically merging identities.
These explanations are unreviewed and do not broaden the formal-verification family.
Missing governing documents and ambiguities must stay visible.

Refresh on startup and every 15 minutes while the local app runs, with manual
refresh and pause controls. Show per-venue coverage/freshness, partial failures,
explanation progress and formal-verification progress separately. Retain the last
successful data during outages. Never label an incomplete scan as complete.

No model download or cloud inference occurs implicitly. Discovery must work without
a model; the app explains how to configure one. Local processing is progressive,
with no promise that all explanations finish within a refresh interval. Human
review remains required for accepted formal claims.

## Trust promise

OddsFox reports separately:

- How the contract interpretation was established and reviewed.
- Whether the formal relationship follows from the encoded premises.
- Whether settlement rules support the same conclusion, introduce conditions,
  differ materially, or remain unresolved.

A formal proof is conditional on an accurate interpretation and stated premises;
it is not independent proof that a natural-language contract was understood
correctly. No aggregate confidence score may conceal these distinctions.

Provisional results remain visibly separate from accepted exports. Missing rules,
ambiguous sources, and unsupported settlement behavior must produce a qualified
result or abstention. Source changes withdraw affected current conclusions while
preserving their history and evidence.

## Acceptance criteria

Freeze the benchmark, labeling guide, evaluation procedure, and versioned automatic
acceptance policy before the release evaluation. Persist the policy version and
complete configuration with each run. Report denominators, sample sizes,
uncertainty intervals, and results by venue and contract template; do not describe
targets as achieved without data.

| Measure | V1 requirement |
| --- | --- |
| Relationship precision | Target at least 99% correctness of automatic proposals selected by a frozen acceptance policy, against independent human labels. Score before case-specific approval, rejection, or correction; count erroneous proposals even if review later catches them. Evaluate the complete claim, including scope and conditions. |
| Coverage and abstention | Report supported contracts / all sampled contracts, completed interpretations / eligible contracts, relationship recall against labeled relationships, and abstention reasons. Publish these alongside precision; abstaining on everything is not success. |
| Stage-level evaluation | Report field extraction, canonical resolution, settlement interpretation and compatibility classification, relationship precision/recall, automatic acceptance coverage, and abstention using the [metric definitions](tech_spec_v1.md#benchmark-metric-contract). Component metrics supplement the end-to-end gate and identify error sources. |
| Misleading similarities | Include distinct sources, units, observation times, strict versus inclusive thresholds, and settlement exceptions. No known false equivalence may remain in the release regression suite. |
| Review effort | Demonstrate lower median time to a correct comparison against manual review on paired tasks, including correction time. Report task count and remaining errors. |
| Provenance and revisions | Every accepted assertion has complete evidence. All revision fixtures withdraw affected assertions and prevent stale assertions from appearing as current. |
| Cross-platform value | Demonstrate at least one human-validated relationship automatically derived across venues, plus a correctly explained near-match rejection. If the corpus contains no eligible example, this gate remains unmet. |

Syntax validity, semantic interpretation accuracy, processing latency, memory, and
cost are supporting diagnostics. They do not substitute for claim correctness or
demonstrated review value. Small samples must not be presented as establishing a
population-wide error rate.

## Durable value

The accumulated assets are a reviewed contract corpus, reusable interpretations,
canonical observation definitions, correction data, and versioned evidence.
Their defensibility depends on demonstrated coverage, precision, and reduced
review effort; the choice of solver or graph representation alone is not a moat.
