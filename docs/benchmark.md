# Reproducible evaluation

The [product specification](../product_spec_v1.md#acceptance-criteria) owns release
gates; the [technical specification](../tech_spec_v1.md#benchmark-metric-contract)
owns metric meaning. The examples here are synthetic development fixtures.

```sh
uv run oddsfox benchmark examples/benchmark.json examples/run.json --output /tmp/oddsfox-metrics.json
uv run oddsfox benchmark examples/benchmark-v4.json examples/run-v4.json --output /tmp/oddsfox-metrics-v4.json
```

The output must be a new file. Benchmarks and runs are content-hashed; changed
labels, policy or configuration produce a different run rather than an overwrite.
Retain the original inputs alongside the output. No later human review may retroactively
alter the proposals being scored.

## Benchmark input

Provide `benchmark_id`, `revision`, `label_version`, `labeling_guide`, `split`, and
an honest `independent_human_labels` boolean. Freeze held-out event groups/templates
before model selection or acceptance-policy tuning.

Metric definition `oddsfox-metrics/3` is the synthetic-fixture path. It may record
paired review diagnostics; it does not certify independent human release evidence.

Metric definition `oddsfox-metrics/4` is the local unanimous-consensus path. It
requires `label_source: local_unanimous_consensus`, panel and protocol provenance,
and `independent_human_labels: false`. Model ballots cannot set
`independent_human_labels`, `cross_venue_human_validation`, or
`near_match_human_validation`. v4 reports **agreement** with the evaluator panel,
not correctness or human truth.

The evaluator and producer panel manifests identify distinct operator-reviewed
lineages and weights-only revisions. Each model directory has an adjacent,
operator-controlled `<directory>.oddsfox-lineage.json` record whose digest must
match the actual safetensors and whose architecture must match model configuration.
Changing weights, lineage metadata, prompts, or configuration creates different
provenance and invalidates reuse.

- `contracts`: records with immutable contract-version `id`, `venue`, `template`
  and `eligible`. Include hard negatives and unsupported inputs.
- `comparisons`: the fixed comparison universe, each with `a`, `b`, `scope` and
  explicit `conditions`. The pair is unordered; implication direction belongs
  to a claim. A batch supports at most 250 contracts. Candidate generation
  determines what is scored, never the gold relation.
- `pair_labels`: one explicit evaluator relationship for every unanimously labeled
  comparison, including `NONE` and `NEAR_MATCH`. `pair_outcomes` covers every
  frozen comparison as `complete` or `abstained`; only labeled pairs enter the
  relationship denominator. The report exposes pair-label coverage separately.
- `gold_claims`: positive complete claims with `a`, `b`, `relation`, `scope` and
  `conditions`. Symmetric relations normalize operand order; implication preserves
  direction. Claims outside the comparison universe are rejected. v4 gold is
  included only when every evaluator ballot is byte-equivalent after
  canonicalization; disagreements and invalid/missing ballots are abstentions
  in the coverage denominator, never negative labels.
- `stage_labels`: records with `id`, `stage`, `mode`, and an `expected` field map.
  Stages are `ir_fields`, `canonical_resolution`, `settlement_fields`, and
  `settlement_compatibility`; modes are `pipeline` and `gold-upstream`.
  Each `(id, stage, mode)` label key must be unique; duplicate labels are rejected.
  Comparison-stage labels also carry `operands` for venue/template breakdowns.
  Label exact canonical ID **and version**, payout meanings, compatibility state
  **and conditions**, and explicit unknown values. Include all expected fields.

The frozen `exact-string-set/1` guide normalizes conditions by trimming/collapsing
whitespace and sorting/deduplicating the exact strings. It does not infer semantic
equivalence between differently worded conditions. Normalize field labels under
the published IR rules before freezing them. Store ballot provenance and
disagreement records with the corpus; the scorer does not manufacture them.
Ballot citations must name captured artifacts and valid, nonempty Unicode-offset
ranges. Retain raw responses and diagnostics for contract and pair latency, total
latency, peak memory, failures, and retries.

Runtime validation bundles include `judge-evidence.json`, limited to the exact
evaluator and producer label sets used by that run. It contains their configs,
ballots, dependencies, raw responses, and cited source artifacts with content
hashes. The run binds this file by `judge_evidence_hash` and
`evidence_mode: immutable-local-store`. Hand-authored development examples use
`evidence_mode: synthetic-fixture`; that mode can exercise the scorer but can
never satisfy the provenance-invalidation release gate.

Complete two-venue scans with zero event errors are required before a corpus freeze.
If more than 250 contracts exist, use a frozen deterministic stratified sample by
venue, template/category, and a non-labeling lexical family stratum. Record full
inventory counts and the selection algorithm. Insufficient inventory yields an unmet
gate, not relaxed criteria.

## Original run input

`benchmark_hash` is `oddsfox.ir.fingerprint(benchmark)`. Include the complete
`pipeline_configuration` and set `scored_before_case_review` only for the preserved
original automatic output. Store `proposals`, `stage_outputs`, `outcomes`, complete
producer `pair_outcomes`, and producer diagnostics.

The complete acceptance policy contains `version`, `allowed_scopes`,
`allowed_settlement_states`, `require_resolved`, and `normalization`.
The v3 solver selector also requires a proven result and two supported
interpretation assessments. Metrics v4 producer runs use
`consensus-agreement/1` with `require_resolved: false`: selected agreement is
in-universe producer pair claims versus evaluator labels, not fabricated solver
proofs. This benchmark selector never grants semantic approval in the product.

Stage outputs use the label's `id`, `stage`, and `mode` with a `values` map.
Outcomes record contract `id`, stage (`interpret`, `resolve`, `compare`), `state`
(`complete`, `abstained`, `failed`, `unsupported`), and an abstention `reason` when
applicable. Each sampled contract has at most one outcome per stage; unknown
contract IDs, stages and states are rejected. Missing predictions remain errors; a missing field is different from
a correctly predicted null.

The report separates stage metrics, all-proposal and selected agreement/recall,
coverage, abstentions/failures, counts, Wilson intervals, and venue/template
breakdowns. Complete claims are deduplicated. Implication/equivalence closure is
computed separately for all and selected proposals over the full frozen universe,
then filtered for breakdowns. Exclusion and complement are not transitively
chained. Settlement conditions must identify the exact contract versions they
refer to; a condition about pair A/B is distinct from one about B/C.
Zero denominators produce null scores and explicit counts.

Metric definition `oddsfox-metrics/3` reports coverage and outcome metrics both
overall and by venue/template. Unsupported, failed, abstained, reason and missing
counts cover all sampled contracts, including ineligible inputs. The
abstention `rate` counts eligible abstentions over eligible contracts;
`eligible_missing` separately reports eligible inputs without an outcome.
`sampled` identifies the population for the other counts. Completion coverage
counts completed interpretations over eligible contracts. Version 3 also applies
the frozen condition-set normalization to settlement-compatibility stage scores:
permutations, duplicate strings and whitespace differences are equivalent; missing
or genuinely different conditions remain incorrect.

Metric definition `oddsfox-metrics/4` adds evaluator-label coverage, label
abstentions, panel/protocol provenance, and machine-verifiable consensus gates:
complete zero-error scans, sample size, both-venue representation, label coverage
at least 0.80, selected agreement at least 0.99 with Wilson lower bound at least
0.95, one unanimous evaluator-panel cross-venue relationship, one unanimous
evaluator-panel near-match rejection, zero accepted known false equivalences, and
passing provenance/invalidation checks. The two relationship gates are frozen
corpus evidence, not producer self-report. Automation diagnostics replace
review-time claims.

Evidence bundles are assembled in a sibling staging directory, hash-verified,
fsynced, and renamed into place. A failed or repeated publication does not expose
a partial bundle or overwrite an existing one.

For paired review experiments, record `paired_review_tasks` with `manual_seconds`,
`assisted_seconds`, `manual_correct`, and `assisted_correct`; times must include
corrections. Record `review_times_include_corrections` explicitly. These remain
optional non-release diagnostics. Cross-venue and near-match human validation
flags require independent human evidence; consensus ballots cannot set them.
The scorer reports gate components, not a release certificate.
