# Reproducible evaluation

The [product specification](../product_spec_v1.md#acceptance-criteria) owns release
gates; the [technical specification](../tech_spec_v1.md#benchmark-metric-contract)
owns metric meaning. The examples here are synthetic development fixtures.

```sh
uv run oddsfox benchmark examples/benchmark.json examples/run.json --output /tmp/oddsfox-metrics.json
```

The output must be a new file. Benchmarks and runs are content-hashed; changed
labels, policy or configuration produce a different run rather than an overwrite.
Retain the original inputs alongside the output. No human review may retroactively
alter the proposals being scored.

## Benchmark input

Provide `benchmark_id`, `revision`, `label_version`, `labeling_guide`, `split`, and
an honest `independent_human_labels` boolean. Freeze held-out event groups/templates
before model selection or acceptance-policy tuning.

- `contracts`: records with immutable contract-version `id`, `venue`, `template`
  and human-labeled `eligible`. Include hard negatives and unsupported inputs.
- `comparisons`: the fixed comparison universe, each with `a`, `b`, `scope` and
  explicit `conditions`. The pair is unordered; implication direction belongs
  to a claim. A batch supports at most 250 contracts.
- `gold_claims`: positive complete claims with `a`, `b`, `relation`, `scope` and
  `conditions`. Symmetric relations normalize operand order; implication preserves
  direction. Claims outside the comparison universe are rejected.
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
the published IR rules before freezing them. Store human label provenance and
disagreement resolutions with the corpus; the scorer does not manufacture them.

## Original run input

`benchmark_hash` is `oddsfox.ir.fingerprint(benchmark)`. Include the complete
`pipeline_configuration` and set `scored_before_case_review` only for the preserved
original automatic output. Store `proposals`, `stage_outputs`, and `outcomes`.

The complete acceptance policy contains `version`, `allowed_scopes`,
`allowed_settlement_states`, `require_resolved`, and `normalization`.
Selection also requires a proven result and two supported interpretation
assessments. This benchmark selector never grants semantic approval in the product.

Stage outputs use the label's `id`, `stage`, and `mode` with a `values` map.
Outcomes record contract `id`, stage (`interpret`, `resolve`, `compare`), `state`
(`complete`, `abstained`, `failed`, `unsupported`), and an abstention `reason` when
applicable. Each sampled contract has at most one outcome per stage; unknown
contract IDs, stages and states are rejected. Missing predictions remain errors; a missing field is different from
a correctly predicted null.

The report separates stage metrics, all-proposal and selected precision/recall,
coverage, abstentions/failures, counts, Wilson intervals, and venue/template
breakdowns. Complete claims are deduplicated. Implication/equivalence closure is
computed separately for all and selected proposals over the full frozen universe,
then filtered for breakdowns. Exclusion and complement are not transitively
chained. Settlement conditions must identify the exact contract versions they
refer to; a condition about pair A/B is distinct from one about B/C.
Zero denominators produce null scores and explicit counts.

Metric definition `oddsfox-metrics/2` reports coverage and outcome metrics both
overall and by venue/template. Unsupported, failed, abstained, reason and missing
counts cover all sampled contracts, including human-ineligible inputs. The
abstention `rate` counts eligible abstentions over eligible contracts;
`eligible_missing` separately reports eligible inputs without an outcome.
`sampled` identifies the population for the other counts. Completion coverage
counts completed interpretations over eligible contracts.

For paired review experiments, record `paired_review_tasks` with `manual_seconds`,
`assisted_seconds`, `manual_correct`, and `assisted_correct`; times must include
corrections. Record `review_times_include_corrections` explicitly. Preserve task
assignment/order and human evidence in the research dataset. Cross-venue and
near-match human validation flags require that evidence; passing synthetic tests
does not set them. The scorer reports gate components, not a release certificate.
