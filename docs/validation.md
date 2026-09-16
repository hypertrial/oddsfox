# Implementation validation

This record separates implemented software from the product release gates.
The specifications remain the acceptance contract.

## Target environment

Validation started on 2026-09-08 on Apple Silicon with 32 GiB RAM, macOS 26.6.2,
and Python 3.14.7. `uv.lock` pins the resolved runtime and development tools.
The full dependency set, including cvc5, DuckDB, MLX-LM and Outlines, installed;
core and Outlines imports passed. Model inference is a separate check.

## Current verification policy (2026-09-16)

GitHub Actions is not used because hosted Actions are unavailable to the free
organization. The failed run for commit `e7f66dc` did not start a runner or execute
repository checks. A fresh clean-checkout `scripts/verify` run is the authoritative software
completion gate. The latest verified baseline passed Ruff, formatting, ty,
public-schema drift, the package build and 269 tests, with two upstream dependency
deprecation warnings.
Historical CI results below remain execution records for their exact revisions;
they are not the current completion mechanism.

## Software checks

The automated suite covers canonical IR, exact decimals, explicit nulls, source
evidence, unsupported schemas, strict/inclusive threshold boundaries, differential
cvc5 checks, review gates, dependency revisions, stale jobs, crash recovery,
backup restoration, local API security, captured markup, exports and benchmark
deduplication/closure. The completion wrapper also runs lint, formatting, type
checks, schema drift validation and a package build. In an earlier historical
validation pass, the local completion gate passed with 90 tests and two upstream
test-client deprecation warnings. Independent architecture, security/recovery and
final verification reviews for that revision passed after their findings were fixed.

Independent reviews exposed and prompted regression coverage for incorrect job
operand binding, configuration rollback, mixed-comparator candidate selection,
metric denominators and breakdown closure, incomplete backups, final-attempt crash
recovery, public schema constraints and missing near-match explanations.
A subsequent independent repository review fixed pair-specific settlement
conditions, manual A→B→A correction revisions, duplicate stage-label rejection,
and sampled-versus-eligible outcome counts with venue/template breakdowns.
All four fixes passed independent re-review and the full local completion gate.
Final export and recovery review also added regression coverage for missing
quarantined proofs and versioned Parquet metadata, including empty exports.

The synthetic report was exercised in the browser at narrow and desktop widths.
Source excerpts and near-match differences rendered correctly. Withdrawing a
synthetic approval removed both dependent accepted claims; approving again and
publishing restored them. These are software checks, not human quality labels.

## Verified audit fixes

The follow-up bug-fix gate passes 108 tests, including a Node built-in test that
executes the shipped report JavaScript and checks that a loaded draft stays in
the visible editor and can be saved. No npm dependencies are required.

Regressions cover normalization-rule withdrawal after canonical rebinding,
unknown/wrong-kind/stale rules and withdrawal during insertion; reviewed endpoint
comparisons with an unreviewed intermediate; explicit nonadjacent settlement
pairs and the 16,000-pair ceiling; equivalent versus missing/different benchmark
conditions; and malformed Kalshi responses that retain failed freshness evidence
while processing the rest of a batch. Independent semantic, security and metric
reviews passed. These checks do not supply human product-release evidence.

## Real capture smoke

The read-only Polymarket Gamma and Kalshi market endpoints returned successful
responses. Exact individual payloads from both adapters were captured locally.
The smoke exposed decimal points in Kalshi native tickers; the adapter and its
regression test cover those identifiers.

The sampled Kalshi BTC threshold rules use a sixty-second average. The current
instantaneous family must not silently reinterpret these as point observations.
Other sampled election and multi-event markets are outside V1. This small sample
does not establish that no eligible cross-venue contracts exist.

## Local model smoke

The smoke used `mlx-community/Qwen3-4B-Instruct-2507-4bit`, revision
`50d427756c6b1b2fe0c0a10f67fbda1fc8e82c1b`, with the published weight SHA-256
verified before loading. MLX-LM 0.31.3 and Outlines 1.3.3 loaded the local model.
Weights and raw run artifacts remain outside version control.

Direct expansion of all public schema size ceilings stalled in Outlines grammar
compilation and was interrupted. The implementation now uses a compact grammar
while applying every public restriction in mandatory validation afterward.
Three constrained synthetic runs completed generation with recorded generation
times of 39.97, 129.97 and 140.94 seconds, and MLX peaks of 3.44, 3.75 and 3.64 GB.
These timings exclude initial job preparation and model-file hashing.

Those responses failed strict evidence validation and were retained as failed
attempts. Finite citation choices fixed pointer and offset errors; further trials
exposed omitted evidence and a three-minute timeout. The final decoder requires
known evidence slots and chooses only captured line spans. Empty known slots
under a null payout are omitted from public IR; nonempty dangling evidence remains
invalid. Original responses and processed inputs have separate persisted links,
and replay uses the recorded normalization version.

With the final decoder and recorded defaults of 8,192 tokens, 300 seconds and an
8 GiB MLX allocation limit, a synthetic compilation passed all IR/evidence checks:
26 evidence fields, 179.98 seconds, and 3.75 GB peak MLX memory. Its populated
semantic fields matched the synthetic fixture. Replaying its original response
under an explicitly synthetic reviewed observation produced a supported,
unapproved interpretation; zero assertions were accepted. Replay preserved the
original response link and dataset integrity passed. This establishes a working
technical path, not real-contract accuracy or independent human precision.

## Release evidence still required

- Frozen real corpus with local unanimous evaluator-panel labels, held-out
  templates and a measured acceptance policy meeting the documented agreement
  target, Wilson bound, coverage, and provenance gates.
- Unanimous evaluator-panel cross-venue relationship and unanimous evaluator-panel
  near-match rejection in that frozen corpus (not producer self-report).
- Complete zero-error two-venue scans before freeze; insufficient inventory leaves
  the gate unmet.
- Local six-model panel resource results on that representative corpus.

Synthetic demo approvals are explicitly labeled as such. Neither the demo nor
the scorer manufactures independent human evidence or consensus evidence. As a
historical record, the first pushed implementation run passed its 90 tests but
failed type checking because optional MLX/Outlines imports were unresolved in the
core-only environment. Optional backends now load through the standard-library
module loader only when a configured model job executes. The corrected local
core-only completion gate passed 109 tests and Linux-target type checking. Current
verification policy and results are recorded above; no remote rerun is required.

## Historical three-venue implementation (2026-09-08)

The default event browser, public adapters, snapshot migration, durable processing,
local explanation interface and provisional matching are implemented. The expanded
completion gate passes 140 tests, Ruff lint/format, ty, unchanged Semantic IR
1.0.0 schema generation and the package build. Tests include 300 distinct discovered
events across overlapping pages, 251 captured contracts, comparisons crossing the
250-contract boundary, checkpoint recovery, bounded rate-limit retries, partial
outages, unreadable PDF preservation, migration/backup restoration and suggestion
candidate recovery/reversion. Independent architecture, data and document-security
reviews found defects that were fixed and retested. This is software evidence,
not the independent human acceptance evidence described above.

Browser checks used a separate temporary dataset: volume ordering, venue filtering,
event details, unknown-volume empty state, model-setup guidance and authenticated
pause were exercised. The 390-pixel viewport had no horizontal overflow; the final
reloaded desktop page reported no browser console errors. Frontend regression
checks exercise pagination, filter reset, authenticated pause and the legacy editor.

### Actual discovery coverage

Read-only live sampling retrieved the first 100 events from International and
Kalshi, then fully processed two representative events from each. International
samples included Kraken IPO (reported USD 1,607,032.853199; four active children)
and Macron out (USD 2,151,795.36855; one active child). Kalshi samples included
Elon Musk visiting Mars (USD 118,572.210000 face-value notional, qualifying) and the
next NATO secretary-general (USD 6,234.970000, below threshold). Current/historical
Kalshi membership and linked governing material were exercised; dataset integrity
passed. These were bounded samples, **not completed universe scans**.

Polymarket US returned HTTP 403 from this environment during implementation
validation. Its public adapter is covered by fixtures, including missing lifetime
volume, but current live US coverage remains unverified. No liquidity, open interest
or book statistic substitutes for missing lifetime turnover. US's separately
[documented combo API](https://docs.polymarket.us/api-reference/combos/overview)
requires beta-enabled authenticated access and an exact symbol; its
[lookup contract](https://docs.polymarket.us/api-reference/combos/get-combos) is
not a public paginated combo-universe feed. That adapter has since been removed; this paragraph records the earlier validation outcome.

### Actual explanation quality

The already installed Qwen3-4B-Instruct-2507-4bit model described above was run
locally with production `explain-captured-lines/3`, temperature zero, 4,096 tokens,
180-second budget and 8 GiB MLX allocation ceiling. Four synthetic cases completed
strict schema and captured-citation validation: threshold (18.86 s), sports
(12.23 s), election (11.61 s), and combination (15.03 s). These are invocation
latencies, not a throughput benchmark or representative real-corpus accuracy score.
No cloud inference or new weight download was used.

Quality is **not validated for release**. Inspection found unsupported statements:
the threshold response questioned a currency that was explicitly USD; the election
response inferred that recounts cannot alter certification; some statements still
confused observation conditions with settlement timing. Family output was threshold,
unknown, unknown and other respectively. Deterministic routing and existing formal
review gates remain in force independently of those model labels. All explanations
and suggested matches remain UNREVIEWED; none of these runs accepted a formal claim.
Raw outputs remain in the local validation dataset. A cited span establishes
provenance, not that its generated paraphrase is correct. Full discovery completeness,
real-contract explanation quality and the existing human product gates remain open.


## Follow-up change review

Adversarial review reproduced and fixed two additional failures: a parent event
rule revision could leave accepted child claims current, and pausing or crashing
after a partial-membership event could lose its error and falsely retire unseen
events after resumption. Regressions now cover both, preserving reviews on event
volume-only changes. Exhausted formal-compilation jobs also retain a terminal
diagnostic instead of repeatedly preparing and hashing the same model. These
software fixes do not alter the open live-completeness or human quality gates.
Manual child-only capture/import cannot discard previously discovered parent
evidence; the prior version survives with an actionable discovery-refresh error.


## Subsequent audit regression coverage

Regression tests cover Kalshi outcome-subtitle revision invalidation, source-preserving
legacy/WAL restore, exact catalog volume serialization and ordering, transactional
indexed migration and rollback, bounded transient compilation retries (including
legacy failure records), normalized-candidate editor corrections, and selected event
detail refresh after claim withdrawal. Background updates preserve open evidence
and keyboard focus, and identical detail payloads leave the DOM intact. Retry
coverage reopens a dataset after its first failed attempt. Negative controls retain invalid-derivation
rejection, exhausted-job limits, live-source exclusion and closed-detail behavior.
These are software checks; the live-discovery and human quality gates remain open.


## Two-venue removal and explicit migration (2026-09-08)

The supported implementation now covers Kalshi and Polymarket International.
Polymarket US support has been removed. Schema 4 migration regression coverage
includes verified backups, legacy restore without source mutation, historical and
cross-venue result deletion, shared chunk/artifact preservation, failed transaction
rollback, interrupted cleanup and unsupported-venue rejection before jobs/network.
Shared pagination, outage and restart tests now use supported-venue fixtures.
Software checks and bounded live observations for this revision are recorded below;
the earlier full-universe and independent human quality gates remain unfulfilled.

Executed verification: `scripts/verify-fast` and `scripts/verify` passed with 188
tests; lint, formatting, type checks, public schema drift and package build passed.
Independent migration review found and verified fixes for backup artifact integrity,
historical match jobs suppressing regeneration, and unnecessary startup history scans.
Browser checks used an isolated synthetic dataset: two venue filters, filtered empty
state, event details and the two-choice manual import selector all worked; the detail
panel was visually inspected. The temporary server was stopped afterward.

Bounded live requests returned one event each from Kalshi ordinary listings
(`KXELONMARS-99`), Kalshi multivariate listings
(`KXMVECROSSCATEGORY-SHARD1-S2026FF7C4259D33`) and International keyset listings
(`16183`). Each returned a continuation cursor. These checks establish endpoint
reachability and response parsing only; they do not establish complete scans or
explanation quality. No user dataset was migrated during implementation. Exact-revision
CI awaits the separate commit/push step.

## Local visual system restyle (2026-09-08)

The event browser and research workspace now use the canonical dark OddsFox
palette, Inter and JetBrains Mono, and the local OddsFox mark. Fonts and the
logo are served from the explicit `/assets` allowlist; OFL notices and a brand
carve-out are in the README. `scripts/verify-fast` and `scripts/verify` passed
189 tests, formatting, types, schema drift and the package build. Browser
checks used an isolated synthetic demo on `127.0.0.1:8777`: empty home events,
populated research contracts, skip-link/header/nav, no remote font or image
requests, and no horizontal overflow at 390px, 768px, and desktop. Keyboard
landmarks and cyan focus styles are present. The temporary demo server was
stopped afterward. Independent human product gates remain separate.

## Local UI/UX repair (2026-09-08)

Follow-up inspection of both local pages found remaining interaction defects
after the visual restyle: every control used the same orange primary, review
status badges did not distinguish rejection, generated buttons omitted
`type="button"`, the header was wider than the 1200px content shell, in-page
tabs did not match primary nav, and `scrollIntoView` ignored reduced motion.

Repairs: primary / secondary / danger button variants; header inner aligned to
the content shell; skip-link clip pattern; cyan disclosure chevrons; pill tabs;
review status uses success/warning badges; generated controls set
`type="button"`. `scripts/verify-fast` and `scripts/verify` passed 189 tests,
formatting, types, schema drift and the package build. Browser checks used an
isolated synthetic demo on `127.0.0.1:8778`: empty home events, populated
research contracts including Approve vs Reject, no remote asset requests, and
no page-level horizontal overflow at 390px, 768px, and desktop. Populated
event cards were not present in that demo (discovery empty); Understand/Close
behavior is covered by the Node frontend tests. Skip-to-content targets
`main#main-content` with `tabindex="-1"`. A leftover demo process may still be
bound to port 8778 if it was not stopped locally.

## Local unanimous consensus software (2026-09-08)

The local unanimous-consensus path is implemented as software: pinned model
manifests (including an adjacent operator-controlled lineage sidecar, weights-only identity,
chat-template hash, and `model_file`/Python/symlink rejection before load), disjoint
producer/evaluator panels, immutable ballots with captured-source citation checks,
write-once frozen labels, metrics v4 (`oddsfox-metrics/4`, `label_source:
local_unanimous_consensus`), `LOCAL_MODEL_CONSENSUS` publication off by default,
and human-rejection veto. Event details deep-link to
`/research?contract=<contract-version-id>`. Consensus is not described as ground
truth, independent human review, or proven natural-language correctness.

Metrics v4 now freezes explicit labels or abstentions for every comparison and
scores relationships only over the labeled-pair universe while reporting pair-label
coverage. Corpus and producer evidence include all mandatory stages, completion and
abstention outcomes, contract/pair latency, total latency, peak memory, failures,
and retries. Validation stages and hash-checks the full bundle before an atomic,
write-once publication; a failed write leaves no visible destination.
The bundle's `judge-evidence.json` is scoped to the exact label sets in the run and
retains panel configs, immutable ballots, dependency edges, raw responses, and
cited captured-source artifacts. Synthetic fixtures are explicitly marked and do
not satisfy the immutable-provenance release gate.

Executed verification: `scripts/verify-fast` and `scripts/verify` passed with
231 tests, Ruff, ty, public schema drift, and the package build. The historical
hosted run remained weight-free. **Local six-model panel evaluation and complete
two-venue scans were not run in this session**; those remain unmet product gates,
not a software-gate substitute.
Do not enable `--enable-consensus-publication` until a complete v4 local evidence
bundle satisfies every frozen machine gate.
