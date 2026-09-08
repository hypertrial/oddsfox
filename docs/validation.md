# Implementation validation

This record separates implemented software from the product release gates.
The specifications remain the acceptance contract.

## Target environment

Validation started on 2026-09-08 on Apple Silicon with 32 GiB RAM, macOS 26.6.2,
and Python 3.14.7. `uv.lock` pins the resolved runtime and development tools.
The full dependency set, including cvc5, DuckDB, MLX-LM and Outlines, installed;
core and Outlines imports passed. Model inference is a separate check.

## Software checks

The automated suite covers canonical IR, exact decimals, explicit nulls, source
evidence, unsupported schemas, strict/inclusive threshold boundaries, differential
cvc5 checks, review gates, dependency revisions, stale jobs, crash recovery,
backup restoration, local API security, captured markup, exports and benchmark
deduplication/closure. The completion wrapper also runs lint, formatting, type
checks, schema drift validation and a package build.
The final local completion gate passed with 90 tests. Two upstream test-client
deprecation warnings remain. Independent architecture, security/recovery and
final verification reviews passed after their findings were fixed.

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

- Frozen real corpus with independent human labels, held-out templates and a
  measured acceptance policy meeting the documented relationship target.
- Human validation of an automatically derived eligible cross-venue relationship
  and an explained near-match rejection.
- Paired manual/assisted review-time measurements including corrections.
- Local model semantic quality and resource results on that representative corpus.

Synthetic demo approvals are explicitly labeled as such. Neither the demo nor
the scorer manufactures independent human evidence. CI configuration is included;
remote CI results only exist after the implementation is pushed and run.
