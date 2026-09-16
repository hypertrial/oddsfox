# OddsFox

OddsFox is free and open-source software (FOSS), released under the
[MIT License](LICENSE).

OddsFox is a local compiler and verifier for prediction-market contracts. It
produces traceable formal interpretations and verified relationships, helping
researchers compare contract meanings and inspect settlement differences.
Its primary compiler output is a public, versioned semantic IR exported as
canonical JSON; see the [IR contract](tech_spec_v1.md#public-v1-semantic-ir).
Symbolic probability constraints are derived outputs of verified semantics.
Global probability coherence and larger constraint graphs are downstream extensions.
The repository now includes a runnable local implementation. Release readiness
still depends on the local unanimous-consensus gates in the product specification.
Synthetic examples and passing software tests do not establish those results.
Unanimous local models are not independent human review and do not establish
natural-language truth.

## Run locally

Install [uv](https://docs.astral.sh/uv/), then:

```sh
uv sync --locked
uv run oddsfox serve
```

Open `http://127.0.0.1:8777`. The home page discovers open events from **Kalshi and
Polymarket International**, with a default lifetime-volume
threshold strictly above $100,000 per venue event. All active markets in a qualifying
event are included. Kalshi volume is USD face-value notional; Polymarket International
uses reported USD turnover. Missing lifetime totals appear in **Volume unknown**.

Discovery runs on startup and every 15 minutes while the server runs. The session
token printed in the terminal authorizes Refresh now, pause/resume and review
changes. Partial scans and failures remain visible; last successful data is retained.
Use venue/category/analysis filters and open an event to inspect its captured rules.

For automatic local explanations, install the optional model runtime and provide
an explicitly downloaded quantized model:

```sh
uv sync --locked --extra model
uv run oddsfox serve --model /path/to/quantized-mlx-model
```

Explanations and cross-venue match suggestions are **unreviewed AI outputs**, with
captured citations and explicit gaps. They are not verified equivalences. No model
is downloaded automatically and there is no cloud fallback. Initial processing can
take hours; source chunks are cached and progress appears as work completes.

One-shot discovery is also available with `uv run oddsfox sync`, optionally limited
by `--venue kalshi` or `--venue polymarket`.
Use one process per dataset; stop the server before invoking offline CLI commands.
The public venue endpoints require no trading credentials, but network access or
venue-side restrictions can prevent a scan; the app reports these failures.
See [executed validation and coverage limits](docs/validation.md).

### Synthetic demonstration

```sh
uv run oddsfox --data .oddsfox/demo demo
uv run oddsfox --data .oddsfox/demo serve --no-sync
```

Open `http://127.0.0.1:8777/research` for the synthetic contract/review demonstration.
Use an empty directory for a new demo. `demo --approve-synthetic` is available for
automated checks; synthetic approvals are never consensus or independent-human
benchmark evidence.
The review workspace also remains available from the event browser. Event details
link each captured contract version to `/research?contract=<contract-version-id>`.

## Research workflow

1. Capture a bounded list of native market IDs with the report or
   `uv run oddsfox capture kalshi <ticker> ...`. Alternatively,
   `uv run oddsfox import polymarket market.json` preserves an exact local payload.
   Referenced documents can be supplied with `--documents documents.json`, a list
   of `{ "url": "https://...", "text": "...", "status": "captured" }` records.
   `status` may be `inaccessible` without `text`. A later inaccessible refetch of a
   previously captured official URL keeps the last governing document bytes; it does
   not mint a new contract version. Missing governing documents prevent approval. A
   contract already captured through
   event discovery must also be refreshed through discovery; a child-only import or
   capture cannot discard its known parent rules. Existing manual-only captures
   and imports retain their original workflow.
2. Register an exact reviewed observation with
   `uv run oddsfox register <stable-name> observation.json --reviewer <name> --rationale <reason>`.
   The observation initially has null canonical IDs. The registry fixes every
   other field. The API also supports explicitly reviewed aliases with exact,
   positive unit conversions; model suggestions never merge definitions.
3. Obtain a draft through the report or `oddsfox draft <contract-version-id>`.
   Save candidate IR with `oddsfox interpret candidate.json`. Each populated
   semantic leaf needs source evidence. Corrections create new interpretations
   and invalidate dependent approvals and claims. Restoring earlier candidate
   content creates a new revision requiring fresh review. Referenced normalization
   rules remain dependencies even if the canonical identity is corrected; withdrawing
   a rule makes its dependent interpretations and accepted claims stale.
4. Review through the report or `oddsfox review <interpretation-id> --approve
   --governing-material-complete --reviewer <name> --rationale <reason>`.
   Omitting `--approve` rejects or withdraws an interpretation and vetoes model
   consensus. `oddsfox compare` produces provisional proposals; `oddsfox publish`
   checks current human reviews or, when `--allow-consensus` is set, current
   unanimous producer-panel approvals, plus dependencies, before accepting them.
   Consensus publication is off by default. Record a producer-panel approval with
   `oddsfox consensus-approve <interpretation-id> --producer-model ...` (at least
   three disjoint local models). That command never writes a human `review`
   record. Enable server-side publication with `--enable-consensus-publication`.
5. Export with `oddsfox export --output research.json`, or
   `oddsfox export --format parquet --output accepted.parquet` for current accepted
   assertions. `--history` explicitly includes obsolete JSON records. Refresh a
   source using capture again; governing-content changes make affected conclusions stale while
   their evidence remains available.

Parquet files store accepted assertions as rows and the JSON export's version,
configuration, supporting records and freshness in the `oddsfox` footer metadata.
This metadata is present even for an empty export; DuckDB exposes it through
`parquet_kv_metadata('accepted.parquet')`.

Prefix these commands with `uv run`; use `--data <directory>` before the command
to select a dataset. `oddsfox --help` lists all commands. The local OpenAPI contract
is at `/openapi.json`; all API mutations require `X-Oddsfox-Token`. Only the exact
loopback host and origin are accepted. Imported text is rendered as text.

V1 reasoning uses exact rational arithmetic for finite threshold predicates over
a shared real-valued observation. The supported method is `instantaneous`.
Interval averages, maxima and unresolved methods remain unsupported. cvc5 checks
the supported encodings and probability constraints. Settlement policy prose is
not executable: the initial settlement rule produces explicit conditions for
ordinary binary resolution. Shared native outcome IDs must have matching
true-branches; inverted YES/NO maps are settlement `DIFFERENT` and stay in
observed-event scope. Disjoint ordinary-binary IDs (cross-venue tokens) remain
conditional. Each condition names the exact pair of contract
versions, so differently conditioned pairs cannot share probability variables or
chain into a settlement claim. Observed-event claims remain a distinct scope,
with sparse edges over both provisional and reviewed interpretations. Settlement
pairs are evaluated directly. Background comparisons retain progress per canonical
observation and process bounded batches across the complete group. The dataset is
not capped at 250 contracts; explicit capture requests remain bounded to 250 IDs.
`GET /api/report` includes at most 250 current comparison rows and sets
`comparison_coverage` when more exist; `GET /api/comparisons` remains paginated.
The legacy bounded candidate helper retains its limits for evaluation callers.

## Local model candidates

```sh
uv sync --locked --extra model
uv run oddsfox compile <contract-version-id> /path/to/quantized-mlx-model
# One explanation model; panels are separate flags:
uv run oddsfox serve --model /path/to/quantized-mlx-model \
  --producer-model /path/to/producer-a --producer-model /path/to/producer-b --producer-model /path/to/producer-c \
  --evaluator-model /path/to/evaluator-a --evaluator-model /path/to/evaluator-b --evaluator-model /path/to/evaluator-c
uv run oddsfox validate --output /path/to/new-bundle \
  --producer-model /path/to/producer-a --producer-model /path/to/producer-b --producer-model /path/to/producer-c \
  --evaluator-model /path/to/evaluator-a --evaluator-model /path/to/evaluator-b --evaluator-model /path/to/evaluator-c
```

Pass a single `--model` for explanations. Producer and evaluator panels must each
contain at least three distinct families with no overlap. `validate` writes a
create-only hashed evidence bundle. Consensus publication stays off until
`oddsfox publish --allow-consensus`.

Model weights are separate downloads. OddsFox hashes their files and records
quantization, family, chat-template hash, runtime, prompt, schema and decoding
settings for each job. Initial
generation is serialized, with an 8 GiB MLX allocation limit and at most 8,192
generated tokens. A 300-second budget is checked between prefill/decode steps;
it is not a hard deadline for model loading or a stalled native backend.
The normal path uses an Outlines grammar for the IR structure, valid evidence
field names, and captured source-line spans (at most 256 lines). Large string and
array size limits are enforced by the complete public IR validator afterward,
avoiding excessive decoder automaton expansion. The explicit
`--unconstrained` option creates a different recorded configuration. Invalid model
outputs remain failed attempts with raw evidence. There is no external inference
fallback or automatic approval. `replay` revalidates a stored response without
running inference. Inspect `status` for failures, pending work and measurements.
The selected local model passed a synthetic compilation smoke with 26 evidence
fields. Real-contract accuracy and the local unanimous-consensus release gates remain
unevaluated; see the [validation record](docs/validation.md).

## Evidence, evaluation and recovery

- [Public JSON Schema](schemas/semantic-ir-1.0.0.schema.json) and
  [consumer conformance notes](schemas/README.md).
- [Benchmark format and release procedure](docs/benchmark.md), with explicitly
  synthetic [v3](examples/benchmark.json) and [v4](examples/benchmark-v4.json)
  fixtures. v3 remains compatibility-only; v4 is the consensus schema.
- Stop the server, then `oddsfox backup /path/to/new-backup`. Restore into a new
  dataset with `oddsfox --data /path/to/new-dataset restore /path/to/backup`.
  Backups verify all database-referenced artifacts and hashes before publication.
- [Implementation validation record](docs/validation.md) distinguishes executed
  software checks from outstanding product gates.

## Specifications

- [Product specification](product_spec_v1.md): user, workflow, scope, and acceptance criteria.
- [Technical specification](tech_spec_v1.md): semantics, verification, and system behavior.
- [Technology stack](tech_stack_v1.md): local implementation and deployment choices.

## Development workflow

This repository uses Universal Pad for engineering work. Agent workflow and
repository-specific verification are documented in [AGENTS.md](AGENTS.md) and
[PROJECT_AGENT.md](PROJECT_AGENT.md).

Install Node.js 22 or newer for the browser JavaScript regression test; it uses
Node’s built-in test runner and requires no npm packages.

Run `scripts/verify-fast` during development and `scripts/verify` before completing
work. The latter runs lint, formatting, type checks, tests, schema drift checks and
a package build. A fresh local `scripts/verify` run is the authoritative completion
gate. GitHub Actions is not used because hosted Actions are unavailable to the free
organization. The latest verified baseline is 235 passing tests with two upstream
dependency deprecation warnings; model and product benchmarks also remain local.

## License

Original OddsFox source code and documentation in this repository are licensed
under MIT. See [LICENSE](LICENSE) for the full terms.

Third-party dependencies, model weights, and market data retain their respective
licenses and terms; this repository's MIT license does not relicense them.

Bundled Inter and JetBrains Mono variable fonts remain under the SIL Open Font
License, Version 1.1. See `src/oddsfox/static/OFL-Inter.txt` and
`src/oddsfox/static/OFL-JetBrainsMono.txt`. The OddsFox mark is a trademark and
brand asset; it is included for product identification in this application and is
not licensed under MIT.

## Dataset upgrades

New datasets use database version 4. Existing versions 1–3 require an explicit
migration before the server or ordinary CLI commands can open them. Stop OddsFox,
then run this once for each dataset (the backup directory must not exist):

```sh
uv run oddsfox --data .oddsfox migrate --backup ../oddsfox-pre-v4-backup
```

The migration verifies a complete backup before permanently removing retired
Polymarket US data and dependent results from that dataset. Kalshi and International
records, shared evidence and model weights are retained. Interrupted artifact cleanup
resumes on the next startup. If migration fails before commit, the database changes
roll back; if cleanup fails afterward, startup resumes cleanup before processing jobs.
The command reports the backup location and deletion counts.

Restore into a new directory with `oddsfox --data /path/to/new-dataset restore
/path/to/backup`. Restore preserves the backup's schema and never changes the source;
legacy restored datasets require the migration command before use with this release.
For rollback to the old application, use the preserved pre-upgrade backup with that
release. Database versions are independent of Semantic IR 1.0.0.

A code/model/configuration upgrade can still invalidate current conclusions under
the existing reproducibility policy. Volume/status-only refreshes do not invalidate
unchanged governing interpretations. Source history increases disk use over time.
