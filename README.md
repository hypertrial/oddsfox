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
still depends on the independent human benchmark, real cross-venue validation,
and measured review-effort gates in the product specification. Synthetic examples
and passing software tests do not establish those results.

## Run locally

Install [uv](https://docs.astral.sh/uv/), then:

```sh
uv sync --locked
uv run oddsfox serve
```

Open `http://127.0.0.1:8777`. The home page discovers open events from **Kalshi,
Polymarket International and Polymarket US**, with a default lifetime-volume
threshold strictly above $100,000 per venue event. All active markets in a qualifying
event are included. Kalshi volume is USD face-value notional; the Polymarket venues
use reported USD turnover. Missing lifetime totals appear in **Volume unknown**.

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

Offline discovery is also available with `uv run oddsfox sync`, optionally limited
by `--venue kalshi`, `--venue polymarket`, or `--venue polymarket_us`.
Use one process per dataset; stop the server before invoking offline CLI commands.
The public venue endpoints require no trading credentials, but network access or
venue-side restrictions can prevent a scan; the app reports these failures.
Polymarket US public listings do not expose a complete combination universe; its
separate beta combo lookup requires authenticated access and a known symbol.
See [executed validation and coverage limits](docs/validation.md).

### Synthetic demonstration

```sh
uv run oddsfox --data .oddsfox/demo demo
uv run oddsfox --data .oddsfox/demo serve --no-sync
```

Open `http://127.0.0.1:8777/research` for the synthetic contract/review demonstration.
Use an empty directory for a new demo. `demo --approve-synthetic` is available for
automated checks; synthetic approvals are never independent human benchmark evidence.
The review workspace also remains available from the event browser.

## Research workflow

1. Capture a bounded list of native market IDs with the report or
   `uv run oddsfox capture kalshi <ticker> ...`. Alternatively,
   `uv run oddsfox import polymarket market.json` preserves an exact local payload.
   Referenced documents can be supplied with `--documents documents.json`, a list
   of `{ "url": "https://...", "text": "...", "status": "captured" }` records.
   Missing governing documents prevent approval.
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
   Omitting `--approve` rejects or withdraws an interpretation. `oddsfox compare`
   produces provisional proposals; `oddsfox publish` checks current reviews and
   dependencies before accepting them.
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
ordinary binary resolution. Each condition names the exact pair of contract
versions, so differently conditioned pairs cannot share probability variables or
chain into a settlement claim. Observed-event claims remain a distinct scope,
with sparse edges over both provisional and reviewed interpretations. Settlement
pairs are evaluated directly. Background comparisons retain progress per canonical
observation and process bounded batches across the complete group. The dataset is
not capped at 250 contracts; explicit capture requests remain bounded to 250 IDs.
The legacy bounded candidate helper retains its limits for evaluation callers.

## Local model candidates

```sh
uv sync --locked --extra model
uv run oddsfox compile <contract-version-id> /path/to/quantized-mlx-model
# Or expose that already-downloaded model in the report:
uv run oddsfox serve --model /path/to/quantized-mlx-model
```

Model weights are separate downloads. OddsFox hashes their files and records
quantization, runtime, prompt, schema and decoding settings for each job. Initial
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
fields. Real-contract accuracy and the independent human release gates remain
unevaluated; see the [validation record](docs/validation.md).

## Evidence, evaluation and recovery

- [Public JSON Schema](schemas/semantic-ir-1.0.0.schema.json) and
  [consumer conformance notes](schemas/README.md).
- [Benchmark format and release procedure](docs/benchmark.md), with explicitly
  synthetic [benchmark](examples/benchmark.json) and [run](examples/run.json) inputs.
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
Node’s built-in test runner and requires no npm packages. CI configures Node 22.

Run `scripts/verify-fast` during development and `scripts/verify` before completing
work. The latter runs lint, formatting, type checks, tests, schema drift checks and
a package build. CI runs these lightweight checks; model and product benchmarks
remain local.

## License

Original OddsFox source code and documentation in this repository are licensed
under MIT. See [LICENSE](LICENSE) for the full terms.

Third-party dependencies, model weights, and market data retain their respective
licenses and terms; this repository's MIT license does not relicense them.

## Dataset upgrades

Database version 2 adds event catalogs, snapshots, semantic fingerprints and
processing progress transactionally. Existing immutable records and review history
are retained. Back up the complete dataset before upgrading; older application
versions refuse a newer database. Restore the pre-upgrade backup for rollback.
A code/model/configuration upgrade can still invalidate current conclusions under
the existing reproducibility policy. Subsequent volume/status-only refreshes do
not invalidate unchanged governing interpretations. Source artifact history is
retained, so long-running discovery increases disk use.
