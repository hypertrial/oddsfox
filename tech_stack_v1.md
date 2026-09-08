# OddsFox — Technology Stack V1

This stack supports the MIT-licensed OddsFox FOSS project. Third-party components
retain their own licenses; see the [repository licensing scope](README.md#license).

## Ownership and deployment decision

This document owns technology choices for the behavior in the
[technical specification](tech_spec_v1.md) and scope in the
[product specification](product_spec_v1.md). It does not redefine their semantics
or acceptance criteria.

V1 is a single-user local application developed and evaluated on a MacBook M4 Air.
Local execution is an initial deployment constraint for reproducible research and
bounded operating cost, not a claim that local inference meets the quality target.
Market ingestion needs network access; compilation and analysis run locally after
capture. No hosted application, orchestration service, or containers are required.

## Runtime and developer tools

Use Python with `uv` for interpreter management, locked dependencies, environments,
and execution. Python 3.14 is the initial compatibility candidate; confirm that
the complete selected dependency set installs and passes the smoke checks on the
target machine before pinning it. If it fails, select and document a supported
Python version rather than claiming unverified native compatibility.

Use Ruff, ty, pytest, and Hypothesis for linting, type checking, focused tests, and
boundary/property tests. Pin the resolved tool versions in the lockfile.

## Persistence and data processing

Use one local DuckDB database for normalized records, jobs, dependency tracking,
review decisions, assertions, and current-version pointers. Store exact raw
payloads and proof artifacts in an immutable, content-addressed directory beside
the database. Use Parquet for analytical exports and bulk evaluation datasets.

Stage artifact writes through temporary files and atomically rename them before
committing database references. An interrupted write may leave an unreferenced
artifact; it must not leave a committed reference to an incomplete file. Back up
the database and artifact directory as one logical dataset while the application
is stopped, and verify restoration before relying on a backup.

Use DuckDB SQL and ordinary Python collections for transformations and threshold
indexing. V1 needs no separate vector database, graph database, dataframe library,
or in-memory graph library. Add one only after a measured workload justifies it.

## Ingestion and validation

Use `httpx` for the three explicit public venue adapters, with explicit timeouts, bounded retries, and
rate-limit handling. Use Pydantic to validate typed semantic objects and generate
the public versioned IR JSON Schema specified in the
[technical specification](tech_spec_v1.md#public-v1-semantic-ir). Pydantic is the
implementation schema; the canonical JSON interface is language-independent.
Preserve validation failures as processing attempts with diagnostics, outside the
accepted semantic tables.

## Local model inference

Use MLX-LM as the initial runtime candidate. Evaluate one quantized model first;
do not require a small/large model cascade, embeddings, or a reranker for V1.
Exact matching and reviewed aliases cover the initial canonical registry.

Evaluate Outlines with the selected MLX-LM/model combination for constrained
structured generation. Treat working schema support as a smoke-test requirement,
not an assumed guarantee for every schema or backend version. Pydantic validation
remains mandatory regardless of generation constraints.

Pin model identity, weight revision/hash, quantization, runtime, prompt, decoding
settings, and schema per evaluation run. Select the model using measured semantic
quality, peak memory, and latency on the target machine. Do not change models
silently according to free memory: a different model is a versioned configuration
change requiring evaluation and dependent recomputation.

Set resource limits from measured workloads, and serialize inference initially.
If no local model meets the product gates, report that limitation; do not relax
the meaning of accepted results. Alternate runtimes or external inference require
a documented deployment decision, not an automatic fallback.

## Formal reasoning

Use Python rules for supported threshold relationships and cvc5 as the initial SMT
solver for remaining supported checks. Retain SMT-LIB inputs, solver options and
version, results, and proof certificates where the selected configuration supports
them. Rule-derived claims retain their rule artifacts instead.

Z3 is an optional development-only differential check if an encoding discrepancy
needs investigation. It is not a required second production solver, and agreement
does not validate contract interpretation. V1 has no CVXPY, numerical probability
projection, or optimization-solver dependency.

## Process model, reports, and API

Run one application process that owns the writable DuckDB connection and
serializes database writes through a coordinator. Bounded discovery workers and one local-model lane consume persisted work;
formal comparisons use a separate bounded lane. Inference, document extraction and
network work execute outside short database transactions. A two-second scheduler
checks persisted due times; a completed discovery run becomes due again after 900
seconds. One active run per venue coalesces requests and resumes page checkpoints.
Worker-level failures expose diagnostics and backoff without stopping other lanes. All consumers access the database through this process while it is
running. Do not launch independent writable workers or multiple server processes.

Use FastAPI and Pydantic for a loopback-only API. Provide a simple local HTML
comparison report and explicit review actions through the same application; no
frontend framework is required. Bind to `127.0.0.1`, reject untrusted host/origin
requests, and require an application-issued session token for review writes so
unrelated web pages cannot silently approve interpretations.

Provide canonical IR JSON and structured relationship exports through the
application; use Parquet for bulk datasets. Preserve the public IR schema version
across persistence and export. Model work must not block report access. Temporal,
Redis, PostgreSQL, object-storage services, and a multi-user deployment are outside
this V1 stack.

## Implementation readiness checks

Before treating this stack as validated, record the target machine's memory,
operating system, chosen Python version, lockfile, and results for:

1. Clean installation and imports on Apple Silicon.
2. Model loading, constrained generation, and schema validation on representative
   supported contracts and malformed output; public IR canonical round trips,
   decimal/null preservation, evidence references, and schema-version transitions.
3. Rule and SMT agreement on threshold boundaries, plus captured proof artifacts
   and bounded solver failure behavior.
4. End-to-end ingest, review, export, source revision, and stale-result withdrawal.
5. Interrupted-run recovery, consistent reads during processing, and restoration
   of the database with its artifacts.
6. The product benchmark and technical-spec stage metrics, including the frozen
   acceptance-policy version/configuration, memory, latency, and review effort.

These are implementation gates, not checks already performed by this document.

## Discovery runtime additions

Use additive transactional DuckDB version-2 migration for catalogs, retrieval
snapshots, semantic-head indices, sync state and comparison progress. Existing nodes
and dependency IDs remain immutable. Backup/restore integrity checks cover the new
artifact and node references. Keep network clients bounded and model inference
serialized through the existing MLX lock.

Use `pypdf` for official PDF text extraction and the standard-library HTML parser
for HTML. Run extraction in a disposable subprocess: 8 MiB input, 200 PDF pages,
2 MiB extracted text, CPU/time limits and Linux address-space limits. Preserve
unreadable material as an explicit gap. There is no OCR fallback or arbitrary-site
crawler. Keep the host allowlist in the document boundary module.

Use the existing plain HTML/CSS/JavaScript frontend for the event browser, with
server-side pagination and cached comparison reads. Node's built-in test runner
continues to exercise shipped JavaScript; no frontend package manager is required.
