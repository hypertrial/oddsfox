# OddsFox project notes

## Project and documentation

OddsFox is MIT-licensed FOSS: a local compiler and verifier for prediction-market
contracts. The repository includes a Python local application and V1 specifications.
The independent human product gates remain separate from software completion.

- `product_spec_v1.md` owns user needs, scope, and acceptance criteria.
- `tech_spec_v1.md` owns the public semantic IR, correctness, and system behavior.
- `tech_stack_v1.md` owns implementation and deployment choices.
- `README.md` is the public entry point; `LICENSE` contains the MIT terms.

Consult these documents before implementation. Keep their responsibilities
distinct and preserve the distinction between planned and implemented behavior.

## Engineering workflow

Use the `oddsfox-engineering` workspace from `.pad.toml`. Follow `AGENTS.md` and
the local `pad-engineering` skill for the shared workflow. Keep ticket bodies,
exports, credentials, and local Pad state out of this public repository.

Keep V1 limited to the documented local compiler/verifier. Preserve provenance,
explicit unknowns, versioned semantics, review gates, and revision invalidation.
Solver results establish encoded relationships; they do not establish the
accuracy of natural-language interpretation.

## Implementation and verification

Use Python 3.14 and `uv sync --locked`; the optional `model` extra installs the
Apple Silicon inference runtime. `src/oddsfox` contains the IR, reasoner, single
DuckDB coordinator, adapters, compiler, report/API, CLI and evaluation runner.
Keep local captures, databases and weights under ignored `.oddsfox/` or outside
the repository. There is one writer per dataset; server users use its API.

- `scripts/verify-fast`: whitespace check, Ruff lint and the pytest suite.
- `scripts/verify`: documentation checks, the fast gate, formatting, ty, public
  JSON Schema drift check and a package build.

Wrappers work from any directory. CI runs the lightweight completion gate. Model
smokes and independent human release benchmarks run locally; record their actual
outcomes in Pad and never substitute synthetic fixtures for human evidence.
Update wrappers, these notes and canonical Universal Pad configuration together
when changing required verification. See `docs/validation.md` for executed checks
and `docs/benchmark.md` for the frozen evaluation interface.
