# OddsFox project notes

## Project and documentation

OddsFox is MIT-licensed FOSS: a local compiler and verifier for prediction-market
contracts. The repository includes a Python local application and V1 specifications.
The local unanimous-consensus product gates remain separate from software completion.

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
explicit unknowns, versioned semantics, consensus or human acceptance, and revision invalidation.
Solver results establish encoded relationships; they do not establish the
accuracy of natural-language interpretation. Unanimous local models do not
establish real-world semantic truth.

## Implementation and verification

Use Python 3.14 and `uv sync --locked`; the optional `model` extra installs the
Apple Silicon inference runtime. `src/oddsfox` contains the IR, reasoner, single
DuckDB coordinator, adapters, discovery/sync/catalog, local explanations, compiler, event browser/report/API, CLI and evaluation runner.
Keep local captures, databases and weights under ignored `.oddsfox/` or outside
the repository. There is one writer per dataset; server users use its API.

- `scripts/verify-fast`: whitespace check, Ruff lint and the pytest suite.
- `scripts/verify`: documentation checks, the fast gate, formatting, ty, public
  JSON Schema drift check and a package build.

Wrappers work from any directory. For OddsFox, a fresh clean-checkout `scripts/verify` run is
the authoritative completion gate. GitHub Actions is not used because hosted
Actions are unavailable to the free organization. The latest verified baseline is
269 passing tests with two upstream dependency deprecation warnings. Model smokes
and local six-model panel evaluation run locally; record their actual outcomes in
Pad and never substitute synthetic fixtures for consensus evidence.
Update wrappers, these notes and canonical Universal Pad configuration together
when changing required verification. See `docs/validation.md` for executed checks
and `docs/benchmark.md` for the frozen evaluation interface.

The default server discovers open events across Kalshi and Polymarket International. Governing contract
revisions are separate from volatile snapshots; preserve this boundary. Event and
explanation interfaces have their own versions; Semantic IR remains 1.0.0. Do not
use model family labels as proof of eligibility or automatically approve suggestions.
Use `serve --no-sync` for deterministic offline/synthetic demonstrations.
Consensus publication remains off unless an operator records producer-panel
approvals (`oddsfox consensus-approve`) and then publishes with `--allow-consensus`
or serves with `--enable-consensus-publication`.
