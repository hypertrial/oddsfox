# OddsFox project notes

## Project and documentation

OddsFox is MIT-licensed FOSS: a local compiler and verifier for prediction-market
contracts. The repository currently contains specifications, not an application.

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

## Current verification

- `scripts/verify-fast`: `git diff --check`.
- `scripts/verify`: working-tree and staged whitespace checks, then checks that
  the README, license, and three specifications exist and are nonempty.

Both wrappers run from the repository root even when invoked elsewhere. These
are documentation sanity checks only. There is no runtime test suite, build,
benchmark runner, or CI workflow yet; a passing wrapper does not validate the
product's semantics or acceptance gates. Review document coherence manually.

When application code is introduced, add its relevant lint, type, test, and
integration commands to the wrappers and update these notes and the canonical
Universal Pad configuration. Run heavy validation locally, as required by the
parent workspace instructions.
