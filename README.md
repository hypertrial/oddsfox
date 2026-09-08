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
The repository currently contains V1 specifications; they describe planned
behavior, not a completed implementation.

## Specifications

- [Product specification](product_spec_v1.md): user, workflow, scope, and acceptance criteria.
- [Technical specification](tech_spec_v1.md): semantics, verification, and system behavior.
- [Technology stack](tech_stack_v1.md): local implementation and deployment choices.

## Development workflow

This repository uses Universal Pad for engineering work. Agent workflow and
repository-specific verification are documented in [AGENTS.md](AGENTS.md) and
[PROJECT_AGENT.md](PROJECT_AGENT.md).

## License

Original OddsFox source code and documentation in this repository are licensed
under MIT. See [LICENSE](LICENSE) for the full terms.

Third-party dependencies, model weights, and market data retain their respective
licenses and terms; this repository's MIT license does not relicense them.
