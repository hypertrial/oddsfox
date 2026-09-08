# Semantic IR consumers

`semantic-ir-1.0.0.schema.json` is generated from the implementation schema and
checked for drift by `scripts/verify`. It targets JSON Schema 2020-12.
`oddsfox schema` and `/api/schema` expose the same schema.

The normative field inventory and serialization contract remain in the
[technical specification](../tech_spec_v1.md#public-v1-semantic-ir).

JSON Schema validation is necessary but insufficient. Consumers must also reject
duplicate object keys, unsupported versions, invalid calendar instants, nonpositive
measurement increments, unpaired Unicode surrogates, partial canonical identities,
unsorted/duplicate payout IDs or spans, and dangling evidence references. Check
captured native outcomes, artifact ownership and code-point span bounds. Every
populated semantic leaf needs a direct quotation or a resolvable derivation that
retains underlying source evidence. Sorting an existing payout array requires
remapping its evidence pointers. Readers never repair array order silently.

Canonical export uses RFC 8785 JCS. The SHA-256 digest belongs to the interpretation
envelope, outside the hashed payload. An IR artifact alone carries no semantic
approval. Current acceptance additionally requires reviewed definitions, explicit
interpretation approval, formal verification and current dependency versions.

The implementation rejects other schema major versions. Persistence has its own
schema version, independent of Semantic IR 1.0.0.
Dataset migrations preserve artifacts and review history; governing or compiler
changes invalidate dependent approvals before recompilation.
