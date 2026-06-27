# Contributing to oddsfox

## Development setup

```bash
git clone git@github.com:hypertrial/oddsfox.git
cd oddsfox
cargo build
```

## Required checks

```bash
cargo test --verbose
cargo clippy --all-targets -- -D warnings
```

## Scope

- Read [AGENTS.md](AGENTS.md) for architecture and boundaries
- Keep diffs minimal and scoped
- Add tests when changing parse, schema, manifest, or metrics

## License

MIT
