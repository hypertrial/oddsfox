# Security Policy

## Reporting

Report vulnerabilities privately via GitHub Security Advisories.

## Posture

oddsfox is a **local-first CLI tool**:

- No hosted service or authentication in v0.1.0
- Downloads from public Polymarket APIs only
- Lake data stays on the local filesystem
- `serve` binds to localhost by default and is read-only

## Out of scope

- Trading or wallet key handling
- Geo-restriction bypass
