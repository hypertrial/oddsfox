# Security Policy

## Supported versions

| Version | Supported |
| ------- | --------- |
| 0.1.x   | Yes       |

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Report security issues through one of these channels:

1. **GitHub Private Vulnerability Reporting** (preferred): open the repository on GitHub, go to **Security** → **Report a vulnerability**, and submit a private report.
2. **Maintainer contact**: if private reporting is unavailable, contact the repository maintainers through Hypertrial's standard security contact process.

Include:

- A description of the issue and potential impact
- Steps to reproduce (proof of concept if available)
- Affected versions or commits
- Suggested fix or mitigation, if you have one

We will acknowledge receipt and work with you on a timeline for investigation and disclosure.

## Scope notes

OddsFox is a **local-first** research pipeline. It reads public Polymarket/Gamma APIs and stores data in a local DuckDB warehouse. Optional CLOB credentials in `.env` are user-supplied and must never be committed to the repository.
