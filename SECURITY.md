# Security Policy

## Reporting

Report vulnerabilities privately via GitHub Security Advisories.

## Posture

oddsfox is a **local-first CLI tool**:

- No hosted service or authentication in v0.2.0
- Downloads from public Polymarket and Kalshi APIs
- Lake data stays on the local filesystem
- `serve` binds to localhost by default and is read-only

## Credentials and user data

- **Kalshi API keys** are read from local config (`key_id`, `private_key_path` in `oddsfox.toml`). Private key files remain on disk; oddsfox does not transmit keys except as signed read-only API requests to Kalshi.
- **Polymarket user PnL** uses a user-supplied public wallet or proxy address. No wallet signing or order submission.
- **User fills, positions, and PnL** are stored only in the local lake (`bronze_user_*`, `gold_user_pnl`).

## Out of scope

- Trading or wallet key handling for order submission
- Geo-restriction bypass
