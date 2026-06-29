# Security Policy

## Scope

This repository is a **decision-support analytics project built entirely on
synthetic data**. It contains no production systems, no real operational data,
and no secrets. The main security surface is the software supply chain and the
code that builds the published artifacts.

CI enforces two automated checks on every push and pull request:

- **bandit** — static security analysis of `src/`.
- **pip-audit** — dependency vulnerability audit (`--strict`).

## Supported versions

The latest release on `main` is the only supported version.

| Version | Supported |
|---|---|
| 1.x (latest on `main`) | ✅ |
| older | ❌ |

## Reporting a vulnerability

If you find a security issue (for example a vulnerable dependency, a code path
that could execute untrusted input, or a supply-chain concern):

1. **Do not** open a public issue for anything exploitable.
2. Use GitHub's **private vulnerability reporting** (the *Report a vulnerability*
   button under the repository's **Security** tab), or contact the maintainer
   directly via the email on the GitHub profile.
3. Please include reproduction steps and the affected version/commit.

You can expect an acknowledgement within a few days. Fixes for confirmed issues
are prioritised on `main`.
