---
trigger: always_on
applyTo: "**"
description: Banking-grade security and data protection rules (PDPA, least privilege, auditability).
---

## Data
- Treat names, national ID, phone, email, account/card numbers, balances, and transaction details as **PII / confidential**.
- Never put real customer data in code, tests, fixtures, prompts, logs, or docs. Use synthetic data only.
- Mask PII in logs and error messages (e.g. `xxxx-xxxx-1234`).

## Code
- Parameterized queries only; no string-built SQL.
- Validate all input at the boundary against the contract (type, length, format, range).
- AuthN/AuthZ on every endpoint; deny by default; check resource ownership (no IDOR).
- Secrets from env / secret manager only. Never commit `.env`, keys, or tokens.
- Money: use integer minor units or decimal types — never float.
- State-changing financial operations must be **idempotent** (idempotency key) and **auditable** (who, what, when, before/after).
- External calls: timeouts, retries with backoff only on idempotent operations, circuit breaker where relevant.

## Dependencies
- Prefer standard library and well-maintained packages. Flag new deps with license and last-release date.
