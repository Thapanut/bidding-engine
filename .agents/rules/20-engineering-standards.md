---
trigger: always_on
applyTo: "**"
description: Coding, testing, and change-size standards.
---

- Follow existing patterns in the repo before introducing new ones.
- Hexagonal layering (ADR-0001): business rules live in `internal/core`; adapters translate only. `core` never imports adapters, platform, Fiber, DB drivers, Redis, Kafka, or net/http. New I/O = new port + adapter, wired in `cmd/`.
- Fiber/pgx guardrails: handlers `return err` (central ErrorHandler), strict JSON decoding, `c.UserContext()` into ports, copy fasthttp strings kept past the handler; SQL stays in the postgres adapter, parameterized queries only.
- Test business rules against the in-memory adapter; prove concurrency/transaction guarantees with `-tags=integration` tests on PostgreSQL.
- Every acceptance criterion in the spec maps to at least one automated test. Name tests after the AC id (e.g. `TestAC03_RejectsDuplicateTransfer`).
- Cover the failure paths listed in the spec, not only the happy path.
- Keep diffs small; if a change exceeds ~400 lines, propose splitting the spec.
- Public functions have doc comments; complex business rules reference the spec/AC id.
- Update `CHANGELOG.md` and any affected docs in the same change.
