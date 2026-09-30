---
name: spec
description: Write an implementable feature spec with testable Given/When/Then acceptance criteria, contract references, data changes, failure cases, NFRs, out-of-scope, and open questions. Use for /spec, "write a spec", "break this into tasks", or before any implementation work. Only after the design/ADR is approved.
---

# Write Feature Spec

1. Read problem statement, architecture, related ADRs, and `contracts/`.
2. Create `docs/02-specs/<feature-kebab>.md` from [assets/spec-template.md](assets/spec-template.md).
3. Acceptance criteria rules:
   - Id each one `AC-01`, `AC-02`…; Given/When/Then; one behavior per AC.
   - Include failure paths: validation error, unauthorized, not found, conflict/duplicate, downstream timeout.
   - Include NFR ACs where relevant (latency p95, idempotency, audit log entry, PII masking).
4. Reference exact contract operations (`operationId`) and tables. If they don't exist yet, list them under **Contract/Schema changes (needs approval)**.
5. For anything touching auth, money, PII, or external integration, run skill `threat-model` and add each mitigation as an AC.
6. Anything unknown goes to **Open questions** — never guess business rules.
7. Estimate size (S/M/L). If L, propose splitting into multiple specs.
8. Status `DRAFT`. Ask the human to review. Implementation starts only when the human sets `APPROVED`.
