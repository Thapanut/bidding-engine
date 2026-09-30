---
name: verify
description: Verify an AI-produced change against its spec and produce an evidence-based Verification Report (AC traceability, test/lint/security results, contract conformance, risk checklist, manual checks for the human). Use for /verify, after /implement, "review this", "is it ready to merge", or before any PR.
---

# Verify Change

You are now the **reviewer**, not the author. Be skeptical of the code you (or another agent) wrote.

## Steps
1. Run `make verify`. Paste the real summary. A gate that is `NOT RUN` makes the verdict CHANGES REQUIRED unless the human explicitly waives it.
2. **AC traceability** — for each AC in the spec, name the test(s) that prove it and their result. Missing test = FAIL.
3. **Contract conformance** — compare handlers with `contracts/` (paths, methods, fields, status/error codes).
4. **Diff review** — read the full diff (`git diff main...HEAD`). Check:
   - [ ] No scope creep beyond the spec
   - [ ] No hard-coded secrets, URLs, or real data
   - [ ] Input validation, authZ + ownership checks
   - [ ] Parameterized SQL; transactions where multiple writes must be atomic
   - [ ] Idempotency + audit log on financial/state-changing ops
   - [ ] Timeouts/retries on external calls
   - [ ] PII not logged; errors don't leak internals
   - [ ] Money not using float
   - [ ] Migrations are backward compatible / reversible
5. **Hallucination check** — every imported package, function, env var, and table actually exists.

## Output: Verification Report
```
## Verification Report — <spec> — <date>
Verdict: READY FOR HUMAN REVIEW | CHANGES REQUIRED
| Gate | Result | Evidence |
| Tests | PASS/FAIL | x passed, y failed, coverage z% |
| Lint | | |
| Security (gosec/govulncheck/gitleaks) | | |
| Contract | | |
### AC traceability
| AC | Test | Result |
### Findings (severity: High/Med/Low)
### Human must check manually
- e.g. business rule interpretation of AC-04, UX copy, production config
```
Append one line to `docs/03-verification/verification-log.md`.
