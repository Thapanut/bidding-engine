---
name: implement
description: Implement an APPROVED feature spec test-first, strictly within its scope, following AGENTS.md standards. Use for /implement, "build this spec", or "code this feature".
---

# Implement from Spec

## Preconditions
- Spec status is `APPROVED`. Otherwise stop and say so.
- All referenced contracts/tables exist, or their changes are explicitly approved in the spec.

## Steps
1. **Plan** — list files to create/modify and map each AC to its test(s). Show the plan and wait for "go" if it touches more than ~10 files.
2. **Tests first** — one or more tests per AC, named `TestAC<NN>_<Behavior>`. Use synthetic data only.
3. **Implement** — minimal code to satisfy the ACs. Follow handler → service → repository.
4. **Conform to contract** — request/response shapes and error codes must match `contracts/` exactly.
5. **Run** `make verify`; iterate until green. Never skip, delete, or weaken tests/checks.
6. **Document** — update CHANGELOG and any doc the change affects.
7. **Hand off** — list deviations from the spec (should be none), assumptions, and `UNCONFIRMED` items, then run `/verify`.

## Stop conditions (ask the human)
- The spec is ambiguous or contradicts contracts/ADR.
- You need a new dependency, a schema change, or a new external integration not in the spec.
- A security rule in `.agents/rules/10-security-compliance.md` would be violated.
