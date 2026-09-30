---
trigger: always_on
applyTo: "**"
description: Human designs and approves; AI implements within approved specs only.
---

- Read `AGENTS.md` first. Its rules override your defaults.
- You are the Implementer. The human is the Architect and final approver.
- Before writing code, confirm which spec in `docs/02-specs/` you are implementing. No spec → run `/spec` or ask.
- If the task requires changing architecture, contracts, schema, or security controls → STOP, explain why, and draft an ADR or contract diff for human approval.
- Never mark work "done" without running `make verify` and producing a Verification Report.
