---
name: design
description: Help the human architect shape a solution for a business problem — clarify goals and constraints, propose 2–3 options with a trade-off table, recommend one, then record the human's choice as an ADR and architecture update. Use for /design, "how should we build X", "propose an architecture", or "compare approaches". Never writes implementation code.
---

# Design (AI proposes, human decides)

1. **Clarify the business problem** — read `docs/00-business/problem-statement.md`. Ask at most 5 questions about goal, users, success metric, constraints (budget, deadline, compliance, legacy systems). Do not proceed on unstated assumptions.
2. **Propose 2–3 options** with a trade-off table: complexity, cost, time-to-market, scalability, operational risk, security/compliance impact, reversibility.
3. **Recommend one**, stating what would make you change the recommendation. Then STOP and wait for the human's decision.
4. After the human chooses, use skill `write-adr` to draft the ADR and update `docs/01-architecture/architecture.md` (Mermaid C4/sequence where useful).
5. If interfaces change, draft the diff to `contracts/` and mark it **PROPOSED — needs human approval**.
6. Do not write implementation code in this skill. Next step: `/spec`.
