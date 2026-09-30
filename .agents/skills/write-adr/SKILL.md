---
name: write-adr
description: Draft an Architecture Decision Record (ADR) capturing context, options, trade-offs, decision, and consequences. Use when the human has chosen an architectural option, when a spec requires changing architecture/contracts/schema, or when asked to "record a decision" or "write an ADR".
---

# Write ADR

1. Find the next number in `docs/01-architecture/adr/` (`NNNN-kebab-title.md`).
2. Copy [assets/adr-template.md](assets/adr-template.md). Fill every section; do not leave placeholders.
3. **Options**: at least 2 real alternatives, including "do nothing / keep current" when meaningful.
4. **Trade-offs**: compare on the same criteria (complexity, cost, scalability, security/compliance, operability, reversibility).
5. **Decision**: state it in one sentence, then the reason tied to the business driver.
6. **Consequences**: positive, negative, and what would trigger revisiting the decision.
7. Status is `Proposed`. Only the human changes it to `Accepted`.
8. Link the ADR from `docs/01-architecture/architecture.md` §Decisions.
