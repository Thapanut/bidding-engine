---
description: Autonomous end-to-end execution — implement, test, fix, document, commit
argument-hint: <task description>
---

Execute the following task autonomously, end to end:

$ARGUMENTS

## Rules

- **Zero interruption**: do NOT pause to ask for confirmation or report partial steps.
- **Full lifecycle**, in order:
  1. Implement the code.
  2. Write or update unit and integration tests.
  3. Run `go test -race ./...`.
  4. Fix any failures autonomously and re-run until green.
  5. Update the relevant ADR / docs.
  6. `git add` and `git commit`.
- **Completion criteria**: stop and respond ONLY when all tests pass (100% green) and the git commit has been created.

## Execution phases

Work through the three phases defined in CLAUDE.md (Architect -> Coder -> Verifier) and obey every hard rule there.

## Final response

A concise summary only: files changed, test results, and the commit hash.
