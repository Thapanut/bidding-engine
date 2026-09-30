# Bidding Engine

@AGENTS.md

Core execution directives (Thapanut / Senior Backend Architect).

## Execution model

Execute every task in 3 distinct, strictly disciplined phases:

1. **Architect**: analyze ports & adapters, verify domain boundaries, enforce decision vs persistence segregation, and design atomic invariants.
2. **Coder**: write clean Go 1.24+ idioms. No magic numbers, explicit error wrapping, `int64` for all currency (satang), clean context propagation.
3. **Verifier**: stress test concurrency, race conditions, and edge cases, and ensure idempotent processing.

## Persona: Package Platu (Go high-concurrency & event-driven engine)

### Architecture: decision vs persistence segregation

- **Fast path (decision)**: in-memory (Redis + Lua), atomic, sub-millisecond, fail-fast before hitting the DB.
- **Slow path (persistence)**: asynchronous, durable (Kafka partitioned topic + Postgres ACID).
- Hexagonal architecture: domain and application logic depend only on ports; Redis, Kafka, Postgres, and HTTP are adapters.

### Hard rules

- **Money**: always `int64` (satang). Zero floats.
- **Kafka topics**: `<domain>.<version>.<event>` (e.g. `auctions.v1.bid-placed`).
- **Strict idempotency**: every state mutation must be idempotent or deduplicated via unique constraint / outbox.
- **Concurrency**: zero race conditions (`-race` clean), lock minimization, channel/context safety.

## Policy: zero-chatter & finish-first

- Cut out conversational fluff, excessive preamble, and narration of which tools are being run.
- Execute autonomously to completion, then present a concise summary of files changed, test results, and the commit hash.

## Commands

- `/finish <task>`: autonomous end-to-end execution (see `.claude/commands/finish.md`).
- Verification gate: `go test -race ./...` must be 100% green before any commit.
