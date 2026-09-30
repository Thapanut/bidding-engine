# AGENTS.md — Project Operating Manual for AI Agents

> Single source of truth for every AI tool (Antigravity, Copilot in VS Code, Claude Code, Codex).
> `CLAUDE.md` and `.github/copilot-instructions.md` only point here.

## 1. Roles (non-negotiable)

| Who | Owns |
|---|---|
| **Human (Architect / Owner)** | Business problem, architecture, ADRs, interfaces (OpenAPI/AsyncAPI), data model, specs & acceptance criteria, final approval |
| **AI Agent (Implementer)** | Code, tests, migrations, docs, IaC — **only** within an approved spec |

- The AI **never** changes architecture, public contracts, DB schema, or security controls on its own. If the spec requires it, STOP and propose an ADR / contract change instead.
- The human is accountable for everything merged. AI output is a draft until verified.

## 2. Source of truth (read before any task)

1. `docs/00-business/problem-statement.md` — why we build this, success metrics, constraints
2. `docs/01-architecture/architecture.md` — context, containers, key flows, NFRs
3. `docs/01-architecture/adr/` — decisions already made (do not re-litigate)
4. `contracts/` — OpenAPI / AsyncAPI are the API truth; code must conform
5. `docs/02-specs/<feature>.md` — the task you are implementing

If these conflict: contracts > ADR > spec > architecture doc > your assumptions. Report conflicts; never silently pick one.

## 3. Workflow (skills, invoked as slash commands)

`/design` → `/spec` → `/implement` → `/verify`  (see `.agents/skills/`; helpers: `write-adr`, `threat-model`)

- One spec = one branch = one PR. Small, reviewable diffs.
- Every task ends with the **Verification Report** (see skill `verify`).

## 4. Tech defaults (override via ADR)

- **Go 1.24+** (`go 1.24` in `go.mod`; do not add dependencies that need a newer Go), **Fiber v2**, PostgreSQL via **pgx** (ADR-0001), Kafka via **franz-go**, Redis via **go-redis**
- Kafka topics are named `<domain>.<version>.<event>` (e.g. `auctions.v1.bid-placed`)
- **Decision vs persistence segregation (ADR-0001)** — bids are decided atomically in Redis + Lua, then persisted asynchronously through Kafka to PostgreSQL
- **Hexagonal architecture** — dependencies point inward only:
  - `internal/core/auction/` the auction hexagon:
    - `domain` entities, value objects, errors — stdlib + `internal/kernel` only
    - `port` inbound (use-case) and outbound (decision store, publisher, repository, clock, id) interfaces
    - `service` use cases; depend on ports only
  - `internal/kernel` shared kernel (`Money`) — stdlib only
  - `internal/adapter/inbound/*` (HTTP, Kafka consumer) → call inbound ports; `internal/adapter/outbound/*` (redis, kafka, postgres, memory) → implement outbound ports
  - `cmd/bidding-engine` is the only composition root; `internal/platform/*` holds config, clock and ID generation
- Money: `int64` satang (`kernel.Money`); never float
- Config via env vars; no hard-coded URLs, credentials, or tenant data
- Errors: wrap with context, map to contract error codes; never leak internals to clients
- Logging: structured JSON, include `trace_id`; **never** log PII, tokens, card/account numbers

## 5. Commands

| Purpose | Command |
|---|---|
| Full verification gate | `make verify` |
| Tests, no external services | `make test` (`go test -race ./...`) |
| Integration tests (real PostgreSQL + Redis) | `make up` then `make test-integration` |
| Start / stop local dependencies | `make up` / `make down` |
| Run locally | `make run` |
| Format + vet | `make lint` |

Run `make verify` before claiming a task is done. `NOT RUN` counts as failure. Paste the real output — do not summarize a run you did not execute.

## 6. Hard rules

- Do not invent APIs, tables, env vars, or library functions. If unknown → mark `UNCONFIRMED` and ask.
- Do not add dependencies without stating why in the PR (license + maintenance).
- Do not disable tests, linters, or security checks to make things pass.
- Do not touch `contracts/`, `docs/01-architecture/`, migrations of released versions unless the spec explicitly allows it.
- Temp scripts go in `tmp/` and are deleted after use.
- Answer the human in Thai unless asked otherwise; code, commits, and docs in English.
