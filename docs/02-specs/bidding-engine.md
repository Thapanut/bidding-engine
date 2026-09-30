# Spec: Bidding Engine v1

Scope: place bids on timed auctions, report auction status, list auctions and
recent bids, stream live auction state, and a demo bidding page. Out of scope:
authentication, auction closing and winner settlement, full bid history paging.

Contracts: [openapi.yaml](../../contracts/openapi.yaml),
[asyncapi.yaml](../../contracts/asyncapi.yaml).

## Acceptance criteria

Each criterion names the tests that cover it (`TestACnn_*` or the `ACnn_*`
subtests of the decision-store contract).

| ID | Criterion |
|---|---|
| AC01 | The first bid at or above the start price is accepted with sequence 1 |
| AC02 | A bid on an unknown auction is rejected |
| AC03 | A bid before `starts_at` or at/after `ends_at` is rejected and changes nothing |
| AC04 | The seller cannot bid on their own auction |
| AC05 | The current highest bidder cannot outbid themselves |
| AC06 | A later bid must be at least current price + minimum increment; the rejection states the minimum |
| AC07 | A bid within 2 minutes of the end moves the end to bid time + 2 minutes; an earlier bid does not |
| AC08 | Replaying a bid ID returns the original decision; reusing it for a different bid is rejected |
| AC09 | Re-opening an auction never resets its live state |
| AC10 | 2,000 concurrent bidders on one auction produce gapless sequences, every accepted bid at least one increment above the previous, and the highest submitted amount wins |
| AC11 | Creating an auction stores it durably and opens it; repeating the same ID returns the existing auction |
| AC12 | If publishing fails the client gets `503` with the `Idempotency-Key`; retrying with it publishes the original decision once |
| AC13 | Recording a bid is idempotent and tolerates out-of-order delivery |
| AC14 | `POST /v1/auctions/{id}/bids` and `GET /v1/auctions/{id}` return the contract shapes |
| AC15 | Every rejection maps to its contract status and error code; fractional money and unknown fields are rejected |
| AC16 | Duplicated and reordered Kafka events produce exactly one audit row per bid; offsets are committed |
| AC17 | Invalid, orphaned and sequence-conflicting events go to the dead-letter topic without blocking valid ones |
| AC18 | A transient persistence failure is retried, not dropped |
| AC19 | 2,000 concurrent bidders through the HTTP API: live state, decision ledger and durable audit agree |
| AC20 | PostgreSQL repository: round trip, idempotent and order-tolerant apply, rollback on conflict, 2,000 concurrent redeliveries converge (`-tags=integration`) |
| AC21 | 5,000 concurrent bidders over HTTP on real Redis, Kafka and PostgreSQL: gapless sequences, and the Redis ledger, the `bids` audit and the `auctions` row all match (`-tags=integration`, `make test-concurrency`) |
| AC22 | The decision store keeps the latest 50 accepted bids per auction, newest first |
| AC23 | Auctions list newest first with live state overlaid; recent bids come from the live feed, or from the audit trail once live state has expired |
| AC24 | `GET /v1/auctions` and `GET /v1/auctions/{id}/bids` return the contract shapes; an auction may carry an optional title of up to 120 characters |
| AC25 | `GET /` serves the Bid Arena page |
| AC26 | `GET /v1/auctions/{id}/events` streams a snapshot on connect, on every change (at most every 100 ms) and at least every second; unknown auctions get `404`; shutdown ends open streams |

## Non-functional

- All money is `int64` satang, 1 to 2^52.
- `go test -race ./...` passes with no external services.
