# ADR-0001: Decide bids in Redis, persist them through Kafka to PostgreSQL

- Status: Accepted
- Date: 2026-09-30

## Context

Many bidders race on one auction, most intensely in its last seconds. Each bid
must be checked against the current price, and only one of two competing bids
may win. Doing that check in PostgreSQL puts a row lock on the hottest row in
the system and ties bid latency to disk commits.

## Decision

Split every bid into two paths.

**Fast path (decision).** Live auction state lives in a Redis hash. One Lua
script (`place_bid.lua`) validates and applies a bid atomically: active window,
seller not bidding on own auction, current leader not outbidding themselves,
minimum increment, and the 2-minute anti-sniping extension. It rejects before
anything touches Kafka or the database.

**Slow path (persistence).** An accepted bid is produced to
`auctions.v1.bid-placed`, keyed by auction ID. A consumer group writes the bid
audit row and advances the auction row in one PostgreSQL transaction.

Supporting decisions:

| Topic | Decision | Why |
|---|---|---|
| Clock | The script reads Redis `TIME` | One clock for all decisions; app-server skew cannot reopen an ended auction |
| Idempotency, fast path | Accepted bids are stored in a per-auction hash keyed by bid ID (the `Idempotency-Key`) | A retry returns the original decision instead of being re-evaluated |
| Idempotency, slow path | `bids` primary key `(auction_id, id)`; auction update guarded by `bid_sequence < $new` | Redelivery and out-of-order delivery are harmless |
| Ordering | Each accepted bid carries a gapless per-auction `sequence` assigned in the script | Consumers order by sequence, not by Kafka offset, because two API instances can produce out of decision order |
| Publish | Synchronous, idempotent producer, `acks=all`; the HTTP request waits for the ack | The client gets `201` only when the bid is durable in Kafka |
| Consumer failures | Transient errors retry with backoff and block the partition; invalid, orphaned or sequence-conflicting events go to `auctions.v1.bid-placed-dlq` | An audit record is never dropped silently |
| Money | `int64` satang everywhere, capped at 2^52 | Lua numbers are doubles; the cap keeps `current + increment` exact |
| Topic names | `<domain>.<version>.<event>` | Project rule |
| PostgreSQL access | `pgx` with hand-written SQL, not GORM | The idempotent upsert and the sequence-guarded update are two explicit statements; overrides the starter's GORM default |
| Go version | Go 1.24; dependencies pinned to releases that build on it (`franz-go` 1.19.5, `pgx` 5.7.6) | Project rule; newer releases of both require Go 1.25+ |

## Consequences

- Bid latency is one Redis round trip plus one Kafka ack. PostgreSQL is not on
  the request path.
- PostgreSQL trails Redis by the consumer lag. `GET /v1/auctions/{id}` reads
  Redis and falls back to PostgreSQL once the live state has expired
  (24 hours after the auction ends).
- The in-memory decision store and the Lua script implement the same rules
  twice. `internal/adapter/outbound/decisiontest` runs one contract against
  both, and against a real Redis when `TEST_REDIS_ADDR` is set.

## Known gaps

1. **Accept-then-publish is not atomic.** If the API process dies after Redis
   accepts a bid and before Kafka acks it, the bid is live in Redis but absent
   from Kafka. A client retry with the same `Idempotency-Key` republishes it; a
   client that never retries leaves Redis and PostgreSQL diverged, visible as a
   sequence gap in `bids`. Closing this needs an outbox written by the same Lua
   script (for example a Redis Stream) and a relay.
2. **Redis is authoritative for live auctions.** Losing Redis data mid-auction
   loses decisions that were not yet published. Run Redis with AOF and a
   replica. There is no rebuild of a live auction from PostgreSQL.
3. **No authentication.** `seller_id` and `bidder_id` are taken from the
   request body. The service must sit behind a gateway that authenticates the
   caller and sets those fields, or gain its own auth middleware.
4. **No auction closing event.** An auction ends by time alone; nothing emits a
   winner or settles payment.
