# Bidding Engine architecture

## Flow

```
             POST /v1/auctions/{id}/bids
                        |
                 Fiber HTTP adapter
                        |
               service.Bidding (core)
              /                      \
   DecisionStore port          BidEventPublisher port
   Redis + Lua, atomic         Kafka auctions.v1.bid-placed
   accept / reject             key = auction ID, acks=all
                                        |
                              Kafka consumer adapter
                                        |
                              service.Recorder (core)
                                        |
                              AuctionRepository port
                              PostgreSQL: bids + auctions, one transaction
```

A rejected bid stops at Redis. An accepted bid returns `201` after Kafka
acknowledges it. See [ADR-0001](adr/0001-decision-vs-persistence-segregation.md).

## Layout

| Path | Role |
|---|---|
| `internal/kernel` | `Money` (`int64` satang) |
| `internal/core/auction/domain` | Auction, Bid, the bidding rules, errors. Standard library only |
| `internal/core/auction/port` | Inbound (`Bidding`, `BidRecorder`) and outbound (`DecisionStore`, `BidEventPublisher`, `AuctionRepository`, `Clock`, `IDGenerator`) interfaces |
| `internal/core/auction/service` | Use cases; depend on ports only |
| `internal/adapter/inbound/httpapi` | Fiber REST API |
| `internal/adapter/inbound/kafkaconsumer` | bid-placed consumer, retry and dead-letter handling |
| `internal/adapter/outbound/redisdecision` | Lua scripts and the Redis decision store |
| `internal/adapter/outbound/kafkapublisher` | bid-placed producer |
| `internal/adapter/outbound/postgres` | Repository and schema |
| `internal/adapter/outbound/memory` | In-memory adapters; the decision store is the reference for the Lua script |
| `internal/adapter/outbound/decisiontest` | Contract every decision store must pass |
| `internal/adapter/contract/bidplaced` | Wire format of the topic |
| `internal/platform` | Config, clock, ID generator |
| `cmd/bidding-engine` | Composition root |

Dependencies point inward: `core` imports nothing from `adapter` or `platform`.

## Bidding rules

Evaluated in this order, atomically per auction:

1. A bid ID already accepted returns the original decision.
2. The auction must exist.
3. Now must be within `[starts_at, ends_at)` on the Redis clock.
4. The bidder must not be the seller.
5. The bidder must not already hold the highest bid.
6. The amount must be at least the start price (first bid) or the current
   price plus the minimum increment.
7. If the auction ends within 2 minutes, its end moves to now + 2 minutes.

## Redis keys

| Key | Type | Content |
|---|---|---|
| `auction:{<id>}` | hash | seller, prices, highest bidder, sequence, start and end in ms |
| `auction:{<id>}:bids` | hash | bid ID to accepted-bid record; the idempotency ledger |

Both expire 24 hours after the auction's current end. The `{<id>}` hash tag
keeps them in one Redis Cluster slot.

## Runtime

One binary, `RUN_MODE=all|api|consumer`. API instances are stateless and scale
horizontally. Consumer instances share the group
`bidding-engine.bid-recorder`; partitions are processed in parallel, records
within a partition in order.
