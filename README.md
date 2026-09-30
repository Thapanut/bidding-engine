# Bidding Engine

A Go service that decides auction bids atomically in Redis and persists them
asynchronously through Kafka to PostgreSQL.

- **Fast path:** a Redis Lua script validates each bid (active auction, no
  self-bidding, minimum increment) and applies the 2-minute anti-sniping
  extension, atomically per auction.
- **Slow path:** accepted bids go to `auctions.v1.bid-placed`; a consumer
  writes the bid audit and the auction state to PostgreSQL idempotently.
- **Bid Arena:** a live bidding page at `/`, fed by server-sent events, for
  watching up to 5,000 k6 bots fight over one auction and bidding against them.
- All money is `int64` satang.

Design: [architecture](docs/01-architecture/architecture.md),
[ADR-0001](docs/01-architecture/adr/0001-decision-vs-persistence-segregation.md)
(including known gaps), [spec](docs/02-specs/bidding-engine.md).
Contracts: [OpenAPI](contracts/openapi.yaml), [AsyncAPI](contracts/asyncapi.yaml).

## Run

Requires Go 1.24+ and Docker.

```sh
make up                 # Redis, PostgreSQL, Kafka + topics
cp .env.example .env
make run                # API on :8080 and the consumer, in one process
```

If a default port is taken, move it: `POSTGRES_PORT=55432 make up` (also
`REDIS_PORT`, `KAFKA_PORT`), and update `.env` to match.

```sh
curl -X POST localhost:8080/v1/auctions -H 'Content-Type: application/json' -d '{
  "auction_id": "demo", "seller_id": "seller-1",
  "start_price_satang": 10000, "min_increment_satang": 500,
  "starts_at": "2026-01-01T00:00:00Z", "ends_at": "2030-01-01T00:00:00Z"}'

curl -X POST localhost:8080/v1/auctions/demo/bids \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: bid-1' \
  -d '{"bidder_id": "bidder-1", "amount_satang": 10000}'

curl localhost:8080/v1/auctions/demo
```

## Bid Arena (live demo page)

Open `http://localhost:8080/` (or your `HTTP_ADDR`) after `make run`.

1. Enter a bidder name (letters, digits, `-`, `_`).
2. Open an auction from the form, or pick one from the list.
3. Copy the command shown on the page into a terminal, for example
   `make loadtest-k6 AUCTION_ID=<id>`, to release 5,000 bots onto that auction.
4. Watch the price, bids per second and the bid feed move, and bid against the
   bots. They bid at most 10 increments above the minimum, so the `+100` and
   `+1,000` buttons are the way to overtake them.

While bots keep bidding inside the last 2 minutes the auction keeps extending,
which is the anti-sniping rule at work. It ends 2 minutes after the bots stop.

The page receives coalesced snapshots, at most every 100 ms plus a 1-second
heartbeat, from `GET /v1/auctions/{id}/events`. The server samples each watched
auction once per interval regardless of how many viewers it has.

The page has no login: anyone can bid under any name. It is a demo, not a
production front end.

## Configuration

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `RUN_MODE` | no | `all` | `all`, `api` or `consumer` |
| `HTTP_ADDR` | no | `:8080` | API listen address |
| `REDIS_ADDR` | API | | Redis `host:port` |
| `KAFKA_BROKERS` | yes | | Comma-separated brokers |
| `KAFKA_CONSUMER_GROUP` | no | `bidding-engine.bid-recorder` | Consumer group |
| `DATABASE_URL` | yes | | PostgreSQL URL |

The schema is created on start. Topics are not: create
`auctions.v1.bid-placed` and `auctions.v1.bid-placed-dlq` (`make up` does).

## Test

```sh
make test               # go test -race ./...; no external services
make test-integration   # adds real PostgreSQL, Redis and Kafka; needs `make up`
make test-concurrency   # only the 5,000-goroutine test on the real stack
```

`make test` runs the Lua scripts on miniredis and Kafka on an in-process fake,
including two races of 2,000 goroutines on one auction: against the decision
store directly and end to end through the HTTP API.

`make test-concurrency` fires 5,000 goroutines at one auction over real HTTP
and checks that the responses, the Redis ledger, the PostgreSQL `bids` audit
and the `auctions` row all agree.

Settings come from `.env` when it exists, so the tests use the same ports as
`make run`.

## Load test

Requires [k6](https://k6.io) (`brew install k6`) and a running engine.

```sh
make up
make run                              # terminal 1
make loadtest-k6                      # terminal 2: 5,000 VUs, 30s ramp, 1m hold
make loadtest-k6 VUS=500 HOLD=30s     # smaller run
make loadtest-k6 BASE_URL=http://host:8080
```

Add `AUCTION_ID=<id>` to bid on an existing auction, such as one open in the
Bid Arena page, instead of creating a new one.

Each virtual user reads the auction status, bids at or just above the minimum
next bid with a unique `Idempotency-Key`, and pauses 0.5-1.5 s. The run fails
if any response is a 5xx, any response is outside the contract, or bid or
status latency exceeds p95 500 ms / p99 1 s. The summary reports requests per
second, bids per second, and p95/p99 latency.

Accepted bids return `201`; being outbid returns `422 bid_too_low`, and bidding
while already leading returns `409 already_highest_bidder`. The script counts
those as valid outcomes.

Under sustained load the audit trail in PostgreSQL trails Redis, because one
auction maps to one Kafka partition and its bids are written one transaction
at a time. It catches up after the load stops.
