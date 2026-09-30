# Bidding Engine

A Go service that decides auction bids atomically in Redis and persists them
asynchronously through Kafka to PostgreSQL.

- **Fast path:** a Redis Lua script validates each bid (active auction, no
  self-bidding, minimum increment) and applies the 2-minute anti-sniping
  extension, atomically per auction.
- **Slow path:** accepted bids go to `auctions.v1.bid-placed`; a consumer
  writes the bid audit and the auction state to PostgreSQL idempotently.
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
make test-integration   # adds real PostgreSQL and Redis; needs `make up`
```

`make test` runs the Lua scripts on miniredis and Kafka on an in-process fake,
including two races of 2,000 goroutines on one auction: against the decision
store directly and end to end through the HTTP API.
