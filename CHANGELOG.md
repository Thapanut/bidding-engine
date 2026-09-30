# Changelog

## Unreleased

### Added

- Bidding engine v1: Redis + Lua decision layer, Kafka `auctions.v1.bid-placed`
  producer and consumer with dead-letter topic, PostgreSQL bid audit and
  auction state, Fiber REST API (`POST /v1/auctions`, `GET /v1/auctions/{id}`,
  `POST /v1/auctions/{id}/bids`).
- Bid Arena demo page at `/`, live auction stream
  `GET /v1/auctions/{id}/events` (server-sent events), `GET /v1/auctions`,
  `GET /v1/auctions/{id}/bids`, and an optional auction `title`.
- `make loadtest-k6 AUCTION_ID=<id>` to aim the bots at an existing auction.
- `make test-concurrency`: 5,000-goroutine integration test against real
  PostgreSQL, Redis and Kafka.
- `make loadtest-k6`: k6 load test with 5,000 virtual users
  (`scripts/loadtest/bidding_5000_vus.js`).
- ADR-0001, architecture doc, v1 spec, OpenAPI and AsyncAPI contracts.
