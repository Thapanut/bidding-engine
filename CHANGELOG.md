# Changelog

## Unreleased

### Added

- Bidding engine v1: Redis + Lua decision layer, Kafka `auctions.v1.bid-placed`
  producer and consumer with dead-letter topic, PostgreSQL bid audit and
  auction state, Fiber REST API (`POST /v1/auctions`, `GET /v1/auctions/{id}`,
  `POST /v1/auctions/{id}/bids`).
- ADR-0001, architecture doc, v1 spec, OpenAPI and AsyncAPI contracts.
