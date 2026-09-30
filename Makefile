.PHONY: build test test-integration verify up down run lint

# Dev-only DSNs matching docker-compose.yml.
TEST_DB_DSN ?= postgres://auctions:auctions@localhost:5432/auctions?sslmode=disable
TEST_REDIS_ADDR ?= localhost:6379

build:
	go build ./...

# Unit + in-process integration tests (miniredis, fake Kafka). No Docker needed.
test:
	go test -race ./...

# Adds real PostgreSQL and real Redis. Requires `make up`.
test-integration:
	TEST_DB_DSN="$(TEST_DB_DSN)" TEST_REDIS_ADDR="$(TEST_REDIS_ADDR)" go test -race -count=1 -tags=integration ./...

lint:
	gofmt -l . && go vet -tags=integration ./...

verify: build lint test test-integration

up:
	docker compose up -d --wait redis postgres kafka && docker compose run --rm kafka-topics

down:
	docker compose down -v

run:
	set -a && . ./.env && set +a && go run ./cmd/bidding-engine
