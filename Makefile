.PHONY: build test test-integration test-concurrency verify up down run lint loadtest loadtest-k6

# Local settings (ports, DSNs) come from .env when it exists; see .env.example.
-include .env

# Dev-only defaults matching docker-compose.yml.
DATABASE_URL  ?= postgres://auctions:auctions@localhost:5432/auctions?sslmode=disable
REDIS_ADDR    ?= localhost:6379
KAFKA_BROKERS ?= localhost:9092
HTTP_ADDR     ?= :8080

TEST_DB_DSN        ?= $(DATABASE_URL)
TEST_REDIS_ADDR    ?= $(REDIS_ADDR)
TEST_KAFKA_BROKERS ?= $(KAFKA_BROKERS)
INTEGRATION_ENV     = TEST_DB_DSN="$(TEST_DB_DSN)" TEST_REDIS_ADDR="$(TEST_REDIS_ADDR)" TEST_KAFKA_BROKERS="$(TEST_KAFKA_BROKERS)"

# Load test settings; override on the command line, e.g. `make loadtest-k6 VUS=500 HOLD=30s`.
BASE_URL  ?= http://localhost$(HTTP_ADDR)
VUS       ?= 5000
RAMP_UP   ?= 30s
HOLD      ?= 1m
RAMP_DOWN ?= 10s
# Set to bid on an existing auction, e.g. the one open in the Bid Arena page.
AUCTION_ID ?=

build:
	go build ./...

# Unit + in-process integration tests (miniredis, fake Kafka). No Docker needed.
test:
	go test -race ./...

# Adds real PostgreSQL, Redis and Kafka. Requires `make up`.
test-integration:
	$(INTEGRATION_ENV) go test -race -count=1 -tags=integration ./...

# 5,000 goroutines bidding on one auction over HTTP against the real stack.
# Requires `make up`.
test-concurrency:
	$(INTEGRATION_ENV) go test -race -count=1 -tags=integration -v -run TestAC21 ./test/integration/

# k6 load test, 5,000 virtual users by default. Requires k6 (`brew install k6`)
# and a running engine: `make up`, then `make run` in another terminal.
# 5,000 VUs need a raised file-descriptor limit, which the recipe sets.
loadtest-k6:
	ulimit -n 65535 2>/dev/null; \
	BASE_URL="$(BASE_URL)" VUS="$(VUS)" RAMP_UP="$(RAMP_UP)" HOLD="$(HOLD)" RAMP_DOWN="$(RAMP_DOWN)" AUCTION_ID="$(AUCTION_ID)" \
		k6 run scripts/loadtest/bidding_5000_vus.js

loadtest: loadtest-k6

lint:
	gofmt -l . && go vet -tags=integration ./...

verify: build lint test test-integration

up:
	docker compose up -d --wait redis postgres kafka && docker compose run --rm kafka-topics

down:
	docker compose down -v

run:
	set -a && . ./.env && set +a && go run ./cmd/bidding-engine
