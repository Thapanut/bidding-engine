//go:build integration

// Package integration runs the whole engine against real infrastructure:
// PostgreSQL, Redis and Kafka from docker-compose.yml.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/Thapanut/bidding-engine/internal/adapter/inbound/httpapi"
	"github.com/Thapanut/bidding-engine/internal/adapter/inbound/kafkaconsumer"
	"github.com/Thapanut/bidding-engine/internal/adapter/outbound/kafkapublisher"
	"github.com/Thapanut/bidding-engine/internal/adapter/outbound/postgres"
	"github.com/Thapanut/bidding-engine/internal/adapter/outbound/redisdecision"
	"github.com/Thapanut/bidding-engine/internal/core/auction/service"
	"github.com/Thapanut/bidding-engine/internal/platform/system"
)

const (
	dsnEnv     = "TEST_DB_DSN"
	redisEnv   = "TEST_REDIS_ADDR"
	brokersEnv = "TEST_KAFKA_BROKERS"

	concurrentBidders = 5000

	startPriceSatang   int64 = 10_000
	minIncrementSatang int64 = 500

	// maxConnections caps open sockets; the 5,000 goroutines still run
	// concurrently and queue for a connection like clients behind a proxy.
	maxConnections = 512
	// sendAttempts bounds retries of transport failures. A retry reuses the
	// Idempotency-Key, so it can never place a second bid.
	sendAttempts = 5
	retryPause   = 100 * time.Millisecond

	consumerGroup      = "bidding-engine.integration-5000"
	publishTimeout     = time.Minute
	redisPoolTimeout   = time.Minute
	requestTimeout     = 2 * time.Minute
	convergenceTimeout = 2 * time.Minute
	convergencePoll    = 50 * time.Millisecond
)

// ledgerEntry is one accepted bid in a form comparable across the HTTP
// responses, the Redis ledger and the PostgreSQL audit table.
type ledgerEntry struct {
	BidID        string
	BidderID     string
	AmountSatang int64
	Sequence     int64
	PlacedAtMS   int64
	EndsAtMS     int64
	Extended     bool
}

type bidResponse struct {
	BidID         string    `json:"bid_id"`
	BidderID      string    `json:"bidder_id"`
	AmountSatang  int64     `json:"amount_satang"`
	Sequence      int64     `json:"sequence"`
	PlacedAt      time.Time `json:"placed_at"`
	AuctionEndsAt time.Time `json:"auction_ends_at"`
	Extended      bool      `json:"extended"`
	Error         struct {
		Code string `json:"code"`
	} `json:"error"`
}

type stack struct {
	baseURL string
	client  *http.Client
	redis   *redis.Client
	pool    *pgxpool.Pool
}

func requireEnv(t *testing.T, key string) string {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		t.Skipf("%s not set; start the stack with `make up` and run `make test-concurrency`", key)
	}
	return value
}

// startStack wires the production adapters to real infrastructure and serves
// the API on a real TCP listener.
func startStack(t *testing.T) stack {
	t.Helper()
	dsn, redisAddr := requireEnv(t, dsnEnv), requireEnv(t, redisEnv)
	brokers := strings.Split(requireEnv(t, brokersEnv), ",")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	repo := postgres.NewRepository(pool)
	if err := repo.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr, PoolTimeout: redisPoolTimeout})
	t.Cleanup(func() { _ = redisClient.Close() })
	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis: %v", err)
	}

	publisher, err := kafkapublisher.New(brokers, publishTimeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(publisher.Close)

	consumer, err := kafkaconsumer.New(brokers, consumerGroup, service.NewRecorder(repo), log)
	if err != nil {
		t.Fatal(err)
	}
	consumerCtx, stopConsumer := context.WithCancel(ctx)
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		_ = consumer.Run(consumerCtx)
	}()
	t.Cleanup(func() {
		stopConsumer()
		<-consumerDone
		consumer.Close()
	})

	ids, clock := system.UUIDGenerator{}, system.Clock{}
	bidding := service.NewBidding(
		redisdecision.NewStore(redisClient, redisdecision.DefaultRetention),
		publisher, repo, clock, ids)
	app := httpapi.New(bidding, ids, clock, log)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		_ = app.Listener(listener)
	}()
	t.Cleanup(func() {
		_ = app.Shutdown()
		<-serverDone
	})

	transport := &http.Transport{MaxConnsPerHost: maxConnections, MaxIdleConnsPerHost: maxConnections}
	t.Cleanup(transport.CloseIdleConnections)
	return stack{
		baseURL: "http://" + listener.Addr().String(),
		client:  &http.Client{Transport: transport, Timeout: requestTimeout},
		redis:   redisClient,
		pool:    pool,
	}
}

// post sends a JSON request and decodes the response, retrying transport
// failures with the same headers.
func (s stack) post(path string, body any, headers map[string]string, out any) (int, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	var lastErr error
	for range sendAttempts {
		req, err := http.NewRequest(http.MethodPost, s.baseURL+path, bytes.NewReader(payload))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := s.client.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(retryPause)
			continue
		}
		err = json.NewDecoder(resp.Body).Decode(out)
		_ = resp.Body.Close()
		if err != nil {
			return resp.StatusCode, fmt.Errorf("decode response: %w", err)
		}
		return resp.StatusCode, nil
	}
	return 0, fmt.Errorf("gave up after %d attempts: %w", sendAttempts, lastErr)
}

func (s stack) seedAuction(t *testing.T) string {
	t.Helper()
	id := "race-" + uuid.NewString()
	now := time.Now().UTC()
	var created map[string]any
	status, err := s.post("/v1/auctions", map[string]any{
		"auction_id": id, "seller_id": "seller-1",
		"start_price_satang": startPriceSatang, "min_increment_satang": minIncrementSatang,
		"starts_at": now.Add(-time.Hour), "ends_at": now.Add(time.Hour),
	}, nil, &created)
	if err != nil || status != http.StatusCreated {
		t.Fatalf("seed auction: %d %v %v", status, created, err)
	}
	return id
}

// redisLedger reads the accepted-bid ledger the Lua script maintains.
func (s stack) redisLedger(t *testing.T, auctionID string) []ledgerEntry {
	t.Helper()
	records, err := s.redis.HGetAll(context.Background(), "auction:{"+auctionID+"}:bids").Result()
	if err != nil {
		t.Fatalf("read redis ledger: %v", err)
	}
	ledger := make([]ledgerEntry, 0, len(records))
	for bidID, record := range records {
		// seq|bidder_id|amount|placed_at_ms|ends_at_ms|extended
		parts := strings.Split(record, "|")
		if len(parts) != 6 {
			t.Fatalf("malformed redis record %q", record)
		}
		ledger = append(ledger, ledgerEntry{
			BidID: bidID, BidderID: parts[1],
			AmountSatang: atoi(t, parts[2]), Sequence: atoi(t, parts[0]),
			PlacedAtMS: atoi(t, parts[3]), EndsAtMS: atoi(t, parts[4]), Extended: parts[5] == "1",
		})
	}
	sortLedger(ledger)
	return ledger
}

func (s stack) postgresLedger(t *testing.T, auctionID string) []ledgerEntry {
	t.Helper()
	rows, err := s.pool.Query(context.Background(), `
SELECT id, bidder_id, amount_satang, sequence, placed_at, auction_ends_at, extended
FROM bids WHERE auction_id = $1 ORDER BY sequence`, auctionID)
	if err != nil {
		t.Fatalf("read postgres audit: %v", err)
	}
	defer rows.Close()
	var ledger []ledgerEntry
	for rows.Next() {
		var (
			entry            ledgerEntry
			placedAt, endsAt time.Time
		)
		if err := rows.Scan(&entry.BidID, &entry.BidderID, &entry.AmountSatang, &entry.Sequence, &placedAt, &endsAt, &entry.Extended); err != nil {
			t.Fatal(err)
		}
		entry.PlacedAtMS, entry.EndsAtMS = placedAt.UnixMilli(), endsAt.UnixMilli()
		ledger = append(ledger, entry)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ledger
}

func (s stack) postgresBidCount(t *testing.T, auctionID string) (count int) {
	t.Helper()
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM bids WHERE auction_id = $1`, auctionID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func atoi(t *testing.T, raw string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return n
}

func sortLedger(ledger []ledgerEntry) {
	sort.Slice(ledger, func(i, j int) bool { return ledger[i].Sequence < ledger[j].Sequence })
}

func diffLedgers(t *testing.T, gotName string, got []ledgerEntry, wantName string, want []ledgerEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s has %d bids, %s has %d", gotName, len(got), wantName, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s[%d] = %+v, %s[%d] = %+v", gotName, i, got[i], wantName, i, want[i])
		}
	}
}

// TestAC21 fires 5,000 goroutines at one auction over real HTTP against real
// Redis, Kafka and PostgreSQL, then checks that the three views of the
// outcome agree: the responses clients received, the Redis decision layer,
// and the PostgreSQL system of record.
func TestAC21_FiveThousandConcurrentBiddersOnRealInfrastructure(t *testing.T) {
	s := startStack(t)
	auctionID := s.seedAuction(t)

	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		accepted   []ledgerEntry
		rejections = map[string]int{}
		ticket     atomic.Int64
		gate       = make(chan struct{})
		inFlight   = make(chan struct{}, maxConnections)
	)
	for i := range concurrentBidders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			// The amount is drawn when the request gets a connection, so
			// later senders bid higher, as in a live auction. The requests
			// in flight still reach Redis in arbitrary order.
			inFlight <- struct{}{}
			amount := startPriceSatang + ticket.Add(1)*minIncrementSatang
			var resp bidResponse
			status, err := s.post("/v1/auctions/"+auctionID+"/bids",
				map[string]any{"bidder_id": "bidder-" + strconv.Itoa(i), "amount_satang": amount},
				map[string]string{httpapi.HeaderIdempotencyKey: uuid.NewString()}, &resp)
			<-inFlight

			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				t.Errorf("bidder %d: %v", i, err)
			case status == http.StatusCreated:
				accepted = append(accepted, ledgerEntry{
					BidID: resp.BidID, BidderID: resp.BidderID, AmountSatang: resp.AmountSatang, Sequence: resp.Sequence,
					PlacedAtMS: resp.PlacedAt.UnixMilli(), EndsAtMS: resp.AuctionEndsAt.UnixMilli(), Extended: resp.Extended,
				})
			case status == http.StatusUnprocessableEntity && resp.Error.Code == "bid_too_low":
				rejections[resp.Error.Code]++
			default:
				t.Errorf("bidder %d: unexpected response %d %q", i, status, resp.Error.Code)
			}
		}()
	}
	started := time.Now()
	close(gate)
	wg.Wait()
	elapsed := time.Since(started)
	if t.Failed() {
		t.FailNow()
	}
	sortLedger(accepted)
	if len(accepted) == 0 {
		t.Fatal("no bid was accepted")
	}
	t.Logf("%d bidders in %v (%.0f req/s): %d accepted, %d rejected as too low",
		concurrentBidders, elapsed.Round(time.Millisecond), float64(concurrentBidders)/elapsed.Seconds(),
		len(accepted), rejections["bid_too_low"])

	// (b) Gapless sequences, each accepted bid at least one increment above
	// the previous, and the highest amount submitted wins.
	for i, bid := range accepted {
		if bid.Sequence != int64(i+1) {
			t.Fatalf("accepted[%d].Sequence = %d, want gapless %d", i, bid.Sequence, i+1)
		}
		minRequired := startPriceSatang
		if i > 0 {
			minRequired = accepted[i-1].AmountSatang + minIncrementSatang
		}
		if bid.AmountSatang < minRequired {
			t.Fatalf("sequence %d accepted %d satang, below the minimum %d", bid.Sequence, bid.AmountSatang, minRequired)
		}
	}
	winner := accepted[len(accepted)-1]
	if want := startPriceSatang + concurrentBidders*minIncrementSatang; winner.AmountSatang != want {
		t.Fatalf("winning amount = %d, want the highest submitted %d", winner.AmountSatang, want)
	}

	// Redis: the ledger equals what clients were told, and the auction hash
	// reflects the last accepted bid.
	diffLedgers(t, "redis ledger", s.redisLedger(t, auctionID), "http responses", accepted)
	live, err := s.redis.HGetAll(context.Background(), "auction:{"+auctionID+"}").Result()
	if err != nil {
		t.Fatal(err)
	}
	if atoi(t, live["seq"]) != winner.Sequence || atoi(t, live["current_price"]) != winner.AmountSatang ||
		live["highest_bidder"] != winner.BidderID || atoi(t, live["ends_at_ms"]) != winner.EndsAtMS {
		t.Fatalf("redis auction = %v, want it to reflect the winner %+v", live, winner)
	}

	// (c) PostgreSQL audit: exactly the accepted bids, once the consumer drains.
	deadline := time.Now().Add(convergenceTimeout)
	for s.postgresBidCount(t, auctionID) < len(accepted) {
		if time.Now().After(deadline) {
			t.Fatalf("audit has %d of %d bids after %v", s.postgresBidCount(t, auctionID), len(accepted), convergenceTimeout)
		}
		time.Sleep(convergencePoll)
	}
	t.Logf("audit caught up %v after the last response", time.Since(started.Add(elapsed)).Round(time.Millisecond))
	diffLedgers(t, "postgres bids", s.postgresLedger(t, auctionID), "redis ledger", accepted)

	// (d) PostgreSQL auction row matches the Redis decision layer.
	var (
		price, sequence int64
		highestBidder   string
		endsAt          time.Time
	)
	err = s.pool.QueryRow(context.Background(), `
SELECT current_price_satang, highest_bidder_id, bid_sequence, ends_at FROM auctions WHERE id = $1`, auctionID).
		Scan(&price, &highestBidder, &sequence, &endsAt)
	if err != nil {
		t.Fatal(err)
	}
	if price != winner.AmountSatang || highestBidder != winner.BidderID || sequence != winner.Sequence || endsAt.UnixMilli() != winner.EndsAtMS {
		t.Fatalf("postgres auction = price %d bidder %s sequence %d ends %v, want the winner %+v", price, highestBidder, sequence, endsAt, winner)
	}
}
