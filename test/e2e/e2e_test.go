// Package e2e drives the whole engine in one process: Fiber API -> bidding
// service -> Redis Lua (miniredis) -> Kafka (kfake) -> consumer -> recorder.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Thapanut/bidding-engine/internal/adapter/inbound/httpapi"
	"github.com/Thapanut/bidding-engine/internal/adapter/inbound/kafkaconsumer"
	"github.com/Thapanut/bidding-engine/internal/adapter/outbound/decisiontest"
	"github.com/Thapanut/bidding-engine/internal/adapter/outbound/kafkapublisher"
	"github.com/Thapanut/bidding-engine/internal/adapter/outbound/memory"
	"github.com/Thapanut/bidding-engine/internal/adapter/outbound/redisdecision"
	"github.com/Thapanut/bidding-engine/internal/core/auction/domain"
	"github.com/Thapanut/bidding-engine/internal/core/auction/service"
	"github.com/Thapanut/bidding-engine/internal/kernel"
	"github.com/Thapanut/bidding-engine/internal/platform/system"
	"github.com/Thapanut/bidding-engine/internal/testkit"
)

const (
	poolTimeout = time.Minute
	noTimeout   = -1
)

type engine struct {
	app  *httpapi.Server
	repo *memory.Repository
}

func startEngine(t *testing.T) engine {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	brokers := testkit.Kafka(t)

	redisClient := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr(), PoolTimeout: poolTimeout})
	t.Cleanup(func() { _ = redisClient.Close() })
	publisher, err := kafkapublisher.New(brokers, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(publisher.Close)

	repo := memory.NewRepository()
	consumer, err := kafkaconsumer.New(brokers, "e2e-recorder", service.NewRecorder(repo), log)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		consumer.Close()
	})

	ids, clock := system.UUIDGenerator{}, system.Clock{}
	bidding := service.NewBidding(
		redisdecision.NewStore(redisClient, redisdecision.DefaultRetention),
		publisher, repo, clock, ids)
	return engine{app: httpapi.New(bidding, ids, clock, log), repo: repo}
}

func (e engine) call(method, path string, body any, headers map[string]string, out any) (int, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := e.app.Test(req, noTimeout)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return resp.StatusCode, fmt.Errorf("decode response: %w", err)
	}
	return resp.StatusCode, nil
}

type bidJSON struct {
	BidID         string    `json:"bid_id"`
	AuctionID     string    `json:"auction_id"`
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

type auctionJSON struct {
	CurrentPriceSatang int64     `json:"current_price_satang"`
	HighestBidderID    string    `json:"highest_bidder_id"`
	BidCount           int64     `json:"bid_count"`
	EndsAt             time.Time `json:"ends_at"`
}

// placeOverHTTP adapts the REST API to the shape decisiontest.RaceBidders drives.
func (e engine) placeOverHTTP(_ context.Context, a domain.BidAttempt) (domain.Bid, error) {
	var resp bidJSON
	status, err := e.call(http.MethodPost, "/v1/auctions/"+a.AuctionID+"/bids",
		map[string]any{"bidder_id": a.BidderID, "amount_satang": int64(a.Amount)},
		map[string]string{httpapi.HeaderIdempotencyKey: a.BidID}, &resp)
	switch {
	case err != nil:
		return domain.Bid{}, err
	case status == http.StatusCreated:
		return domain.Bid{
			ID: resp.BidID, AuctionID: resp.AuctionID, BidderID: resp.BidderID,
			Amount: kernel.Money(resp.AmountSatang), Sequence: resp.Sequence,
			PlacedAt: resp.PlacedAt.UTC(), AuctionEndsAt: resp.AuctionEndsAt.UTC(), Extended: resp.Extended,
		}, nil
	case status == http.StatusUnprocessableEntity && resp.Error.Code == "bid_too_low":
		return domain.Bid{}, domain.ErrBidTooLow
	default:
		return domain.Bid{}, fmt.Errorf("unexpected response %d %s", status, resp.Error.Code)
	}
}

// TestAC19 races 2,000 goroutines on one auction through the public API and
// checks that the decision ledger, the live state and the durable audit trail
// all agree once the slow path has drained.
func TestAC19_TwoThousandConcurrentBiddersEndToEnd(t *testing.T) {
	e := startEngine(t)
	now := time.Now()
	opened, err := domain.NewAuction("auction-race", "seller-1", 10_000, 500, now.Add(-time.Hour), now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	var created auctionJSON
	status, err := e.call(http.MethodPost, "/v1/auctions", map[string]any{
		"auction_id": opened.ID, "seller_id": opened.SellerID,
		"start_price_satang": int64(opened.StartPrice), "min_increment_satang": int64(opened.MinIncrement),
		"starts_at": opened.StartsAt, "ends_at": opened.EndsAt,
	}, nil, &created)
	if err != nil || status != http.StatusCreated {
		t.Fatalf("create auction: %d %v", status, err)
	}

	accepted := decisiontest.RaceBidders(t, opened, e.placeOverHTTP)

	var live auctionJSON
	if status, err := e.call(http.MethodGet, "/v1/auctions/"+opened.ID, nil, nil, &live); err != nil || status != http.StatusOK {
		t.Fatalf("get auction: %d %v", status, err)
	}
	decisiontest.AssertLedger(t, opened, accepted, domain.Auction{
		BidSequence: live.BidCount, CurrentPrice: kernel.Money(live.CurrentPriceSatang), HighestBidderID: live.HighestBidderID,
	})

	testkit.Eventually(t, "the audit trail to catch up with the decisions", func() bool {
		return len(e.repo.Bids(opened.ID)) == len(accepted)
	})
	for i, recorded := range e.repo.Bids(opened.ID) {
		if recorded != accepted[i] {
			t.Fatalf("audit[%d] = %+v, want the accepted bid %+v", i, recorded, accepted[i])
		}
	}
	last := accepted[len(accepted)-1]
	testkit.Eventually(t, "the durable auction state to converge", func() bool {
		durable, err := e.repo.GetAuction(context.Background(), opened.ID)
		return err == nil && durable.BidSequence == last.Sequence
	})
	durable, _ := e.repo.GetAuction(context.Background(), opened.ID)
	if durable.CurrentPrice != last.Amount || durable.HighestBidderID != last.BidderID || !durable.EndsAt.Equal(live.EndsAt) {
		t.Fatalf("durable auction = %+v, want it to match live state %+v", durable, live)
	}
}
