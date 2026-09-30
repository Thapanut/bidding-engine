package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Thapanut/bidding-engine/internal/adapter/outbound/memory"
	"github.com/Thapanut/bidding-engine/internal/core/auction/domain"
	"github.com/Thapanut/bidding-engine/internal/core/auction/port"
	"github.com/Thapanut/bidding-engine/internal/core/auction/service"
	"github.com/Thapanut/bidding-engine/internal/kernel"
	"github.com/Thapanut/bidding-engine/internal/platform/system"
)

type fixture struct {
	bidding   *service.Bidding
	recorder  *service.Recorder
	decisions *memory.DecisionStore
	publisher *memory.Publisher
	repo      *memory.Repository
}

func newFixture() fixture {
	clock := system.Clock{}
	f := fixture{
		decisions: memory.NewDecisionStore(clock),
		publisher: memory.NewPublisher(),
		repo:      memory.NewRepository(),
	}
	f.bidding = service.NewBidding(f.decisions, f.publisher, f.repo, clock, system.UUIDGenerator{})
	f.recorder = service.NewRecorder(f.repo)
	return f
}

func (f fixture) openAuction(t *testing.T, id string) port.AuctionView {
	t.Helper()
	now := time.Now()
	view, err := f.bidding.CreateAuction(context.Background(), port.CreateAuctionCommand{
		AuctionID: id, SellerID: "seller-1", StartPrice: 10_000, MinIncrement: 500,
		StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("create auction: %v", err)
	}
	return view
}

func TestAC11_CreateAuctionStoresThenOpens(t *testing.T) {
	f := newFixture()
	ctx := context.Background()
	view := f.openAuction(t, "")

	if view.Status != domain.StatusActive || view.Auction.ID == "" {
		t.Fatalf("view = %+v, want an active auction with a generated id", view)
	}
	if _, err := f.repo.GetAuction(ctx, view.Auction.ID); err != nil {
		t.Fatalf("auction not in the system of record: %v", err)
	}
	if _, err := f.decisions.GetAuction(ctx, view.Auction.ID); err != nil {
		t.Fatalf("auction not open for bidding: %v", err)
	}
}

func TestAC11_CreateAuctionIsIdempotentAndNeverResetsBids(t *testing.T) {
	f := newFixture()
	ctx := context.Background()
	f.openAuction(t, "auction-1")
	bid, err := f.bidding.PlaceBid(ctx, port.PlaceBidCommand{AuctionID: "auction-1", BidderID: "bidder-1", IdempotencyKey: "bid-1", Amount: 10_000})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.recorder.RecordBid(ctx, bid); err != nil {
		t.Fatal(err)
	}

	again := f.openAuction(t, "auction-1")
	if again.Auction.BidSequence != 1 || again.Auction.HighestBidderID != "bidder-1" {
		t.Fatalf("re-create returned %+v, want the existing auction with its bid", again.Auction)
	}
}

func TestAC12_PublishFailureIsRecoveredByRetryWithSameKey(t *testing.T) {
	f := newFixture()
	ctx := context.Background()
	f.openAuction(t, "auction-1")
	cmd := port.PlaceBidCommand{AuctionID: "auction-1", BidderID: "bidder-1", IdempotencyKey: "bid-1", Amount: 10_000}

	f.publisher.FailWith(errors.New("broker down"))
	if _, err := f.bidding.PlaceBid(ctx, cmd); !errors.Is(err, domain.ErrBidNotDurable) {
		t.Fatalf("err = %v, want ErrBidNotDurable", err)
	}

	f.publisher.FailWith(nil)
	bid, err := f.bidding.PlaceBid(ctx, cmd)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	published := f.publisher.Published()
	if len(published) != 1 || published[0] != bid || bid.Sequence != 1 {
		t.Fatalf("published = %+v, want exactly the original decision %+v", published, bid)
	}
}

func TestPlaceBid_RejectionPublishesNothing(t *testing.T) {
	f := newFixture()
	f.openAuction(t, "auction-1")
	_, err := f.bidding.PlaceBid(context.Background(), port.PlaceBidCommand{AuctionID: "auction-1", BidderID: "seller-1", IdempotencyKey: "bid-1", Amount: 10_000})
	if !errors.Is(err, domain.ErrSelfBidding) {
		t.Fatalf("err = %v, want ErrSelfBidding", err)
	}
	if n := len(f.publisher.Published()); n != 0 {
		t.Fatalf("published %d events for a rejected bid", n)
	}
}

func TestGetAuction_FallsBackToSystemOfRecord(t *testing.T) {
	f := newFixture()
	ctx := context.Background()
	now := time.Now()
	stored, err := domain.NewAuction("archived", "seller-1", 10_000, 500, now.Add(-2*time.Hour), now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.repo.CreateAuction(ctx, stored); err != nil {
		t.Fatal(err)
	}

	view, err := f.bidding.GetAuction(ctx, "archived")
	if err != nil || view.Status != domain.StatusEnded {
		t.Fatalf("view = %+v, err = %v, want the ended auction from the repository", view, err)
	}
	if _, err := f.bidding.GetAuction(ctx, "missing"); !errors.Is(err, domain.ErrAuctionNotFound) {
		t.Fatalf("err = %v, want ErrAuctionNotFound", err)
	}
}

func TestAC13_RecordBidIsIdempotentAndOrderTolerant(t *testing.T) {
	f := newFixture()
	ctx := context.Background()
	f.openAuction(t, "auction-1")
	var bids []domain.Bid
	for i, bidder := range []string{"bidder-1", "bidder-2", "bidder-3"} {
		bid, err := f.bidding.PlaceBid(ctx, port.PlaceBidCommand{
			AuctionID: "auction-1", BidderID: bidder, IdempotencyKey: "bid-" + bidder, Amount: domainAmount(i),
		})
		if err != nil {
			t.Fatal(err)
		}
		bids = append(bids, bid)
	}

	// Delivered out of order and with duplicates.
	for _, i := range []int{2, 0, 2, 1, 0, 1} {
		if err := f.recorder.RecordBid(ctx, bids[i]); err != nil {
			t.Fatalf("record bid %d: %v", i, err)
		}
	}

	audit := f.repo.Bids("auction-1")
	if len(audit) != len(bids) {
		t.Fatalf("audit has %d rows, want %d", len(audit), len(bids))
	}
	stored, _ := f.repo.GetAuction(ctx, "auction-1")
	if stored.BidSequence != 3 || stored.CurrentPrice != bids[2].Amount || stored.HighestBidderID != "bidder-3" {
		t.Fatalf("auction = %+v, want the state after the highest sequence", stored)
	}
}

func TestRecordBid_PermanentFailures(t *testing.T) {
	f := newFixture()
	ctx := context.Background()
	if err := f.recorder.RecordBid(ctx, domain.Bid{}); !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("err = %v, want ErrValidation", err)
	}
	orphan := domain.Bid{ID: "bid-1", AuctionID: "missing", BidderID: "bidder-1", Amount: 1, Sequence: 1, PlacedAt: time.Now(), AuctionEndsAt: time.Now()}
	if err := f.recorder.RecordBid(ctx, orphan); !errors.Is(err, domain.ErrAuctionNotFound) {
		t.Fatalf("err = %v, want ErrAuctionNotFound", err)
	}
}

func domainAmount(step int) kernel.Money { return 10_000 + kernel.Money(step)*500 }
