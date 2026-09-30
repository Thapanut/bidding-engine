// Package decisiontest is the behavioural contract every port.DecisionStore
// adapter must satisfy. The in-memory reference and the Redis Lua script run
// the same suite, which is what keeps the two in step.
package decisiontest

import (
	"context"
	"errors"
	"math/rand/v2"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Thapanut/bidding-engine/internal/core/auction/domain"
	"github.com/Thapanut/bidding-engine/internal/core/auction/port"
	"github.com/Thapanut/bidding-engine/internal/kernel"
)

const (
	startPrice   kernel.Money = 10_000
	minIncrement kernel.Money = 500
	seller                    = "seller-1"

	// ConcurrentBidders is the number of goroutines racing on one auction.
	ConcurrentBidders = 2000

	// clockTolerance absorbs the gap between reading the harness clock and
	// the store deciding, for stores whose clock cannot be frozen.
	clockTolerance = 30 * time.Second
)

// Harness is one adapter under test plus the clock it decides with.
type Harness struct {
	Store port.DecisionStore
	Now   func() time.Time
}

// Run executes the full contract against the adapter built by newHarness.
func Run(t *testing.T, newHarness func(t *testing.T) Harness) {
	t.Helper()
	cases := []struct {
		name string
		run  func(t *testing.T, h Harness)
	}{
		{"AC01_AcceptsFirstBidAtStartPrice", acceptsFirstBid},
		{"AC02_RejectsUnknownAuction", rejectsUnknownAuction},
		{"AC03_RejectsBidOutsideActiveWindow", rejectsInactive},
		{"AC04_RejectsSellerBiddingOnOwnAuction", rejectsSelfBid},
		{"AC05_RejectsBidderOutbiddingThemselves", rejectsAlreadyHighest},
		{"AC06_EnforcesMinIncrement", enforcesMinIncrement},
		{"AC07_ExtendsAuctionOnLateBid", extendsOnLateBid},
		{"AC07_DoesNotExtendOnEarlyBid", noExtensionOnEarlyBid},
		{"AC08_ReplaysAcceptedBidIdempotently", replaysIdempotently},
		{"AC08_RejectsIdempotencyKeyReuse", rejectsKeyReuse},
		{"AC09_OpenAuctionNeverResetsLiveState", openIsIdempotent},
		{"AC22_RecentBidsAreNewestFirstAndCapped", recentBids},
		{"AC10_ConcurrentBiddersKeepInvariants", concurrentBidders},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { tc.run(t, newHarness(t)) })
	}
}

func openAuction(t *testing.T, h Harness, startsIn, endsIn time.Duration) domain.Auction {
	t.Helper()
	now := h.Now()
	auction, err := domain.NewAuction(uuid.NewString(), seller, startPrice, minIncrement, now.Add(startsIn), now.Add(endsIn))
	if err != nil {
		t.Fatalf("new auction: %v", err)
	}
	if err := h.Store.OpenAuction(context.Background(), auction); err != nil {
		t.Fatalf("open auction: %v", err)
	}
	return auction
}

func attempt(t *testing.T, auctionID, bidder string, amount kernel.Money) domain.BidAttempt {
	t.Helper()
	a, err := domain.NewBidAttempt(uuid.NewString(), auctionID, bidder, amount)
	if err != nil {
		t.Fatalf("new bid attempt: %v", err)
	}
	return a
}

func mustPlace(t *testing.T, h Harness, a domain.BidAttempt) domain.Bid {
	t.Helper()
	bid, err := h.Store.PlaceBid(context.Background(), a)
	if err != nil {
		t.Fatalf("place bid: %v", err)
	}
	return bid
}

func mustGet(t *testing.T, h Harness, auctionID string) domain.Auction {
	t.Helper()
	auction, err := h.Store.GetAuction(context.Background(), auctionID)
	if err != nil {
		t.Fatalf("get auction: %v", err)
	}
	return auction
}

func wantErr(t *testing.T, h Harness, a domain.BidAttempt, want error) error {
	t.Helper()
	_, err := h.Store.PlaceBid(context.Background(), a)
	if !errors.Is(err, want) {
		t.Fatalf("place bid error = %v, want %v", err, want)
	}
	return err
}

func acceptsFirstBid(t *testing.T, h Harness) {
	auction := openAuction(t, h, -time.Hour, time.Hour)
	a := attempt(t, auction.ID, "bidder-1", startPrice)
	bid := mustPlace(t, h, a)

	if bid.ID != a.BidID || bid.AuctionID != auction.ID || bid.BidderID != "bidder-1" || bid.Amount != startPrice {
		t.Fatalf("bid = %+v, want it to echo the attempt %+v", bid, a)
	}
	if bid.Sequence != 1 || bid.Extended {
		t.Fatalf("bid sequence/extended = %d/%v, want 1/false", bid.Sequence, bid.Extended)
	}
	if d := bid.PlacedAt.Sub(h.Now()); d < -clockTolerance || d > clockTolerance {
		t.Fatalf("placed_at %v is not close to the store clock", bid.PlacedAt)
	}
	got := mustGet(t, h, auction.ID)
	want := auction
	want.CurrentPrice, want.HighestBidderID, want.BidSequence = startPrice, "bidder-1", 1
	if got != want {
		t.Fatalf("auction = %+v, want %+v", got, want)
	}
}

func rejectsUnknownAuction(t *testing.T, h Harness) {
	wantErr(t, h, attempt(t, uuid.NewString(), "bidder-1", startPrice), domain.ErrAuctionNotFound)
	if _, err := h.Store.GetAuction(context.Background(), uuid.NewString()); !errors.Is(err, domain.ErrAuctionNotFound) {
		t.Fatalf("get auction error = %v, want ErrAuctionNotFound", err)
	}
}

func rejectsInactive(t *testing.T, h Harness) {
	scheduled := openAuction(t, h, time.Hour, 2*time.Hour)
	wantErr(t, h, attempt(t, scheduled.ID, "bidder-1", startPrice), domain.ErrAuctionNotActive)

	ended := openAuction(t, h, -2*time.Hour, -time.Hour)
	wantErr(t, h, attempt(t, ended.ID, "bidder-1", startPrice), domain.ErrAuctionNotActive)

	if got := mustGet(t, h, ended.ID); got.BidSequence != 0 {
		t.Fatalf("rejected bid changed state: %+v", got)
	}
}

func rejectsSelfBid(t *testing.T, h Harness) {
	auction := openAuction(t, h, -time.Hour, time.Hour)
	wantErr(t, h, attempt(t, auction.ID, seller, startPrice), domain.ErrSelfBidding)
}

func rejectsAlreadyHighest(t *testing.T, h Harness) {
	auction := openAuction(t, h, -time.Hour, time.Hour)
	mustPlace(t, h, attempt(t, auction.ID, "bidder-1", startPrice))
	wantErr(t, h, attempt(t, auction.ID, "bidder-1", startPrice+minIncrement), domain.ErrAlreadyHighestBidder)
}

func enforcesMinIncrement(t *testing.T, h Harness) {
	auction := openAuction(t, h, -time.Hour, time.Hour)

	err := wantErr(t, h, attempt(t, auction.ID, "bidder-1", startPrice-1), domain.ErrBidTooLow)
	var tooLow *domain.BidTooLowError
	if !errors.As(err, &tooLow) || tooLow.MinRequired != startPrice {
		t.Fatalf("first-bid minimum = %v, want %d", err, startPrice)
	}

	mustPlace(t, h, attempt(t, auction.ID, "bidder-1", startPrice))
	err = wantErr(t, h, attempt(t, auction.ID, "bidder-2", startPrice+minIncrement-1), domain.ErrBidTooLow)
	if !errors.As(err, &tooLow) || tooLow.MinRequired != startPrice+minIncrement {
		t.Fatalf("next-bid minimum = %v, want %d", err, startPrice+minIncrement)
	}

	bid := mustPlace(t, h, attempt(t, auction.ID, "bidder-2", startPrice+minIncrement))
	if bid.Sequence != 2 {
		t.Fatalf("sequence = %d, want 2", bid.Sequence)
	}
}

func extendsOnLateBid(t *testing.T, h Harness) {
	auction := openAuction(t, h, -time.Hour, domain.AntiSnipingWindow/2)
	bid := mustPlace(t, h, attempt(t, auction.ID, "bidder-1", startPrice))

	if !bid.Extended {
		t.Fatal("bid inside the anti-sniping window did not extend the auction")
	}
	if want := bid.PlacedAt.Add(domain.AntiSnipingExtension); !bid.AuctionEndsAt.Equal(want) {
		t.Fatalf("auction_ends_at = %v, want placed_at + extension = %v", bid.AuctionEndsAt, want)
	}
	if got := mustGet(t, h, auction.ID); !got.EndsAt.Equal(bid.AuctionEndsAt) {
		t.Fatalf("stored ends_at = %v, want %v", got.EndsAt, bid.AuctionEndsAt)
	}
}

func noExtensionOnEarlyBid(t *testing.T, h Harness) {
	auction := openAuction(t, h, -time.Hour, time.Hour)
	bid := mustPlace(t, h, attempt(t, auction.ID, "bidder-1", startPrice))
	if bid.Extended || !bid.AuctionEndsAt.Equal(auction.EndsAt) {
		t.Fatalf("bid = %+v, want no extension and ends_at %v", bid, auction.EndsAt)
	}
}

func replaysIdempotently(t *testing.T, h Harness) {
	auction := openAuction(t, h, -time.Hour, time.Hour)
	a := attempt(t, auction.ID, "bidder-1", startPrice)
	first := mustPlace(t, h, a)
	mustPlace(t, h, attempt(t, auction.ID, "bidder-2", startPrice+minIncrement))

	// The replay must return the original decision even though the same
	// request would now be rejected as too low.
	if replay := mustPlace(t, h, a); replay != first {
		t.Fatalf("replay = %+v, want original %+v", replay, first)
	}
	if got := mustGet(t, h, auction.ID); got.BidSequence != 2 || got.HighestBidderID != "bidder-2" {
		t.Fatalf("replay changed state: %+v", got)
	}
}

func rejectsKeyReuse(t *testing.T, h Harness) {
	auction := openAuction(t, h, -time.Hour, time.Hour)
	a := attempt(t, auction.ID, "bidder-1", startPrice)
	mustPlace(t, h, a)

	a.Amount = startPrice + minIncrement
	wantErr(t, h, a, domain.ErrIdempotencyKeyReused)
}

func openIsIdempotent(t *testing.T, h Harness) {
	auction := openAuction(t, h, -time.Hour, time.Hour)
	mustPlace(t, h, attempt(t, auction.ID, "bidder-1", startPrice))
	if err := h.Store.OpenAuction(context.Background(), auction); err != nil {
		t.Fatalf("re-open: %v", err)
	}
	if got := mustGet(t, h, auction.ID); got.BidSequence != 1 || got.HighestBidderID != "bidder-1" {
		t.Fatalf("re-open reset live state: %+v", got)
	}
}

func recentBids(t *testing.T, h Harness) {
	ctx := context.Background()
	if bids, err := h.Store.RecentBids(ctx, uuid.NewString()); err != nil || len(bids) != 0 {
		t.Fatalf("unknown auction: bids = %v, err = %v, want an empty list", bids, err)
	}

	auction := openAuction(t, h, -time.Hour, time.Hour)
	const placed = domain.RecentBidsKept + 5
	var last domain.Bid
	for i := range placed {
		bidder := "bidder-" + string(rune('a'+i%2))
		last = mustPlace(t, h, attempt(t, auction.ID, bidder, startPrice+kernel.Money(i)*minIncrement))
	}
	wantErr(t, h, attempt(t, auction.ID, "bidder-z", startPrice), domain.ErrBidTooLow)

	bids, err := h.Store.RecentBids(ctx, auction.ID)
	if err != nil {
		t.Fatalf("recent bids: %v", err)
	}
	if len(bids) != domain.RecentBidsKept {
		t.Fatalf("kept %d bids, want the cap %d", len(bids), domain.RecentBidsKept)
	}
	if bids[0] != last {
		t.Fatalf("newest = %+v, want the last accepted bid %+v", bids[0], last)
	}
	for i, bid := range bids {
		if want := int64(placed - i); bid.Sequence != want {
			t.Fatalf("bids[%d].Sequence = %d, want %d (newest first)", i, bid.Sequence, want)
		}
	}
}

// concurrentBidders releases ConcurrentBidders goroutines on one auction at
// once. Each accepted bidder immediately replays its own request.
func concurrentBidders(t *testing.T, h Harness) {
	auction := openAuction(t, h, -time.Hour, time.Hour)
	accepted := RaceBidders(t, auction, func(ctx context.Context, a domain.BidAttempt) (domain.Bid, error) {
		bid, err := h.Store.PlaceBid(ctx, a)
		if err != nil {
			return bid, err
		}
		replay, err := h.Store.PlaceBid(ctx, a)
		if err != nil {
			return bid, err
		}
		if replay != bid {
			t.Errorf("replay of %s = %+v, want %+v", a.BidID, replay, bid)
		}
		return bid, nil
	})
	AssertLedger(t, auction, accepted, mustGet(t, h, auction.ID))
}

// HighestRacedAmount is the largest amount RaceBidders submits.
func HighestRacedAmount(auction domain.Auction) kernel.Money {
	return auction.StartPrice + kernel.Money(ConcurrentBidders-1)*auction.MinIncrement
}

// RaceBidders starts ConcurrentBidders goroutines, each a distinct bidder with
// a distinct amount one increment apart, releases them together, and returns
// the accepted bids ordered by sequence. Any rejection other than "too low"
// fails the test.
func RaceBidders(t *testing.T, auction domain.Auction, place func(context.Context, domain.BidAttempt) (domain.Bid, error)) []domain.Bid {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		accepted []domain.Bid
		gate     = make(chan struct{})
	)
	// Amounts are shuffled so that goroutine start order does not decide
	// which bids are acceptable when they reach the store.
	for _, step := range rand.Perm(ConcurrentBidders) {
		a, err := domain.NewBidAttempt(uuid.NewString(), auction.ID, "bidder-"+uuid.NewString(),
			auction.StartPrice+kernel.Money(step)*auction.MinIncrement)
		if err != nil {
			t.Fatalf("new bid attempt: %v", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			bid, err := place(ctx, a)
			switch {
			case err == nil:
				mu.Lock()
				accepted = append(accepted, bid)
				mu.Unlock()
			case errors.Is(err, domain.ErrBidTooLow):
			default:
				t.Errorf("bid %d satang: unexpected error: %v", int64(a.Amount), err)
			}
		}()
	}
	close(gate)
	wg.Wait()

	sort.Slice(accepted, func(i, j int) bool { return accepted[i].Sequence < accepted[j].Sequence })
	return accepted
}

// AssertLedger checks the invariants a correct decision layer guarantees no
// matter how the race interleaved: gapless sequences, every accepted bid at
// least one increment above the previous one, and final state equal to the
// last accepted bid, which must be the highest amount submitted.
func AssertLedger(t *testing.T, opened domain.Auction, accepted []domain.Bid, final domain.Auction) {
	t.Helper()
	if len(accepted) == 0 {
		t.Fatal("no bid was accepted")
	}
	for i, bid := range accepted {
		if bid.Sequence != int64(i+1) {
			t.Fatalf("accepted[%d].Sequence = %d, want gapless %d", i, bid.Sequence, i+1)
		}
		minRequired := opened.StartPrice
		if i > 0 {
			minRequired = accepted[i-1].Amount + opened.MinIncrement
		}
		if bid.Amount < minRequired {
			t.Fatalf("sequence %d accepted %d satang, below the minimum %d", bid.Sequence, int64(bid.Amount), int64(minRequired))
		}
	}
	last := accepted[len(accepted)-1]
	if want := HighestRacedAmount(opened); last.Amount != want {
		t.Fatalf("winning amount = %d, want the highest submitted %d", int64(last.Amount), int64(want))
	}
	if final.BidSequence != last.Sequence || final.CurrentPrice != last.Amount || final.HighestBidderID != last.BidderID {
		t.Fatalf("final auction = %+v, want it to reflect the last accepted bid %+v", final, last)
	}
	t.Logf("%d bidders raced, %d bids accepted, winner %d satang", ConcurrentBidders, len(accepted), int64(last.Amount))
}
