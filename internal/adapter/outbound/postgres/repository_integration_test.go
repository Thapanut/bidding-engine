//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Thapanut/bidding-engine/internal/adapter/outbound/postgres"
	"github.com/Thapanut/bidding-engine/internal/core/auction/domain"
	"github.com/Thapanut/bidding-engine/internal/kernel"
)

const (
	dsnEnv = "TEST_DB_DSN"

	concurrentWriters = 2000
	distinctBids      = 200
)

func setup(t *testing.T) (*postgres.Repository, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skipf("%s not set", dsnEnv)
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	repo := postgres.NewRepository(pool)
	if err := repo.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Migrating twice must be harmless: every instance migrates on start.
	if err := repo.Migrate(context.Background()); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	return repo, pool
}

func createAuction(t *testing.T, repo *postgres.Repository) domain.Auction {
	t.Helper()
	now := time.Now()
	auction, err := domain.NewAuction(uuid.NewString(), "seller-1", 10_000, 500, now.Add(-time.Hour), now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateAuction(context.Background(), auction); err != nil {
		t.Fatal(err)
	}
	return auction
}

func bidAt(auction domain.Auction, sequence int64) domain.Bid {
	placedAt := domain.NormalizeTime(time.Now())
	return domain.Bid{
		ID: uuid.NewString(), AuctionID: auction.ID, BidderID: "bidder-" + uuid.NewString(),
		Amount: auction.StartPrice + kernel.Money(sequence-1)*auction.MinIncrement, Sequence: sequence,
		PlacedAt: placedAt, AuctionEndsAt: auction.EndsAt.Add(time.Duration(sequence) * time.Second), Extended: true,
	}
}

func countBids(t *testing.T, pool *pgxpool.Pool, auctionID string) (count int64) {
	t.Helper()
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM bids WHERE auction_id = $1`, auctionID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestAC20_AuctionRoundTripAndDuplicateID(t *testing.T) {
	repo, _ := setup(t)
	ctx := context.Background()
	auction := createAuction(t, repo)

	got, err := repo.GetAuction(ctx, auction.ID)
	if err != nil || got != auction {
		t.Fatalf("got %+v (%v), want %+v", got, err, auction)
	}
	if err := repo.CreateAuction(ctx, auction); !errors.Is(err, domain.ErrAuctionExists) {
		t.Fatalf("duplicate create: err = %v, want ErrAuctionExists", err)
	}
	if _, err := repo.GetAuction(ctx, uuid.NewString()); !errors.Is(err, domain.ErrAuctionNotFound) {
		t.Fatalf("missing: err = %v, want ErrAuctionNotFound", err)
	}
}

func TestAC20_ApplyBidIsIdempotentAndOrderTolerant(t *testing.T) {
	repo, pool := setup(t)
	ctx := context.Background()
	auction := createAuction(t, repo)
	first, second, third := bidAt(auction, 1), bidAt(auction, 2), bidAt(auction, 3)

	for _, bid := range []domain.Bid{third, first, third, second, first} {
		if err := repo.ApplyBid(ctx, bid); err != nil {
			t.Fatalf("apply sequence %d: %v", bid.Sequence, err)
		}
	}

	if n := countBids(t, pool, auction.ID); n != 3 {
		t.Fatalf("audit has %d rows, want 3", n)
	}
	got, _ := repo.GetAuction(ctx, auction.ID)
	if got.BidSequence != 3 || got.CurrentPrice != third.Amount || got.HighestBidderID != third.BidderID || !got.EndsAt.Equal(third.AuctionEndsAt) {
		t.Fatalf("auction = %+v, want the state after %+v", got, third)
	}
}

func TestAC20_ApplyBidPermanentFailures(t *testing.T) {
	repo, pool := setup(t)
	ctx := context.Background()
	auction := createAuction(t, repo)

	orphan := bidAt(auction, 1)
	orphan.AuctionID = uuid.NewString()
	if err := repo.ApplyBid(ctx, orphan); !errors.Is(err, domain.ErrAuctionNotFound) {
		t.Fatalf("orphan: err = %v, want ErrAuctionNotFound", err)
	}

	if err := repo.ApplyBid(ctx, bidAt(auction, 1)); err != nil {
		t.Fatal(err)
	}
	if err := repo.ApplyBid(ctx, bidAt(auction, 1)); !errors.Is(err, domain.ErrBidSequenceConflict) {
		t.Fatalf("sequence clash: err = %v, want ErrBidSequenceConflict", err)
	}
	if n := countBids(t, pool, auction.ID); n != 1 {
		t.Fatalf("audit has %d rows, want 1: the failed transaction must roll back", n)
	}
}

// 2,000 goroutines deliver 200 distinct bids ten times each, in random order.
func TestAC20_ConcurrentRedeliveryConvergesOnPostgres(t *testing.T) {
	repo, pool := setup(t)
	ctx := context.Background()
	auction := createAuction(t, repo)

	bids := make([]domain.Bid, distinctBids)
	for i := range bids {
		bids[i] = bidAt(auction, int64(i+1))
	}
	deliveries := make([]domain.Bid, 0, concurrentWriters)
	for len(deliveries) < concurrentWriters {
		deliveries = append(deliveries, bids...)
	}
	rand.Shuffle(len(deliveries), func(i, j int) { deliveries[i], deliveries[j] = deliveries[j], deliveries[i] })

	var wg sync.WaitGroup
	gate := make(chan struct{})
	for _, bid := range deliveries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			if err := repo.ApplyBid(ctx, bid); err != nil {
				t.Errorf("apply sequence %d: %v", bid.Sequence, err)
			}
		}()
	}
	close(gate)
	wg.Wait()

	if n := countBids(t, pool, auction.ID); n != distinctBids {
		t.Fatalf("audit has %d rows, want %d", n, distinctBids)
	}
	last := bids[distinctBids-1]
	got, _ := repo.GetAuction(ctx, auction.ID)
	if got.BidSequence != last.Sequence || got.CurrentPrice != last.Amount || got.HighestBidderID != last.BidderID {
		t.Fatalf("auction = %+v, want the state after %+v", got, last)
	}
}
