// Package postgres is the slow-path system of record.
package postgres

import (
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Thapanut/bidding-engine/internal/core/auction/domain"
	"github.com/Thapanut/bidding-engine/internal/core/auction/port"
	"github.com/Thapanut/bidding-engine/internal/kernel"
)

// PostgreSQL error codes the repository maps to domain errors.
const (
	foreignKeyViolation = "23503"
	uniqueViolation     = "23505"
)

//go:embed migrations/0001_init.sql
var schema string

const (
	insertAuction = `
INSERT INTO auctions (id, seller_id, start_price_satang, min_increment_satang, current_price_satang, starts_at, ends_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (id) DO NOTHING`

	selectAuction = `
SELECT id, seller_id, start_price_satang, min_increment_satang, current_price_satang,
       highest_bidder_id, bid_sequence, starts_at, ends_at
FROM auctions WHERE id = $1`

	// A redelivered bid hits the primary key and is skipped. A different bid
	// claiming a taken sequence violates the second constraint and fails.
	insertBid = `
INSERT INTO bids (auction_id, id, bidder_id, amount_satang, sequence, placed_at, auction_ends_at, extended)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (auction_id, id) DO NOTHING`

	// The sequence guard makes the update idempotent and lets a late,
	// lower-sequence bid be audited without rolling the auction back.
	advanceAuction = `
UPDATE auctions
SET current_price_satang = $2, highest_bidder_id = $3, bid_sequence = $4, ends_at = $5, updated_at = now()
WHERE id = $1 AND bid_sequence < $4`
)

// Repository implements port.AuctionRepository on PostgreSQL.
type Repository struct {
	pool *pgxpool.Pool
}

var _ port.AuctionRepository = (*Repository)(nil)

// NewRepository wraps an open pool.
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// Migrate creates the schema if it does not exist.
func (r *Repository) Migrate(ctx context.Context) error {
	if _, err := r.pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("postgres migrate: %w", err)
	}
	return nil
}

// CreateAuction implements port.AuctionRepository.
func (r *Repository) CreateAuction(ctx context.Context, a domain.Auction) error {
	tag, err := r.pool.Exec(ctx, insertAuction,
		a.ID, a.SellerID, int64(a.StartPrice), int64(a.MinIncrement), int64(a.CurrentPrice), a.StartsAt, a.EndsAt)
	if err != nil {
		return fmt.Errorf("postgres insert auction %s: %w", a.ID, err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrAuctionExists
	}
	return nil
}

// ApplyBid implements port.AuctionRepository. The audit row and the auction
// state change commit together or not at all.
func (r *Repository) ApplyBid(ctx context.Context, bid domain.Bid) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres begin: %w", err)
	}
	// Rollback after a successful commit is a no-op.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	_, err = tx.Exec(ctx, insertBid,
		bid.AuctionID, bid.ID, bid.BidderID, int64(bid.Amount), bid.Sequence, bid.PlacedAt, bid.AuctionEndsAt, bid.Extended)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			case foreignKeyViolation:
				return domain.ErrAuctionNotFound
			case uniqueViolation:
				return domain.ErrBidSequenceConflict
			}
		}
		return fmt.Errorf("postgres insert bid %s: %w", bid.ID, err)
	}
	_, err = tx.Exec(ctx, advanceAuction,
		bid.AuctionID, int64(bid.Amount), bid.BidderID, bid.Sequence, bid.AuctionEndsAt)
	if err != nil {
		return fmt.Errorf("postgres advance auction %s: %w", bid.AuctionID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres commit bid %s: %w", bid.ID, err)
	}
	return nil
}

// GetAuction implements port.AuctionRepository.
func (r *Repository) GetAuction(ctx context.Context, auctionID string) (domain.Auction, error) {
	var (
		a                                      domain.Auction
		startPrice, minIncrement, currentPrice int64
	)
	err := r.pool.QueryRow(ctx, selectAuction, auctionID).Scan(
		&a.ID, &a.SellerID, &startPrice, &minIncrement, &currentPrice,
		&a.HighestBidderID, &a.BidSequence, &a.StartsAt, &a.EndsAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Auction{}, domain.ErrAuctionNotFound
	}
	if err != nil {
		return domain.Auction{}, fmt.Errorf("postgres select auction %s: %w", auctionID, err)
	}
	a.StartPrice, a.MinIncrement, a.CurrentPrice = kernel.Money(startPrice), kernel.Money(minIncrement), kernel.Money(currentPrice)
	a.StartsAt, a.EndsAt = domain.NormalizeTime(a.StartsAt), domain.NormalizeTime(a.EndsAt)
	return a, nil
}
