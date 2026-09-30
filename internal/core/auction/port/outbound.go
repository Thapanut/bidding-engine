package port

import (
	"context"
	"time"

	"github.com/Thapanut/bidding-engine/internal/core/auction/domain"
)

// DecisionStore is the fast path: it holds live auction state and decides
// bids atomically. It owns the clock used for the decision.
type DecisionStore interface {
	// OpenAuction makes the auction biddable. Opening an auction that is
	// already open is a no-op and never resets live state.
	OpenAuction(ctx context.Context, auction domain.Auction) error
	// PlaceBid atomically validates and applies the attempt. Replaying an
	// accepted attempt returns the original bid; reusing its BidID for a
	// different bid returns domain.ErrIdempotencyKeyReused.
	PlaceBid(ctx context.Context, attempt domain.BidAttempt) (domain.Bid, error)
	// GetAuction returns the live state or domain.ErrAuctionNotFound.
	GetAuction(ctx context.Context, auctionID string) (domain.Auction, error)
	// RecentBids returns the latest accepted bids, newest first, up to
	// domain.RecentBidsKept. An unknown auction yields an empty list.
	RecentBids(ctx context.Context, auctionID string) ([]domain.Bid, error)
}

// BidEventPublisher hands accepted bids to the durable slow path.
type BidEventPublisher interface {
	PublishBidPlaced(ctx context.Context, bid domain.Bid) error
}

// AuctionRepository is the durable system of record.
type AuctionRepository interface {
	// CreateAuction returns domain.ErrAuctionExists if the ID is taken.
	CreateAuction(ctx context.Context, auction domain.Auction) error
	// ApplyBid writes the bid audit row and advances the auction state in one
	// transaction. It is idempotent per bid and tolerates out-of-order bids.
	ApplyBid(ctx context.Context, bid domain.Bid) error
	// GetAuction returns the durable state or domain.ErrAuctionNotFound.
	GetAuction(ctx context.Context, auctionID string) (domain.Auction, error)
	// ListAuctions returns up to limit auctions, most recently created first.
	ListAuctions(ctx context.Context, limit int) ([]domain.Auction, error)
	// ListBids returns up to limit bids of an auction, highest sequence first.
	ListBids(ctx context.Context, auctionID string, limit int) ([]domain.Bid, error)
}

// Clock supplies the current time.
type Clock interface {
	Now() time.Time
}

// IDGenerator supplies new unique identifiers.
type IDGenerator interface {
	NewID() string
}
