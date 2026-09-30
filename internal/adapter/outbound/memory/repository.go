package memory

import (
	"context"
	"slices"
	"sort"
	"sync"

	"github.com/Thapanut/bidding-engine/internal/core/auction/domain"
	"github.com/Thapanut/bidding-engine/internal/core/auction/port"
)

// Repository is an in-memory port.AuctionRepository with the same
// idempotency and ordering semantics as the PostgreSQL adapter.
type Repository struct {
	mu       sync.Mutex
	auctions map[string]domain.Auction
	created  []string                         // auction IDs in creation order
	bids     map[string]map[string]domain.Bid // auction ID -> bid ID -> bid
}

var _ port.AuctionRepository = (*Repository)(nil)

// NewRepository returns an empty repository.
func NewRepository() *Repository {
	return &Repository{
		auctions: make(map[string]domain.Auction),
		bids:     make(map[string]map[string]domain.Bid),
	}
}

// CreateAuction implements port.AuctionRepository.
func (r *Repository) CreateAuction(_ context.Context, auction domain.Auction) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.auctions[auction.ID]; exists {
		return domain.ErrAuctionExists
	}
	r.auctions[auction.ID] = auction
	r.created = append(r.created, auction.ID)
	r.bids[auction.ID] = make(map[string]domain.Bid)
	return nil
}

// ApplyBid implements port.AuctionRepository.
func (r *Repository) ApplyBid(_ context.Context, bid domain.Bid) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	auction, exists := r.auctions[bid.AuctionID]
	if !exists {
		return domain.ErrAuctionNotFound
	}
	if _, recorded := r.bids[bid.AuctionID][bid.ID]; !recorded {
		for _, other := range r.bids[bid.AuctionID] {
			if other.Sequence == bid.Sequence {
				return domain.ErrBidSequenceConflict
			}
		}
		r.bids[bid.AuctionID][bid.ID] = bid
	}
	if bid.Sequence > auction.BidSequence {
		auction.BidSequence = bid.Sequence
		auction.CurrentPrice = bid.Amount
		auction.HighestBidderID = bid.BidderID
		auction.EndsAt = bid.AuctionEndsAt
		r.auctions[bid.AuctionID] = auction
	}
	return nil
}

// GetAuction implements port.AuctionRepository.
func (r *Repository) GetAuction(_ context.Context, auctionID string) (domain.Auction, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	auction, exists := r.auctions[auctionID]
	if !exists {
		return domain.Auction{}, domain.ErrAuctionNotFound
	}
	return auction, nil
}

// ListAuctions implements port.AuctionRepository.
func (r *Repository) ListAuctions(_ context.Context, limit int) ([]domain.Auction, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	auctions := make([]domain.Auction, 0, min(limit, len(r.created)))
	for i := len(r.created) - 1; i >= 0 && len(auctions) < limit; i-- {
		auctions = append(auctions, r.auctions[r.created[i]])
	}
	return auctions, nil
}

// ListBids implements port.AuctionRepository.
func (r *Repository) ListBids(_ context.Context, auctionID string, limit int) ([]domain.Bid, error) {
	bids := r.Bids(auctionID)
	slices.Reverse(bids)
	return bids[:min(limit, len(bids))], nil
}

// Bids returns the audit trail of an auction ordered by sequence.
func (r *Repository) Bids(auctionID string) []domain.Bid {
	r.mu.Lock()
	defer r.mu.Unlock()
	bids := make([]domain.Bid, 0, len(r.bids[auctionID]))
	for _, bid := range r.bids[auctionID] {
		bids = append(bids, bid)
	}
	sort.Slice(bids, func(i, j int) bool { return bids[i].Sequence < bids[j].Sequence })
	return bids
}
