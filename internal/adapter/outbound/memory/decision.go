// Package memory provides in-process adapters for tests and local runs.
package memory

import (
	"context"
	"sync"

	"github.com/Thapanut/bidding-engine/internal/core/auction/domain"
	"github.com/Thapanut/bidding-engine/internal/core/auction/port"
)

// DecisionStore is an in-memory port.DecisionStore. It applies the domain
// rules under one mutex, which makes it the executable reference for the
// Redis Lua script.
type DecisionStore struct {
	clock port.Clock

	mu       sync.Mutex
	auctions map[string]domain.Auction
	bids     map[string]map[string]domain.Bid // auction ID -> bid ID -> bid
}

var _ port.DecisionStore = (*DecisionStore)(nil)

// NewDecisionStore returns an empty store that decides using clock.
func NewDecisionStore(clock port.Clock) *DecisionStore {
	return &DecisionStore{
		clock:    clock,
		auctions: make(map[string]domain.Auction),
		bids:     make(map[string]map[string]domain.Bid),
	}
}

// OpenAuction implements port.DecisionStore.
func (s *DecisionStore) OpenAuction(_ context.Context, auction domain.Auction) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, open := s.auctions[auction.ID]; !open {
		s.auctions[auction.ID] = auction
	}
	return nil
}

// PlaceBid implements port.DecisionStore.
func (s *DecisionStore) PlaceBid(_ context.Context, attempt domain.BidAttempt) (domain.Bid, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if prior, seen := s.bids[attempt.AuctionID][attempt.BidID]; seen {
		if !prior.SameRequest(attempt) {
			return domain.Bid{}, domain.ErrIdempotencyKeyReused
		}
		return prior, nil
	}
	auction, open := s.auctions[attempt.AuctionID]
	if !open {
		return domain.Bid{}, domain.ErrAuctionNotFound
	}
	updated, bid, err := auction.Place(attempt, s.clock.Now())
	if err != nil {
		return domain.Bid{}, err
	}
	s.auctions[attempt.AuctionID] = updated
	if s.bids[attempt.AuctionID] == nil {
		s.bids[attempt.AuctionID] = make(map[string]domain.Bid)
	}
	s.bids[attempt.AuctionID][attempt.BidID] = bid
	return bid, nil
}

// GetAuction implements port.DecisionStore.
func (s *DecisionStore) GetAuction(_ context.Context, auctionID string) (domain.Auction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	auction, open := s.auctions[auctionID]
	if !open {
		return domain.Auction{}, domain.ErrAuctionNotFound
	}
	return auction, nil
}
