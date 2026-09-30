// Package service implements the auction use cases on top of the ports.
package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/Thapanut/bidding-engine/internal/core/auction/domain"
	"github.com/Thapanut/bidding-engine/internal/core/auction/port"
)

// Bidding implements port.Bidding: decide on the fast path, then hand the
// accepted bid to the durable slow path.
type Bidding struct {
	decisions port.DecisionStore
	events    port.BidEventPublisher
	repo      port.AuctionRepository
	clock     port.Clock
	ids       port.IDGenerator
}

var _ port.Bidding = (*Bidding)(nil)

// NewBidding wires the bidding use cases.
func NewBidding(decisions port.DecisionStore, events port.BidEventPublisher, repo port.AuctionRepository, clock port.Clock, ids port.IDGenerator) *Bidding {
	return &Bidding{decisions: decisions, events: events, repo: repo, clock: clock, ids: ids}
}

// CreateAuction records the auction durably, then opens it for bidding.
// Re-sending the same auction ID returns the existing auction unchanged.
func (s *Bidding) CreateAuction(ctx context.Context, cmd port.CreateAuctionCommand) (port.AuctionView, error) {
	id := cmd.AuctionID
	if id == "" {
		id = s.ids.NewID()
	}
	auction, err := domain.NewAuction(id, cmd.SellerID, cmd.StartPrice, cmd.MinIncrement, cmd.StartsAt, cmd.EndsAt)
	if err != nil {
		return port.AuctionView{}, err
	}

	err = s.repo.CreateAuction(ctx, auction)
	if errors.Is(err, domain.ErrAuctionExists) {
		// A retry after a partial failure: finish opening what was stored.
		if auction, err = s.repo.GetAuction(ctx, id); err != nil {
			return port.AuctionView{}, fmt.Errorf("load existing auction %s: %w", id, err)
		}
	} else if err != nil {
		return port.AuctionView{}, fmt.Errorf("store auction %s: %w", id, err)
	}

	// An auction that already has bids must never be re-seeded from its
	// opening state; the decision store stays authoritative for it.
	if auction.BidSequence == 0 {
		if err := s.decisions.OpenAuction(ctx, auction); err != nil {
			return port.AuctionView{}, fmt.Errorf("open auction %s: %w", id, err)
		}
	}
	return s.view(auction), nil
}

// PlaceBid decides the bid atomically and publishes it for persistence.
// If publishing fails the decision stands; the caller retries with the same
// idempotency key, which replays the decision and publishes it again.
func (s *Bidding) PlaceBid(ctx context.Context, cmd port.PlaceBidCommand) (domain.Bid, error) {
	key := cmd.IdempotencyKey
	attempt, err := domain.NewBidAttempt(key, cmd.AuctionID, cmd.BidderID, cmd.Amount)
	if err != nil {
		return domain.Bid{}, err
	}

	bid, err := s.decisions.PlaceBid(ctx, attempt)
	if err != nil {
		return domain.Bid{}, fmt.Errorf("decide bid %s: %w", key, err)
	}
	if err := s.events.PublishBidPlaced(ctx, bid); err != nil {
		return domain.Bid{}, fmt.Errorf("publish bid %s: %w: %w", key, domain.ErrBidNotDurable, err)
	}
	return bid, nil
}

// GetAuction returns live state from the decision store, falling back to the
// system of record once the live state has expired.
func (s *Bidding) GetAuction(ctx context.Context, auctionID string) (port.AuctionView, error) {
	if err := domain.ValidateAuctionID(auctionID); err != nil {
		return port.AuctionView{}, err
	}
	auction, err := s.decisions.GetAuction(ctx, auctionID)
	if errors.Is(err, domain.ErrAuctionNotFound) {
		auction, err = s.repo.GetAuction(ctx, auctionID)
	}
	if err != nil {
		return port.AuctionView{}, fmt.Errorf("get auction %s: %w", auctionID, err)
	}
	return s.view(auction), nil
}

func (s *Bidding) view(auction domain.Auction) port.AuctionView {
	return port.AuctionView{Auction: auction, Status: auction.StatusAt(s.clock.Now())}
}
