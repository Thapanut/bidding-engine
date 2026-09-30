package service

import (
	"context"
	"fmt"

	"github.com/Thapanut/bidding-engine/internal/core/auction/domain"
	"github.com/Thapanut/bidding-engine/internal/core/auction/port"
)

// Recorder implements port.BidRecorder: the slow path that makes accepted
// bids durable.
type Recorder struct {
	repo port.AuctionRepository
}

var _ port.BidRecorder = (*Recorder)(nil)

// NewRecorder wires the bid persistence use case.
func NewRecorder(repo port.AuctionRepository) *Recorder {
	return &Recorder{repo: repo}
}

// RecordBid validates the bid and applies it idempotently.
func (r *Recorder) RecordBid(ctx context.Context, bid domain.Bid) error {
	if err := bid.Validate(); err != nil {
		return fmt.Errorf("record bid: %w", err)
	}
	if err := r.repo.ApplyBid(ctx, bid); err != nil {
		return fmt.Errorf("record bid %s: %w", bid.ID, err)
	}
	return nil
}
