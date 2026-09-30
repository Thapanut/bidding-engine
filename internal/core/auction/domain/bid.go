package domain

import (
	"time"

	"github.com/Thapanut/bidding-engine/internal/kernel"
)

// BidAttempt is a bid that has been validated at the boundary but not yet
// decided. BidID doubles as the idempotency key.
type BidAttempt struct {
	BidID     string
	AuctionID string
	BidderID  string
	Amount    kernel.Money
}

// NewBidAttempt validates the inputs of a bid.
func NewBidAttempt(bidID, auctionID, bidderID string, amount kernel.Money) (BidAttempt, error) {
	if err := validateID("idempotency key", bidID); err != nil {
		return BidAttempt{}, err
	}
	if err := validateID("auction_id", auctionID); err != nil {
		return BidAttempt{}, err
	}
	if err := validateID("bidder_id", bidderID); err != nil {
		return BidAttempt{}, err
	}
	if amount <= 0 || amount > kernel.MaxMoney {
		return BidAttempt{}, invalid("amount_satang must be between 1 and %d", int64(kernel.MaxMoney))
	}
	return BidAttempt{BidID: bidID, AuctionID: auctionID, BidderID: bidderID, Amount: amount}, nil
}

// Bid is an accepted bid: the decision layer's output and the audit record.
type Bid struct {
	ID        string
	AuctionID string
	BidderID  string
	Amount    kernel.Money
	// Sequence is the 1-based position of the bid within its auction.
	Sequence int64
	PlacedAt time.Time
	// AuctionEndsAt is the auction end after this bid, including any extension.
	AuctionEndsAt time.Time
	// Extended reports whether this bid triggered the anti-sniping extension.
	Extended bool
}

// Validate checks a bid received from outside the process (e.g. from Kafka).
func (b Bid) Validate() error {
	if _, err := NewBidAttempt(b.ID, b.AuctionID, b.BidderID, b.Amount); err != nil {
		return err
	}
	if b.Sequence <= 0 {
		return invalid("sequence must be positive")
	}
	if b.PlacedAt.IsZero() || b.AuctionEndsAt.IsZero() {
		return invalid("placed_at and auction_ends_at are required")
	}
	return nil
}

// SameRequest reports whether attempt is a replay of the request that produced b.
func (b Bid) SameRequest(attempt BidAttempt) bool {
	return b.ID == attempt.BidID && b.AuctionID == attempt.AuctionID &&
		b.BidderID == attempt.BidderID && b.Amount == attempt.Amount
}
