// Package domain holds the auction entities and the bidding rules.
// It depends on the standard library and the shared kernel only.
package domain

import (
	"time"

	"github.com/Thapanut/bidding-engine/internal/kernel"
)

const (
	// AntiSnipingWindow is how close to the end a bid must land to extend the auction.
	AntiSnipingWindow = 2 * time.Minute
	// AntiSnipingExtension is how far past the sniping bid the new end is set.
	AntiSnipingExtension = 2 * time.Minute

	// TimePrecision is the resolution of every timestamp in the engine.
	// The Redis decision layer works in whole milliseconds.
	TimePrecision = time.Millisecond

	maxIDLength = 64
)

// Status is the lifecycle state of an auction at a given instant.
type Status string

// Auction lifecycle states.
const (
	StatusScheduled Status = "scheduled"
	StatusActive    Status = "active"
	StatusEnded     Status = "ended"
)

// Auction is the aggregate the bidding rules are evaluated against.
type Auction struct {
	ID              string
	SellerID        string
	StartPrice      kernel.Money
	MinIncrement    kernel.Money
	CurrentPrice    kernel.Money
	HighestBidderID string
	BidSequence     int64
	StartsAt        time.Time
	EndsAt          time.Time
}

// NewAuction validates the inputs and returns an auction with no bids.
func NewAuction(id, sellerID string, startPrice, minIncrement kernel.Money, startsAt, endsAt time.Time) (Auction, error) {
	if err := validateID("auction_id", id); err != nil {
		return Auction{}, err
	}
	if err := validateID("seller_id", sellerID); err != nil {
		return Auction{}, err
	}
	if startPrice <= 0 || startPrice > kernel.MaxMoney {
		return Auction{}, invalid("start_price_satang must be between 1 and %d", int64(kernel.MaxMoney))
	}
	if minIncrement <= 0 || minIncrement > kernel.MaxMoney {
		return Auction{}, invalid("min_increment_satang must be between 1 and %d", int64(kernel.MaxMoney))
	}
	startsAt, endsAt = NormalizeTime(startsAt), NormalizeTime(endsAt)
	if startsAt.IsZero() || endsAt.IsZero() {
		return Auction{}, invalid("starts_at and ends_at are required")
	}
	if !endsAt.After(startsAt) {
		return Auction{}, invalid("ends_at must be after starts_at")
	}
	return Auction{
		ID:           id,
		SellerID:     sellerID,
		StartPrice:   startPrice,
		MinIncrement: minIncrement,
		CurrentPrice: startPrice,
		StartsAt:     startsAt,
		EndsAt:       endsAt,
	}, nil
}

// StatusAt reports the lifecycle state at now.
func (a Auction) StatusAt(now time.Time) Status {
	switch {
	case now.Before(a.StartsAt):
		return StatusScheduled
	case now.Before(a.EndsAt):
		return StatusActive
	default:
		return StatusEnded
	}
}

// MinNextBid is the lowest amount the next bid must reach: the start price for
// the first bid, the current price plus the minimum increment afterwards.
func (a Auction) MinNextBid() kernel.Money {
	if a.BidSequence == 0 {
		return a.StartPrice
	}
	return a.CurrentPrice + a.MinIncrement
}

// Place evaluates attempt against the auction at now and returns the updated
// auction and the accepted bid. It is the reference for the rules the Redis
// Lua script enforces atomically; both are checked by the same test suite.
func (a Auction) Place(attempt BidAttempt, now time.Time) (Auction, Bid, error) {
	now = NormalizeTime(now)
	if a.StatusAt(now) != StatusActive {
		return a, Bid{}, ErrAuctionNotActive
	}
	if attempt.BidderID == a.SellerID {
		return a, Bid{}, ErrSelfBidding
	}
	if attempt.BidderID == a.HighestBidderID {
		return a, Bid{}, ErrAlreadyHighestBidder
	}
	if minRequired := a.MinNextBid(); attempt.Amount < minRequired {
		return a, Bid{}, &BidTooLowError{MinRequired: minRequired}
	}

	extended := false
	if a.EndsAt.Sub(now) <= AntiSnipingWindow {
		a.EndsAt = now.Add(AntiSnipingExtension)
		extended = true
	}
	a.BidSequence++
	a.CurrentPrice = attempt.Amount
	a.HighestBidderID = attempt.BidderID

	return a, Bid{
		ID:            attempt.BidID,
		AuctionID:     a.ID,
		BidderID:      attempt.BidderID,
		Amount:        attempt.Amount,
		Sequence:      a.BidSequence,
		PlacedAt:      now,
		AuctionEndsAt: a.EndsAt,
		Extended:      extended,
	}, nil
}

// NormalizeTime converts t to UTC at the engine's time precision.
func NormalizeTime(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}
	return t.UTC().Truncate(TimePrecision)
}

// ValidateAuctionID checks an auction identifier coming from the boundary.
func ValidateAuctionID(id string) error { return validateID("auction_id", id) }

// validateID restricts identifiers to a charset that is safe inside Redis
// keys and the pipe-delimited bid record the decision script stores.
func validateID(field, id string) error {
	if id == "" || len(id) > maxIDLength {
		return invalid("%s must be 1-%d characters", field, maxIDLength)
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
		default:
			return invalid("%s may only contain letters, digits, '-' and '_'", field)
		}
	}
	return nil
}
