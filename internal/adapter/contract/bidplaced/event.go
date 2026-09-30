// Package bidplaced is the wire contract of the auctions.v1.bid-placed topic
// (see contracts/asyncapi.yaml). Producer and consumer adapters share it.
package bidplaced

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Thapanut/bidding-engine/internal/core/auction/domain"
	"github.com/Thapanut/bidding-engine/internal/kernel"
)

// Topic names follow <domain>.<version>.<event>.
const (
	Topic           = "auctions.v1.bid-placed"
	DeadLetterTopic = "auctions.v1.bid-placed-dlq"
)

// Event is the JSON payload. Records are keyed by AuctionID so all bids of
// one auction land on one partition.
type Event struct {
	BidID         string    `json:"bid_id"`
	AuctionID     string    `json:"auction_id"`
	BidderID      string    `json:"bidder_id"`
	AmountSatang  int64     `json:"amount_satang"`
	Sequence      int64     `json:"sequence"`
	PlacedAt      time.Time `json:"placed_at"`
	AuctionEndsAt time.Time `json:"auction_ends_at"`
	Extended      bool      `json:"extended"`
}

// Encode serialises an accepted bid.
func Encode(bid domain.Bid) ([]byte, error) {
	payload, err := json.Marshal(Event{
		BidID:         bid.ID,
		AuctionID:     bid.AuctionID,
		BidderID:      bid.BidderID,
		AmountSatang:  int64(bid.Amount),
		Sequence:      bid.Sequence,
		PlacedAt:      bid.PlacedAt.UTC(),
		AuctionEndsAt: bid.AuctionEndsAt.UTC(),
		Extended:      bid.Extended,
	})
	if err != nil {
		return nil, fmt.Errorf("encode bid-placed event: %w", err)
	}
	return payload, nil
}

// Decode parses a payload. Unknown fields are ignored so the producer can
// add fields without breaking consumers.
func Decode(payload []byte) (domain.Bid, error) {
	var event Event
	if err := json.Unmarshal(payload, &event); err != nil {
		return domain.Bid{}, fmt.Errorf("decode bid-placed event: %w", err)
	}
	return domain.Bid{
		ID:            event.BidID,
		AuctionID:     event.AuctionID,
		BidderID:      event.BidderID,
		Amount:        kernel.Money(event.AmountSatang),
		Sequence:      event.Sequence,
		PlacedAt:      domain.NormalizeTime(event.PlacedAt),
		AuctionEndsAt: domain.NormalizeTime(event.AuctionEndsAt),
		Extended:      event.Extended,
	}, nil
}
