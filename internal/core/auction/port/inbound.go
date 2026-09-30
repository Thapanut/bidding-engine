// Package port declares the interfaces through which the auction core is
// driven (inbound) and through which it reaches infrastructure (outbound).
package port

import (
	"context"
	"time"

	"github.com/Thapanut/bidding-engine/internal/core/auction/domain"
	"github.com/Thapanut/bidding-engine/internal/kernel"
)

// CreateAuctionCommand opens a new auction. AuctionID is optional.
type CreateAuctionCommand struct {
	AuctionID    string
	SellerID     string
	StartPrice   kernel.Money
	MinIncrement kernel.Money
	StartsAt     time.Time
	EndsAt       time.Time
}

// PlaceBidCommand places a bid. IdempotencyKey identifies the bid across retries.
type PlaceBidCommand struct {
	AuctionID      string
	BidderID       string
	IdempotencyKey string
	Amount         kernel.Money
}

// AuctionView is an auction together with its status at read time.
type AuctionView struct {
	Auction domain.Auction
	Status  domain.Status
}

// Bidding is the synchronous use-case API served over HTTP.
type Bidding interface {
	CreateAuction(ctx context.Context, cmd CreateAuctionCommand) (AuctionView, error)
	PlaceBid(ctx context.Context, cmd PlaceBidCommand) (domain.Bid, error)
	GetAuction(ctx context.Context, auctionID string) (AuctionView, error)
}

// BidRecorder is the asynchronous use case driven by the bid-placed consumer.
// RecordBid must be safe to call any number of times for the same bid.
type BidRecorder interface {
	RecordBid(ctx context.Context, bid domain.Bid) error
}
