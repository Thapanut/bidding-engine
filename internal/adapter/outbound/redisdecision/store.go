// Package redisdecision is the fast-path adapter: live auction state in Redis
// hashes, decided atomically by Lua scripts.
package redisdecision

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Thapanut/bidding-engine/internal/core/auction/domain"
	"github.com/Thapanut/bidding-engine/internal/core/auction/port"
	"github.com/Thapanut/bidding-engine/internal/kernel"
)

// DefaultRetention is how long live state outlives the auction's end.
const DefaultRetention = 24 * time.Hour

// Reply codes of place_bid.lua.
const (
	codeAccepted       = "ACCEPTED"
	codeDuplicate      = "DUPLICATE"
	codeNotFound       = "NOT_FOUND"
	codeNotActive      = "NOT_ACTIVE"
	codeSelfBid        = "SELF_BID"
	codeAlreadyHighest = "ALREADY_HIGHEST"
	codeTooLow         = "TOO_LOW"
)

// Auction hash fields.
const (
	fieldSellerID      = "seller_id"
	fieldStartPrice    = "start_price"
	fieldMinIncrement  = "min_increment"
	fieldCurrentPrice  = "current_price"
	fieldHighestBidder = "highest_bidder"
	fieldSequence      = "seq"
	fieldStartsAtMS    = "starts_at_ms"
	fieldEndsAtMS      = "ends_at_ms"
)

const (
	recordSeparator  = "|"
	recordFieldCount = 6
	extendedFlag     = "1"
)

var (
	//go:embed scripts/open_auction.lua
	openAuctionSource string
	//go:embed scripts/place_bid.lua
	placeBidSource string

	openAuctionScript = redis.NewScript(openAuctionSource)
	placeBidScript    = redis.NewScript(placeBidSource)
)

// Store implements port.DecisionStore on Redis.
type Store struct {
	client    redis.UniversalClient
	retention time.Duration
}

var _ port.DecisionStore = (*Store)(nil)

// NewStore returns a decision store that keeps state for retention after an
// auction ends.
func NewStore(client redis.UniversalClient, retention time.Duration) *Store {
	return &Store{client: client, retention: retention}
}

// The {id} hash tag keeps both keys of an auction in one Redis Cluster slot.
func auctionKey(auctionID string) string { return "auction:{" + auctionID + "}" }
func bidsKey(auctionID string) string    { return "auction:{" + auctionID + "}:bids" }

// OpenAuction implements port.DecisionStore.
func (s *Store) OpenAuction(ctx context.Context, auction domain.Auction) error {
	expireAt := auction.EndsAt.Add(s.retention)
	err := openAuctionScript.Run(ctx, s.client, []string{auctionKey(auction.ID)},
		auction.SellerID,
		int64(auction.StartPrice),
		int64(auction.MinIncrement),
		auction.StartsAt.UnixMilli(),
		auction.EndsAt.UnixMilli(),
		expireAt.UnixMilli(),
	).Err()
	if err != nil {
		return fmt.Errorf("redis open auction %s: %w", auction.ID, err)
	}
	return nil
}

// PlaceBid implements port.DecisionStore.
func (s *Store) PlaceBid(ctx context.Context, attempt domain.BidAttempt) (domain.Bid, error) {
	reply, err := placeBidScript.Run(ctx, s.client,
		[]string{auctionKey(attempt.AuctionID), bidsKey(attempt.AuctionID)},
		attempt.BidID,
		attempt.BidderID,
		int64(attempt.Amount),
		domain.AntiSnipingWindow.Milliseconds(),
		domain.AntiSnipingExtension.Milliseconds(),
		s.retention.Milliseconds(),
	).StringSlice()
	if err != nil {
		return domain.Bid{}, fmt.Errorf("redis place bid on %s: %w", attempt.AuctionID, err)
	}
	if len(reply) == 0 {
		return domain.Bid{}, errors.New("redis place bid: empty script reply")
	}

	code, payload := reply[0], ""
	if len(reply) > 1 {
		payload = reply[1]
	}
	switch code {
	case codeAccepted, codeDuplicate:
		bid, err := parseRecord(attempt.BidID, attempt.AuctionID, payload)
		if err != nil {
			return domain.Bid{}, err
		}
		if code == codeDuplicate && !bid.SameRequest(attempt) {
			return domain.Bid{}, domain.ErrIdempotencyKeyReused
		}
		return bid, nil
	case codeNotFound:
		return domain.Bid{}, domain.ErrAuctionNotFound
	case codeNotActive:
		return domain.Bid{}, domain.ErrAuctionNotActive
	case codeSelfBid:
		return domain.Bid{}, domain.ErrSelfBidding
	case codeAlreadyHighest:
		return domain.Bid{}, domain.ErrAlreadyHighestBidder
	case codeTooLow:
		minRequired, err := strconv.ParseInt(payload, 10, 64)
		if err != nil {
			return domain.Bid{}, fmt.Errorf("redis place bid: parse minimum %q: %w", payload, err)
		}
		return domain.Bid{}, &domain.BidTooLowError{MinRequired: kernel.Money(minRequired)}
	default:
		return domain.Bid{}, fmt.Errorf("redis place bid: unknown script reply %q", code)
	}
}

// GetAuction implements port.DecisionStore.
func (s *Store) GetAuction(ctx context.Context, auctionID string) (domain.Auction, error) {
	fields, err := s.client.HGetAll(ctx, auctionKey(auctionID)).Result()
	if err != nil {
		return domain.Auction{}, fmt.Errorf("redis get auction %s: %w", auctionID, err)
	}
	if len(fields) == 0 {
		return domain.Auction{}, domain.ErrAuctionNotFound
	}

	var parseErr error
	number := func(field string) int64 {
		n, err := strconv.ParseInt(fields[field], 10, 64)
		if err != nil && parseErr == nil {
			parseErr = fmt.Errorf("redis get auction %s: parse %s %q: %w", auctionID, field, fields[field], err)
		}
		return n
	}
	auction := domain.Auction{
		ID:              auctionID,
		SellerID:        fields[fieldSellerID],
		StartPrice:      kernel.Money(number(fieldStartPrice)),
		MinIncrement:    kernel.Money(number(fieldMinIncrement)),
		CurrentPrice:    kernel.Money(number(fieldCurrentPrice)),
		HighestBidderID: fields[fieldHighestBidder],
		BidSequence:     number(fieldSequence),
		StartsAt:        time.UnixMilli(number(fieldStartsAtMS)).UTC(),
		EndsAt:          time.UnixMilli(number(fieldEndsAtMS)).UTC(),
	}
	if parseErr != nil {
		return domain.Auction{}, parseErr
	}
	return auction, nil
}

// parseRecord decodes "seq|bidder_id|amount|placed_at_ms|ends_at_ms|extended".
func parseRecord(bidID, auctionID, record string) (domain.Bid, error) {
	parts := strings.Split(record, recordSeparator)
	if len(parts) != recordFieldCount {
		return domain.Bid{}, fmt.Errorf("redis place bid: malformed bid record %q", record)
	}
	var numbers [4]int64
	for i, raw := range []string{parts[0], parts[2], parts[3], parts[4]} {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return domain.Bid{}, fmt.Errorf("redis place bid: malformed bid record %q: %w", record, err)
		}
		numbers[i] = n
	}
	return domain.Bid{
		ID:            bidID,
		AuctionID:     auctionID,
		BidderID:      parts[1],
		Amount:        kernel.Money(numbers[1]),
		Sequence:      numbers[0],
		PlacedAt:      time.UnixMilli(numbers[2]).UTC(),
		AuctionEndsAt: time.UnixMilli(numbers[3]).UTC(),
		Extended:      parts[5] == extendedFlag,
	}, nil
}
