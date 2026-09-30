package domain

import (
	"errors"
	"fmt"

	"github.com/Thapanut/bidding-engine/internal/kernel"
)

// Sentinel errors for every business outcome the engine can reject with.
var (
	ErrValidation           = errors.New("validation failed")
	ErrAuctionNotFound      = errors.New("auction not found")
	ErrAuctionExists        = errors.New("auction already exists")
	ErrAuctionNotActive     = errors.New("auction is not active")
	ErrSelfBidding          = errors.New("seller cannot bid on own auction")
	ErrAlreadyHighestBidder = errors.New("bidder already holds the highest bid")
	ErrBidTooLow            = errors.New("bid is below the minimum required amount")
	ErrIdempotencyKeyReused = errors.New("idempotency key reused with a different bid")
	ErrBidNotDurable        = errors.New("bid accepted but not yet durable")
	ErrBidSequenceConflict  = errors.New("a different bid already holds this sequence")
)

// BidTooLowError carries the minimum amount that would have been accepted.
// It matches ErrBidTooLow with errors.Is.
type BidTooLowError struct {
	MinRequired kernel.Money
}

func (e *BidTooLowError) Error() string {
	return fmt.Sprintf("%s: minimum is %d satang", ErrBidTooLow.Error(), int64(e.MinRequired))
}

// Is reports whether target is ErrBidTooLow.
func (e *BidTooLowError) Is(target error) bool { return target == ErrBidTooLow }

// ValidationError describes which input was rejected at the boundary.
// It matches ErrValidation with errors.Is.
type ValidationError struct {
	Reason string
}

func (e *ValidationError) Error() string { return ErrValidation.Error() + ": " + e.Reason }

// Is reports whether target is ErrValidation.
func (e *ValidationError) Is(target error) bool { return target == ErrValidation }

func invalid(format string, args ...any) error {
	return &ValidationError{Reason: fmt.Sprintf(format, args...)}
}
