// Package httpapi is the REST adapter. It translates HTTP to the inbound
// port and domain errors to the contract in contracts/openapi.yaml.
package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/Thapanut/bidding-engine/internal/core/auction/domain"
	"github.com/Thapanut/bidding-engine/internal/core/auction/port"
	"github.com/Thapanut/bidding-engine/internal/kernel"
)

const (
	// HeaderIdempotencyKey identifies a bid across client retries.
	HeaderIdempotencyKey = "Idempotency-Key"

	bodyLimitBytes    = 16 * 1024
	retryAfterSeconds = "1"
)

// Error codes of the API contract.
const (
	codeValidation         = "validation_failed"
	codeNotFound           = "auction_not_found"
	codeNotActive          = "auction_not_active"
	codeSelfBidding        = "self_bidding"
	codeAlreadyHighest     = "already_highest_bidder"
	codeBidTooLow          = "bid_too_low"
	codeIdempotencyReused  = "idempotency_key_reused"
	codeBidNotDurable      = "bid_not_durable"
	codeInternal           = "internal_error"
	codeRequest            = "request_error"
	messageInternal        = "internal error"
	messageInvalidJSONBody = "request body must be a single JSON object with known fields only"
)

type createAuctionRequest struct {
	AuctionID          string    `json:"auction_id"`
	SellerID           string    `json:"seller_id"`
	StartPriceSatang   int64     `json:"start_price_satang"`
	MinIncrementSatang int64     `json:"min_increment_satang"`
	StartsAt           time.Time `json:"starts_at"`
	EndsAt             time.Time `json:"ends_at"`
}

type placeBidRequest struct {
	BidderID     string `json:"bidder_id"`
	AmountSatang int64  `json:"amount_satang"`
}

type auctionResponse struct {
	AuctionID          string        `json:"auction_id"`
	SellerID           string        `json:"seller_id"`
	Status             domain.Status `json:"status"`
	StartPriceSatang   int64         `json:"start_price_satang"`
	MinIncrementSatang int64         `json:"min_increment_satang"`
	CurrentPriceSatang int64         `json:"current_price_satang"`
	MinNextBidSatang   int64         `json:"min_next_bid_satang"`
	HighestBidderID    string        `json:"highest_bidder_id"`
	BidCount           int64         `json:"bid_count"`
	StartsAt           time.Time     `json:"starts_at"`
	EndsAt             time.Time     `json:"ends_at"`
}

type bidResponse struct {
	BidID         string    `json:"bid_id"`
	AuctionID     string    `json:"auction_id"`
	BidderID      string    `json:"bidder_id"`
	AmountSatang  int64     `json:"amount_satang"`
	Sequence      int64     `json:"sequence"`
	PlacedAt      time.Time `json:"placed_at"`
	AuctionEndsAt time.Time `json:"auction_ends_at"`
	Extended      bool      `json:"extended"`
}

type errorBody struct {
	Code              string `json:"code"`
	Message           string `json:"message"`
	MinRequiredSatang *int64 `json:"min_required_satang,omitempty"`
}

type errorResponse struct {
	Error errorBody `json:"error"`
}

type handler struct {
	bidding port.Bidding
	ids     port.IDGenerator
}

// New builds the Fiber app serving the bidding API.
func New(bidding port.Bidding, ids port.IDGenerator, log *slog.Logger) *fiber.App {
	app := fiber.New(fiber.Config{
		BodyLimit:             bodyLimitBytes,
		DisableStartupMessage: true,
		ErrorHandler:          errorHandler(log),
	})
	h := &handler{bidding: bidding, ids: ids}

	app.Get("/healthz", func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusNoContent) })
	v1 := app.Group("/v1")
	v1.Post("/auctions", h.createAuction)
	v1.Get("/auctions/:id", h.getAuction)
	v1.Post("/auctions/:id/bids", h.placeBid)
	return app
}

func (h *handler) createAuction(c *fiber.Ctx) error {
	var req createAuctionRequest
	if err := decodeStrict(c.Body(), &req); err != nil {
		return err
	}
	view, err := h.bidding.CreateAuction(c.UserContext(), port.CreateAuctionCommand{
		AuctionID:    req.AuctionID,
		SellerID:     req.SellerID,
		StartPrice:   kernel.Money(req.StartPriceSatang),
		MinIncrement: kernel.Money(req.MinIncrementSatang),
		StartsAt:     req.StartsAt,
		EndsAt:       req.EndsAt,
	})
	if err != nil {
		return err
	}
	return c.Status(fiber.StatusCreated).JSON(toAuctionResponse(view))
}

func (h *handler) getAuction(c *fiber.Ctx) error {
	// fasthttp reuses the buffer behind c.Params after the handler returns.
	view, err := h.bidding.GetAuction(c.UserContext(), strings.Clone(c.Params("id")))
	if err != nil {
		return err
	}
	return c.JSON(toAuctionResponse(view))
}

func (h *handler) placeBid(c *fiber.Ctx) error {
	var req placeBidRequest
	if err := decodeStrict(c.Body(), &req); err != nil {
		return err
	}
	key := strings.Clone(c.Get(HeaderIdempotencyKey))
	if key == "" {
		key = h.ids.NewID()
	}
	// Echoed on every outcome so the client can retry a 503 with the same key.
	c.Set(HeaderIdempotencyKey, key)

	bid, err := h.bidding.PlaceBid(c.UserContext(), port.PlaceBidCommand{
		AuctionID:      strings.Clone(c.Params("id")),
		BidderID:       req.BidderID,
		IdempotencyKey: key,
		Amount:         kernel.Money(req.AmountSatang),
	})
	if err != nil {
		return err
	}
	return c.Status(fiber.StatusCreated).JSON(bidResponse{
		BidID:         bid.ID,
		AuctionID:     bid.AuctionID,
		BidderID:      bid.BidderID,
		AmountSatang:  int64(bid.Amount),
		Sequence:      bid.Sequence,
		PlacedAt:      bid.PlacedAt,
		AuctionEndsAt: bid.AuctionEndsAt,
		Extended:      bid.Extended,
	})
}

func toAuctionResponse(view port.AuctionView) auctionResponse {
	a := view.Auction
	return auctionResponse{
		AuctionID:          a.ID,
		SellerID:           a.SellerID,
		Status:             view.Status,
		StartPriceSatang:   int64(a.StartPrice),
		MinIncrementSatang: int64(a.MinIncrement),
		CurrentPriceSatang: int64(a.CurrentPrice),
		MinNextBidSatang:   int64(a.MinNextBid()),
		HighestBidderID:    a.HighestBidderID,
		BidCount:           a.BidSequence,
		StartsAt:           a.StartsAt,
		EndsAt:             a.EndsAt,
	}
}

// decodeStrict rejects unknown fields, wrong types (including fractional
// money) and trailing data.
func decodeStrict(body []byte, dst any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return &domain.ValidationError{Reason: messageInvalidJSONBody}
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return &domain.ValidationError{Reason: messageInvalidJSONBody}
	}
	return nil
}

// errorHandler is the single place where errors become HTTP responses.
// Internal details are logged, never returned.
func errorHandler(log *slog.Logger) fiber.ErrorHandler {
	return func(c *fiber.Ctx, err error) error {
		status, body := mapError(err)
		switch status {
		case fiber.StatusInternalServerError:
			log.ErrorContext(c.UserContext(), "request failed", "method", c.Method(), "path", c.Path(), "error", err)
		case fiber.StatusServiceUnavailable:
			log.WarnContext(c.UserContext(), "bid accepted but not durable", "path", c.Path(), "error", err)
			c.Set(fiber.HeaderRetryAfter, retryAfterSeconds)
		}
		return c.Status(status).JSON(errorResponse{Error: body})
	}
}

func mapError(err error) (int, errorBody) {
	var (
		validation *domain.ValidationError
		tooLow     *domain.BidTooLowError
		fiberErr   *fiber.Error
	)
	switch {
	case errors.As(err, &validation):
		return fiber.StatusBadRequest, errorBody{Code: codeValidation, Message: validation.Reason}
	case errors.As(err, &tooLow):
		minRequired := int64(tooLow.MinRequired)
		return fiber.StatusUnprocessableEntity, errorBody{Code: codeBidTooLow, Message: domain.ErrBidTooLow.Error(), MinRequiredSatang: &minRequired}
	case errors.Is(err, domain.ErrAuctionNotFound):
		return fiber.StatusNotFound, errorBody{Code: codeNotFound, Message: domain.ErrAuctionNotFound.Error()}
	case errors.Is(err, domain.ErrAuctionNotActive):
		return fiber.StatusConflict, errorBody{Code: codeNotActive, Message: domain.ErrAuctionNotActive.Error()}
	case errors.Is(err, domain.ErrSelfBidding):
		return fiber.StatusForbidden, errorBody{Code: codeSelfBidding, Message: domain.ErrSelfBidding.Error()}
	case errors.Is(err, domain.ErrAlreadyHighestBidder):
		return fiber.StatusConflict, errorBody{Code: codeAlreadyHighest, Message: domain.ErrAlreadyHighestBidder.Error()}
	case errors.Is(err, domain.ErrIdempotencyKeyReused):
		return fiber.StatusUnprocessableEntity, errorBody{Code: codeIdempotencyReused, Message: domain.ErrIdempotencyKeyReused.Error()}
	case errors.Is(err, domain.ErrBidNotDurable):
		return fiber.StatusServiceUnavailable, errorBody{Code: codeBidNotDurable, Message: "bid accepted but not yet durable; retry with the same Idempotency-Key"}
	case errors.As(err, &fiberErr) && fiberErr.Code != fiber.StatusInternalServerError:
		return fiberErr.Code, errorBody{Code: codeRequest, Message: fiberErr.Message}
	default:
		return fiber.StatusInternalServerError, errorBody{Code: codeInternal, Message: messageInternal}
	}
}
