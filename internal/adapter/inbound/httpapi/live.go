package httpapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/Thapanut/bidding-engine/internal/core/auction/port"
)

const (
	// snapshotInterval is how often a watched auction is sampled. Under load
	// an auction takes thousands of bids per second; viewers get coalesced
	// snapshots instead of one event per bid.
	snapshotInterval = 100 * time.Millisecond
	// heartbeatInterval is the longest a stream stays silent. It keeps client
	// countdowns in sync and lets the server notice dead connections.
	heartbeatInterval = time.Second
	// liveFeedSize is how many of the latest bids each snapshot carries.
	liveFeedSize = 12
)

type liveState struct {
	Auction    auctionResponse `json:"auction"`
	RecentBids []bidResponse   `json:"recent_bids"`
}

type liveSnapshot struct {
	liveState
	// ServerTime lets clients run countdowns without trusting their own clock.
	ServerTime time.Time `json:"server_time"`
}

// liveFeed is the set of streams watching one auction.
type liveFeed struct {
	subscribers map[chan []byte]struct{}
	stop        context.CancelFunc
}

// liveHub fans auction snapshots out to server-sent-event streams. It samples
// each watched auction once per interval no matter how many viewers it has,
// so read load on the decision store does not grow with the audience.
type liveHub struct {
	bidding port.Bidding
	clock   port.Clock
	log     *slog.Logger

	mu     sync.Mutex
	feeds  map[string]*liveFeed
	closed bool
	done   chan struct{}
}

func newLiveHub(bidding port.Bidding, clock port.Clock, log *slog.Logger) *liveHub {
	return &liveHub{
		bidding: bidding,
		clock:   clock,
		log:     log,
		feeds:   make(map[string]*liveFeed),
		done:    make(chan struct{}),
	}
}

// state reads the current state of an auction.
func (h *liveHub) state(ctx context.Context, auctionID string) (liveState, error) {
	view, err := h.bidding.GetAuction(ctx, auctionID)
	if err != nil {
		return liveState{}, err
	}
	bids, err := h.bidding.ListBids(ctx, auctionID)
	if err != nil {
		return liveState{}, err
	}
	bids = bids[:min(len(bids), liveFeedSize)]
	state := liveState{Auction: toAuctionResponse(view), RecentBids: make([]bidResponse, 0, len(bids))}
	for _, bid := range bids {
		state.RecentBids = append(state.RecentBids, toBidResponse(bid))
	}
	return state, nil
}

func (h *liveHub) encode(state liveState) ([]byte, error) {
	payload, err := json.Marshal(liveSnapshot{liveState: state, ServerTime: h.clock.Now().UTC()})
	if err != nil {
		return nil, fmt.Errorf("encode live snapshot: %w", err)
	}
	return payload, nil
}

// subscribe registers a stream for auctionID. The returned channel holds at
// most the latest snapshot: a slow reader skips ahead instead of queueing.
// ok is false once the hub is closed.
func (h *liveHub) subscribe(auctionID string) (updates <-chan []byte, unsubscribe func(), ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, nil, false
	}
	feed, watched := h.feeds[auctionID]
	if !watched {
		ctx, stop := context.WithCancel(context.Background())
		feed = &liveFeed{subscribers: make(map[chan []byte]struct{}), stop: stop}
		h.feeds[auctionID] = feed
		go h.sample(ctx, auctionID, feed)
	}
	ch := make(chan []byte, 1)
	feed.subscribers[ch] = struct{}{}

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			delete(feed.subscribers, ch)
			if len(feed.subscribers) == 0 && h.feeds[auctionID] == feed {
				feed.stop()
				delete(h.feeds, auctionID)
			}
		})
	}, true
}

// sample polls one auction until its last viewer leaves, broadcasting when
// the state changes or the heartbeat is due.
func (h *liveHub) sample(ctx context.Context, auctionID string, feed *liveFeed) {
	ticker := time.NewTicker(snapshotInterval)
	defer ticker.Stop()
	var (
		lastState []byte
		lastSent  time.Time
		failing   bool
	)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		state, err := h.state(ctx, auctionID)
		if err != nil {
			if !failing && ctx.Err() == nil {
				h.log.WarnContext(ctx, "live snapshot failed", "auction_id", auctionID, "error", err)
			}
			failing = true
			continue
		}
		failing = false

		fingerprint, err := json.Marshal(state)
		if err != nil {
			continue
		}
		if bytes.Equal(fingerprint, lastState) && time.Since(lastSent) < heartbeatInterval {
			continue
		}
		payload, err := h.encode(state)
		if err != nil {
			continue
		}
		lastState, lastSent = fingerprint, time.Now()
		h.broadcast(feed, payload)
	}
}

func (h *liveHub) broadcast(feed *liveFeed, payload []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range feed.subscribers {
		// Replace an unread snapshot with the newer one. Only this goroutine
		// sends, and it holds the lock, so the second send cannot block.
		select {
		case <-ch:
		default:
		}
		ch <- payload
	}
}

// close ends every stream; used on shutdown so open streams do not hold the
// server up.
func (h *liveHub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for id, feed := range h.feeds {
		feed.stop()
		delete(h.feeds, id)
	}
	close(h.done)
}

// streamAuction serves GET /v1/auctions/:id/events as server-sent events.
func (h *handler) streamAuction(c *fiber.Ctx) error {
	auctionID := strings.Clone(c.Params("id"))
	// Fail with a normal error response before the stream starts.
	state, err := h.live.state(c.UserContext(), auctionID)
	if err != nil {
		return err
	}
	first, err := h.live.encode(state)
	if err != nil {
		return err
	}
	updates, unsubscribe, ok := h.live.subscribe(auctionID)
	if !ok {
		return fiber.ErrServiceUnavailable
	}

	c.Set(fiber.HeaderContentType, "text/event-stream")
	c.Set(fiber.HeaderCacheControl, "no-cache")
	c.Set(fiber.HeaderConnection, "keep-alive")
	// Tells nginx-style proxies not to buffer the stream.
	c.Set("X-Accel-Buffering", "no")

	done := h.live.done
	c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
		defer unsubscribe()
		if !writeEvent(w, first) {
			return
		}
		for {
			select {
			case payload := <-updates:
				if !writeEvent(w, payload) {
					return
				}
			case <-done:
				return
			}
		}
	})
	return nil
}

// writeEvent reports whether the client is still connected.
func writeEvent(w *bufio.Writer, payload []byte) bool {
	if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
		return false
	}
	return w.Flush() == nil
}
