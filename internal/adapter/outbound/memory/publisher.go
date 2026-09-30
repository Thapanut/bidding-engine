package memory

import (
	"context"
	"sync"

	"github.com/Thapanut/bidding-engine/internal/core/auction/domain"
	"github.com/Thapanut/bidding-engine/internal/core/auction/port"
)

// Publisher is an in-memory port.BidEventPublisher that records what was
// published and can be told to fail.
type Publisher struct {
	mu        sync.Mutex
	published []domain.Bid
	err       error
}

var _ port.BidEventPublisher = (*Publisher)(nil)

// NewPublisher returns a publisher that accepts every event.
func NewPublisher() *Publisher { return &Publisher{} }

// PublishBidPlaced implements port.BidEventPublisher.
func (p *Publisher) PublishBidPlaced(_ context.Context, bid domain.Bid) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.published = append(p.published, bid)
	return nil
}

// FailWith makes every following publish return err; nil restores success.
func (p *Publisher) FailWith(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
}

// Published returns a copy of the events published so far.
func (p *Publisher) Published() []domain.Bid {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]domain.Bid(nil), p.published...)
}
