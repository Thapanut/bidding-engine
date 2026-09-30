// Package kafkapublisher hands accepted bids to the durable slow path.
package kafkapublisher

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Thapanut/bidding-engine/internal/adapter/contract/bidplaced"
	"github.com/Thapanut/bidding-engine/internal/core/auction/domain"
	"github.com/Thapanut/bidding-engine/internal/core/auction/port"
)

// DefaultDeliveryTimeout bounds how long a bid request waits for Kafka.
const DefaultDeliveryTimeout = 5 * time.Second

// Publisher implements port.BidEventPublisher with an idempotent producer
// that waits for all in-sync replicas.
type Publisher struct {
	client *kgo.Client
}

var _ port.BidEventPublisher = (*Publisher)(nil)

// New connects a producer to brokers.
func New(brokers []string, deliveryTimeout time.Duration) (*Publisher, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(bidplaced.Topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &Publisher{client: client}, nil
}

// PublishBidPlaced implements port.BidEventPublisher. It returns once the
// record is acknowledged. Keying by auction ID keeps an auction's bids on one
// partition; consumers still order by sequence, not by offset.
func (p *Publisher) PublishBidPlaced(ctx context.Context, bid domain.Bid) error {
	payload, err := bidplaced.Encode(bid)
	if err != nil {
		return err
	}
	record := &kgo.Record{Key: []byte(bid.AuctionID), Value: payload}
	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", bidplaced.Topic, err)
	}
	return nil
}

// Close flushes and releases the producer.
func (p *Publisher) Close() { p.client.Close() }
