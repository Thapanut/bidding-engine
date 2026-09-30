// Package kafkaconsumer drives the slow path: it reads bid-placed events and
// hands them to the bid recorder with at-least-once delivery.
package kafkaconsumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Thapanut/bidding-engine/internal/adapter/contract/bidplaced"
	"github.com/Thapanut/bidding-engine/internal/core/auction/domain"
	"github.com/Thapanut/bidding-engine/internal/core/auction/port"
)

const (
	initialBackoff = 100 * time.Millisecond
	maxBackoff     = 5 * time.Second
	backoffFactor  = 2

	headerError     = "x-error"
	headerTopic     = "x-source-topic"
	headerPartition = "x-source-partition"
	headerOffset    = "x-source-offset"
)

// Consumer reads bid-placed events in a consumer group. Offsets are
// committed only after a record is recorded or dead-lettered, so a crash
// re-delivers; the recorder is idempotent, which makes that safe.
type Consumer struct {
	client   *kgo.Client
	recorder port.BidRecorder
	log      *slog.Logger
}

// New joins group on brokers.
func New(brokers []string, group string, recorder port.BidRecorder, log *slog.Logger) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(bidplaced.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return &Consumer{client: client, recorder: recorder, log: log}, nil
}

// Run consumes until ctx is cancelled. Partitions are processed in parallel,
// records within a partition in order.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.log.ErrorContext(ctx, "kafka fetch failed", "topic", topic, "partition", partition, "error", err)
		})

		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			handled []*kgo.Record
		)
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var last *kgo.Record
				for _, record := range p.Records {
					if !c.handle(ctx, record) {
						break
					}
					last = record
				}
				if last != nil {
					mu.Lock()
					handled = append(handled, last)
					mu.Unlock()
				}
			}()
		})
		wg.Wait()

		if len(handled) == 0 {
			continue
		}
		// The commit outlives cancellation so finished work is not redone.
		if err := c.client.CommitRecords(context.WithoutCancel(ctx), handled...); err != nil {
			c.log.ErrorContext(ctx, "kafka offset commit failed; records will be redelivered", "error", err)
		}
	}
}

// Close leaves the group and releases the client.
func (c *Consumer) Close() { c.client.Close() }

// handle reports whether the record is finished (recorded or dead-lettered).
// It returns false only when ctx is cancelled first.
func (c *Consumer) handle(ctx context.Context, record *kgo.Record) bool {
	bid, err := bidplaced.Decode(record.Value)
	if err != nil {
		err = fmt.Errorf("%w: %w", domain.ErrValidation, err)
	} else {
		err = c.retry(ctx, "record bid", func() error { return c.recorder.RecordBid(ctx, bid) })
	}
	if err == nil {
		return true
	}
	if ctx.Err() != nil {
		return false
	}

	c.log.ErrorContext(ctx, "bid-placed event rejected; sending to dead-letter topic",
		"partition", record.Partition, "offset", record.Offset, "error", err)
	deadLetter := &kgo.Record{
		Topic: bidplaced.DeadLetterTopic,
		Key:   record.Key,
		Value: record.Value,
		Headers: []kgo.RecordHeader{
			{Key: headerError, Value: []byte(err.Error())},
			{Key: headerTopic, Value: []byte(record.Topic)},
			{Key: headerPartition, Value: []byte(strconv.FormatInt(int64(record.Partition), 10))},
			{Key: headerOffset, Value: []byte(strconv.FormatInt(record.Offset, 10))},
		},
	}
	err = c.retry(ctx, "dead-letter", func() error {
		return c.client.ProduceSync(ctx, deadLetter).FirstErr()
	})
	return err == nil
}

// retry runs fn until it succeeds, fails permanently, or ctx is cancelled.
// Transient failures block the partition instead of dropping an audit record.
func (c *Consumer) retry(ctx context.Context, operation string, fn func() error) error {
	backoff := initialBackoff
	for {
		err := fn()
		if err == nil || permanent(err) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.log.WarnContext(ctx, "transient failure; retrying", "operation", operation, "backoff", backoff.String(), "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*backoffFactor, maxBackoff)
	}
}

// permanent reports failures that retrying cannot fix.
func permanent(err error) bool {
	return errors.Is(err, domain.ErrValidation) ||
		errors.Is(err, domain.ErrAuctionNotFound) ||
		errors.Is(err, domain.ErrBidSequenceConflict)
}
