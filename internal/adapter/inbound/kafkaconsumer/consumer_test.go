package kafkaconsumer_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Thapanut/bidding-engine/internal/adapter/contract/bidplaced"
	"github.com/Thapanut/bidding-engine/internal/adapter/inbound/kafkaconsumer"
	"github.com/Thapanut/bidding-engine/internal/adapter/outbound/kafkapublisher"
	"github.com/Thapanut/bidding-engine/internal/adapter/outbound/memory"
	"github.com/Thapanut/bidding-engine/internal/core/auction/domain"
	"github.com/Thapanut/bidding-engine/internal/core/auction/port"
	"github.com/Thapanut/bidding-engine/internal/core/auction/service"
	"github.com/Thapanut/bidding-engine/internal/kernel"
	"github.com/Thapanut/bidding-engine/internal/testkit"
)

const (
	group     = "test-recorder"
	auctionID = "auction-1"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// countingRecorder counts deliveries and can fail the first few of them.
type countingRecorder struct {
	port.BidRecorder
	calls      atomic.Int64
	failFirstN int64
}

func (r *countingRecorder) RecordBid(ctx context.Context, bid domain.Bid) error {
	if r.calls.Add(1) <= r.failFirstN {
		return errors.New("database unavailable")
	}
	return r.BidRecorder.RecordBid(ctx, bid)
}

func newRepo(t *testing.T) *memory.Repository {
	t.Helper()
	now := time.Now()
	auction, err := domain.NewAuction(auctionID, "seller-1", 10_000, 500, now.Add(-time.Hour), now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	repo := memory.NewRepository()
	if err := repo.CreateAuction(context.Background(), auction); err != nil {
		t.Fatal(err)
	}
	return repo
}

func bids(n int) []domain.Bid {
	now := domain.NormalizeTime(time.Now())
	out := make([]domain.Bid, n)
	for i := range out {
		out[i] = domain.Bid{
			ID: "bid-" + string(rune('a'+i/26)) + string(rune('a'+i%26)), AuctionID: auctionID, BidderID: "bidder-1",
			Amount: 10_000 + kernel.Money(i)*500, Sequence: int64(i + 1),
			PlacedAt: now, AuctionEndsAt: now.Add(time.Hour),
		}
	}
	return out
}

func newPublisher(t *testing.T, brokers []string) *kafkapublisher.Publisher {
	t.Helper()
	publisher, err := kafkapublisher.New(brokers, kafkapublisher.DefaultDeliveryTimeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(publisher.Close)
	return publisher
}

// startConsumer runs a consumer until the returned stop function is called.
func startConsumer(t *testing.T, brokers []string, recorder port.BidRecorder) (stop func()) {
	t.Helper()
	consumer, err := kafkaconsumer.New(brokers, group, recorder, quiet)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := consumer.Run(ctx); err != nil {
			t.Errorf("consumer: %v", err)
		}
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			<-done
			consumer.Close()
		})
	}
	t.Cleanup(stop)
	return stop
}

func TestAC16_DuplicatedAndReorderedEventsArePersistedExactlyOnce(t *testing.T) {
	const distinct = 50
	brokers := testkit.Kafka(t)
	repo := newRepo(t)
	publisher := newPublisher(t, brokers)
	ctx := context.Background()

	all := bids(distinct)
	deliveries := append(append([]domain.Bid(nil), all...), all...)
	rand.Shuffle(len(deliveries), func(i, j int) { deliveries[i], deliveries[j] = deliveries[j], deliveries[i] })
	for _, bid := range deliveries {
		if err := publisher.PublishBidPlaced(ctx, bid); err != nil {
			t.Fatal(err)
		}
	}

	recorder := &countingRecorder{BidRecorder: service.NewRecorder(repo)}
	stop := startConsumer(t, brokers, recorder)
	testkit.Eventually(t, "all deliveries to be handled", func() bool { return recorder.calls.Load() == int64(len(deliveries)) })
	stop()

	audit := repo.Bids(auctionID)
	if len(audit) != distinct {
		t.Fatalf("audit has %d rows, want %d", len(audit), distinct)
	}
	for i, bid := range audit {
		if bid != all[i] {
			t.Fatalf("audit[%d] = %+v, want %+v", i, bid, all[i])
		}
	}
	last := all[distinct-1]
	stored, _ := repo.GetAuction(ctx, auctionID)
	if stored.BidSequence != last.Sequence || stored.CurrentPrice != last.Amount || !stored.EndsAt.Equal(last.AuctionEndsAt) {
		t.Fatalf("auction = %+v, want the state after %+v", stored, last)
	}

	// Offsets were committed: a restarted consumer in the same group only
	// sees what is published after the restart.
	restarted := &countingRecorder{BidRecorder: service.NewRecorder(repo)}
	startConsumer(t, brokers, restarted)
	if err := publisher.PublishBidPlaced(ctx, all[0]); err != nil {
		t.Fatal(err)
	}
	testkit.Eventually(t, "the restarted consumer to receive the new event", func() bool { return restarted.calls.Load() >= 1 })
	time.Sleep(200 * time.Millisecond)
	if n := restarted.calls.Load(); n != 1 {
		t.Fatalf("restarted consumer handled %d events, want only the 1 new event", n)
	}
}

func TestAC17_PoisonEventsGoToDeadLetterTopicWithoutBlocking(t *testing.T) {
	brokers := testkit.Kafka(t)
	repo := newRepo(t)
	publisher := newPublisher(t, brokers)
	ctx := context.Background()

	raw, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(bidplaced.DeadLetterTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	valid := bids(1)[0]
	orphan := valid
	orphan.ID, orphan.AuctionID = "bid-orphan", "no-such-auction"
	conflicting := valid
	conflicting.ID = "bid-conflict"

	garbage := &kgo.Record{Topic: bidplaced.Topic, Key: []byte(auctionID), Value: []byte("not json")}
	if err := raw.ProduceSync(ctx, garbage).FirstErr(); err != nil {
		t.Fatal(err)
	}
	for _, bid := range []domain.Bid{orphan, valid, conflicting} {
		if err := publisher.PublishBidPlaced(ctx, bid); err != nil {
			t.Fatal(err)
		}
	}
	startConsumer(t, brokers, service.NewRecorder(repo))

	const wantDeadLetters = 3
	var deadLetters []*kgo.Record
	pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for len(deadLetters) < wantDeadLetters {
		fetches := raw.PollFetches(pollCtx)
		if pollCtx.Err() != nil {
			t.Fatalf("got %d dead letters, want %d", len(deadLetters), wantDeadLetters)
		}
		deadLetters = append(deadLetters, fetches.Records()...)
	}
	for _, record := range deadLetters {
		var reason string
		for _, header := range record.Headers {
			if header.Key == "x-error" {
				reason = string(header.Value)
			}
		}
		if reason == "" {
			t.Fatalf("dead letter without x-error header: %+v", record)
		}
	}

	testkit.Eventually(t, "the valid bid to be recorded", func() bool { return len(repo.Bids(auctionID)) == 1 })
	if got := repo.Bids(auctionID)[0]; got != valid {
		t.Fatalf("audit = %+v, want only %+v", got, valid)
	}
}

func TestAC18_TransientFailuresAreRetriedNotDropped(t *testing.T) {
	brokers := testkit.Kafka(t)
	repo := newRepo(t)
	bid := bids(1)[0]
	if err := newPublisher(t, brokers).PublishBidPlaced(context.Background(), bid); err != nil {
		t.Fatal(err)
	}

	recorder := &countingRecorder{BidRecorder: service.NewRecorder(repo), failFirstN: 2}
	startConsumer(t, brokers, recorder)
	testkit.Eventually(t, "the bid to be recorded after retries", func() bool { return len(repo.Bids(auctionID)) == 1 })
	if n := recorder.calls.Load(); n != 3 {
		t.Fatalf("recorder called %d times, want 2 failures then 1 success", n)
	}
}
