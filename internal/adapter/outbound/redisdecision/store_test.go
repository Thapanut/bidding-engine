package redisdecision_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Thapanut/bidding-engine/internal/adapter/outbound/decisiontest"
	"github.com/Thapanut/bidding-engine/internal/adapter/outbound/redisdecision"
	"github.com/Thapanut/bidding-engine/internal/core/auction/domain"
)

// realRedisEnv points the contract at a real Redis server, e.g. localhost:6379.
const realRedisEnv = "TEST_REDIS_ADDR"

const testPoolTimeout = time.Minute

func newClient(t *testing.T, addr string) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: addr, PoolTimeout: testPoolTimeout})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// The Lua scripts run on miniredis (gopher-lua) so the contract needs no
// external services.
func TestDecisionStoreContract_Miniredis(t *testing.T) {
	decisiontest.Run(t, func(t *testing.T) decisiontest.Harness {
		server := miniredis.RunT(t)
		store := redisdecision.NewStore(newClient(t, server.Addr()), redisdecision.DefaultRetention)
		return decisiontest.Harness{Store: store, Now: time.Now}
	})
}

// The same contract against a real Redis, which is what production runs.
func TestDecisionStoreContract_RealRedis(t *testing.T) {
	addr := os.Getenv(realRedisEnv)
	if addr == "" {
		t.Skipf("%s not set", realRedisEnv)
	}
	decisiontest.Run(t, func(t *testing.T) decisiontest.Harness {
		client := newClient(t, addr)
		store := redisdecision.NewStore(client, redisdecision.DefaultRetention)
		return decisiontest.Harness{Store: store, Now: func() time.Time {
			now, err := client.Time(context.Background()).Result()
			if err != nil {
				t.Fatalf("redis TIME: %v", err)
			}
			return now
		}}
	})
}

func TestLiveStateExpiresAfterRetention(t *testing.T) {
	const retention = time.Hour
	server := miniredis.RunT(t)
	store := redisdecision.NewStore(newClient(t, server.Addr()), retention)
	ctx := context.Background()

	now := time.Now()
	auction, err := domain.NewAuction("auction-ttl", "seller-1", 1000, 100, now.Add(-time.Hour), now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.OpenAuction(ctx, auction); err != nil {
		t.Fatal(err)
	}
	attempt, err := domain.NewBidAttempt("bid-1", auction.ID, "bidder-1", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PlaceBid(ctx, attempt); err != nil {
		t.Fatal(err)
	}

	for _, key := range server.Keys() {
		ttl := server.TTL(key)
		if ttl <= retention || ttl > 2*retention+time.Minute {
			t.Fatalf("TTL of %s = %v, want auction end + retention", key, ttl)
		}
	}
	if n := len(server.Keys()); n != 3 {
		t.Fatalf("keys = %v, want the auction hash, the bid ledger and the recent list", server.Keys())
	}
}
