// Package testkit holds helpers shared by tests across packages.
package testkit

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/Thapanut/bidding-engine/internal/adapter/contract/bidplaced"
)

const (
	eventuallyTimeout = 30 * time.Second
	eventuallyTick    = 10 * time.Millisecond
	topicPartitions   = 3
)

// Eventually polls condition until it holds or the timeout fails the test.
func Eventually(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(eventuallyTimeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(eventuallyTick)
	}
}

// Kafka starts an in-process fake Kafka cluster with the engine's topics and
// returns its broker addresses.
func Kafka(t *testing.T) []string {
	t.Helper()
	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(topicPartitions, bidplaced.Topic, bidplaced.DeadLetterTopic),
	)
	if err != nil {
		t.Fatalf("start fake kafka: %v", err)
	}
	t.Cleanup(cluster.Close)
	return cluster.ListenAddrs()
}
