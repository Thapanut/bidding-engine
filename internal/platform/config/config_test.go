package config_test

import (
	"strings"
	"testing"

	"github.com/Thapanut/bidding-engine/internal/platform/config"
)

func setEnv(t *testing.T, env map[string]string) {
	t.Helper()
	for _, key := range []string{"RUN_MODE", "HTTP_ADDR", "REDIS_ADDR", "KAFKA_BROKERS", "KAFKA_CONSUMER_GROUP", "DATABASE_URL"} {
		t.Setenv(key, env[key])
	}
}

func TestLoad_DefaultsAndBrokerList(t *testing.T) {
	setEnv(t, map[string]string{
		"REDIS_ADDR": "redis:6379", "KAFKA_BROKERS": "k1:9092, k2:9092,", "DATABASE_URL": "postgres://db/auctions",
	})
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Mode != config.ModeAll || !cfg.RunsAPI() || !cfg.RunsConsumer() || cfg.HTTPAddr != ":8080" {
		t.Fatalf("defaults = %+v", cfg)
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[1] != "k2:9092" {
		t.Fatalf("brokers = %q", cfg.KafkaBrokers)
	}
}

func TestLoad_ReportsEveryProblem(t *testing.T) {
	setEnv(t, map[string]string{"RUN_MODE": "nope"})
	_, err := config.Load()
	if err == nil {
		t.Fatal("want an error")
	}
	for _, want := range []string{"RUN_MODE", "DATABASE_URL", "KAFKA_BROKERS"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestLoad_ConsumerModeDoesNotNeedRedis(t *testing.T) {
	setEnv(t, map[string]string{"RUN_MODE": "consumer", "KAFKA_BROKERS": "k1:9092", "DATABASE_URL": "postgres://db/auctions"})
	cfg, err := config.Load()
	if err != nil || cfg.RunsAPI() || !cfg.RunsConsumer() {
		t.Fatalf("cfg = %+v, err = %v", cfg, err)
	}
}
