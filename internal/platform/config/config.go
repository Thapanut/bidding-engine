// Package config loads runtime configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// Mode selects which halves of the engine a process runs.
type Mode string

// Run modes.
const (
	ModeAll      Mode = "all"
	ModeAPI      Mode = "api"
	ModeConsumer Mode = "consumer"
)

const (
	defaultHTTPAddr      = ":8080"
	defaultConsumerGroup = "bidding-engine.bid-recorder"
)

// Config is the full runtime configuration.
type Config struct {
	Mode          Mode
	HTTPAddr      string
	RedisAddr     string
	KafkaBrokers  []string
	ConsumerGroup string
	DatabaseURL   string
}

// Load reads and validates the environment.
func Load() (Config, error) {
	cfg := Config{
		Mode:          Mode(envOr("RUN_MODE", string(ModeAll))),
		HTTPAddr:      envOr("HTTP_ADDR", defaultHTTPAddr),
		RedisAddr:     os.Getenv("REDIS_ADDR"),
		ConsumerGroup: envOr("KAFKA_CONSUMER_GROUP", defaultConsumerGroup),
		DatabaseURL:   os.Getenv("DATABASE_URL"),
	}
	for _, broker := range strings.Split(os.Getenv("KAFKA_BROKERS"), ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			cfg.KafkaBrokers = append(cfg.KafkaBrokers, broker)
		}
	}

	var problems []error
	switch cfg.Mode {
	case ModeAll, ModeAPI, ModeConsumer:
	default:
		problems = append(problems, fmt.Errorf("RUN_MODE must be %s, %s or %s", ModeAll, ModeAPI, ModeConsumer))
	}
	if cfg.DatabaseURL == "" {
		problems = append(problems, errors.New("DATABASE_URL is required"))
	}
	if len(cfg.KafkaBrokers) == 0 {
		problems = append(problems, errors.New("KAFKA_BROKERS is required"))
	}
	if cfg.RedisAddr == "" && cfg.RunsAPI() {
		problems = append(problems, errors.New("REDIS_ADDR is required"))
	}
	if err := errors.Join(problems...); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	return cfg, nil
}

// RunsAPI reports whether this process serves HTTP.
func (c Config) RunsAPI() bool { return c.Mode == ModeAll || c.Mode == ModeAPI }

// RunsConsumer reports whether this process persists bid events.
func (c Config) RunsConsumer() bool { return c.Mode == ModeAll || c.Mode == ModeConsumer }

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
