// Command bidding-engine runs the bidding API (fast path) and the bid-placed
// consumer (slow path). It is the only composition root.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/Thapanut/bidding-engine/internal/adapter/inbound/httpapi"
	"github.com/Thapanut/bidding-engine/internal/adapter/inbound/kafkaconsumer"
	"github.com/Thapanut/bidding-engine/internal/adapter/outbound/kafkapublisher"
	"github.com/Thapanut/bidding-engine/internal/adapter/outbound/postgres"
	"github.com/Thapanut/bidding-engine/internal/adapter/outbound/redisdecision"
	"github.com/Thapanut/bidding-engine/internal/core/auction/service"
	"github.com/Thapanut/bidding-engine/internal/platform/config"
	"github.com/Thapanut/bidding-engine/internal/platform/system"
)

const shutdownTimeout = 10 * time.Second

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("bidding-engine stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("postgres connect: %w", err)
	}
	defer pool.Close()
	repo := postgres.NewRepository(pool)
	if err := repo.Migrate(ctx); err != nil {
		return err
	}

	var (
		wg   sync.WaitGroup
		errs = make(chan error, 2)
	)
	if cfg.RunsConsumer() {
		consumer, err := kafkaconsumer.New(cfg.KafkaBrokers, cfg.ConsumerGroup, service.NewRecorder(repo), log)
		if err != nil {
			return err
		}
		defer consumer.Close()
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- consumer.Run(ctx)
		}()
	}
	if cfg.RunsAPI() {
		redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
		defer func() { _ = redisClient.Close() }()
		publisher, err := kafkapublisher.New(cfg.KafkaBrokers, kafkapublisher.DefaultDeliveryTimeout)
		if err != nil {
			return err
		}
		defer publisher.Close()

		ids, clock := system.UUIDGenerator{}, system.Clock{}
		bidding := service.NewBidding(
			redisdecision.NewStore(redisClient, redisdecision.DefaultRetention),
			publisher, repo, clock, ids)
		app := httpapi.New(bidding, ids, clock, log)

		wg.Add(2)
		go func() {
			defer wg.Done()
			errs <- app.Listen(cfg.HTTPAddr)
		}()
		go func() {
			defer wg.Done()
			<-ctx.Done()
			if err := app.ShutdownWithTimeout(shutdownTimeout); err != nil {
				log.Error("http shutdown", "error", err)
			}
		}()
	}
	log.Info("bidding-engine started", "mode", string(cfg.Mode), "http_addr", cfg.HTTPAddr)

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errs:
		stop()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		runErr = errors.Join(runErr, err)
	}
	return runErr
}
