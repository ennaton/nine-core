// Command core joins the events consumer group and writes what it reads.
//
// The handler is the idempotent insert of CO2.2, in its own transaction, and
// the offset is committed after it returns: nine-docs/adr/0002. The retry,
// parked and dead letter producers are CO4 and add a Sink; until then a Retry
// or Poison outcome stops the process rather than losing the record.
//
// The schema is applied by cmd/migrate, as the owner. This binary connects
// as nine_app and can only insert and read.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ennaton/nine-core/internal/consumer"
	"github.com/ennaton/nine-core/internal/event"
	"github.com/ennaton/nine-core/internal/store"
)

// envelope is the consumer's message type: the decoded event.
type envelope = event.AgentRun

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(log); err != nil {
		log.Error("core stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg := consumer.Config{
		Brokers: strings.Split(env("NINE_KAFKA_BROKERS", "localhost:19092"), ","),
		Group:   env("NINE_CONSUMER_GROUP", "core"),
		Topic:   env("NINE_TOPIC", "events"),
		Log:     log,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := store.Open(ctx, env("NINE_CORE_DSN", "postgres://nine_app:nine_app_dev@localhost:15432/nine_core")) // nine:allow-secret, the compose dev stack
	if err != nil {
		return err
	}
	defer db.Close()

	c, err := consumer.New(cfg, event.Decode, store.Handler{Store: db, Log: log}, faultOptions()...)
	if err != nil {
		return err
	}
	defer c.Close()

	log.Info("core joining", "brokers", cfg.Brokers, "group", cfg.Group, "topic", cfg.Topic, "fault_injection", faultInjection)
	if err := c.Run(ctx); err != nil {
		return fmt.Errorf("run: %w", err)
	}
	log.Info("core leaving")
	return nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
