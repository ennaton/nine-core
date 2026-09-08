// Command core joins the events consumer group and reads.
//
// Today the handler answers Done for every record after logging its id, which
// is exactly enough to show two instances splitting the topic (CO2.1) and
// nothing more. The database write is CO2.2 and replaces the handler; the
// retry, parked and dead letter producers are CO4 and add a Sink. Until then a
// Retry or Poison outcome stops the process rather than losing the record,
// and this handler never answers either.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ennaton/nine-core/internal/consumer"
	"github.com/ennaton/nine-core/internal/pipeline"
)

// envelope is the little of agent_run.v1 the log handler needs. The full type,
// mirrored from the contract and checked against it the way ingest does,
// arrives with CO2.2.
type envelope struct {
	Tenant  string
	EventID string `json:"event_id"`
}

func decode(key, value []byte) (envelope, error) {
	var e envelope
	if err := json.Unmarshal(value, &e); err != nil {
		return e, err
	}
	if e.EventID == "" {
		return e, errors.New("event_id missing")
	}
	e.Tenant = string(key)
	return e, nil
}

type logHandler struct{ log *slog.Logger }

func (h logHandler) Handle(_ context.Context, e envelope) (pipeline.Outcome, error) {
	h.log.Info("event", "tenant", e.Tenant, "event_id", e.EventID)
	return pipeline.Done, nil
}

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
	c, err := consumer.New(cfg, decode, logHandler{log: log})
	if err != nil {
		return err
	}
	defer c.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("core joining", "brokers", cfg.Brokers, "group", cfg.Group, "topic", cfg.Topic)
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
