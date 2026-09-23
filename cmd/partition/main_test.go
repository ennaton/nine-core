package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/ennaton/nine-core/internal/store"
	"github.com/jackc/pgx/v5"
)

// The flag that destroys must not turn off the one that keeps the table
// writable. -retain used to return before EnsurePartitions, so a scheduled job
// running with the flag dropped old weeks and never created new ones, and the
// day the last week ahead ran out every insert would land on no partition at
// all. The assertion is that consequence rather than the call order.
func TestARunThatDropsAlsoExtendsTheHorizon(t *testing.T) {
	owner := os.Getenv("NINE_TEST_MIGRATE_DSN")
	if owner == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("NINE_TEST_MIGRATE_DSN is unset in CI, and a skip here is indistinguishable from a pass")
		}
		t.Skip("NINE_TEST_MIGRATE_DSN unset, no database to run against")
	}
	ctx := context.Background()
	name := fmt.Sprintf("nine_core_part_flag_%d", time.Now().UnixNano())

	admin, err := pgx.Connect(ctx, owner)
	if err != nil {
		t.Fatalf("connect as owner: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		_ = admin.Close(context.Background())
	})

	cfg, err := pgx.ParseConfig(owner)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Database = name
	dsn := fmt.Sprintf("postgres://%s@%s:%d/%s", cfg.User, cfg.Host, cfg.Port, name)
	if cfg.Password != "" {
		dsn = fmt.Sprintf("postgres://%s:%s@%s:%d/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, name)
	}
	if err := store.Migrate(ctx, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// The migration seeds twelve weeks of its own, so asking for a horizon
	// inside them would pass whether or not this command extended anything.
	// Twenty five weeks ahead is past the end of what the migration created,
	// which makes the assertion below about this run and nothing else.
	const ahead = 25
	before, err := store.Partitions(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, io.Discard, dsn, store.PartitionSpan{Ahead: ahead, Behind: 2}, false, 720*time.Hour); err != nil {
		t.Fatalf("the run with -retain: %v", err)
	}
	after, err := store.Partitions(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) <= len(before) {
		t.Fatalf("the run with -retain left %d partitions against %d before it, so the horizon was never extended",
			len(after), len(before))
	}

	// And the consequence, which is the reason the order matters: an event
	// dated inside the horizon this command was asked to keep has somewhere to
	// land. Without the extension it raises 23514 and adr/0001 turns that into
	// a Retry that never succeeds.
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	inside := time.Now().UTC().AddDate(0, 0, 7*(ahead-3))
	if _, err := conn.Exec(ctx, `
		INSERT INTO events (tenant_id, event_id, occurred_at, agent, duration_ms, outcome)
		VALUES ('tenant-a', 'flag-1', $1, 0, 1, 0)`, inside); err != nil {
		t.Fatalf("an event dated %s, inside the horizon this run was asked to keep, was refused: %v",
			inside.Format(time.DateOnly), err)
	}
}
