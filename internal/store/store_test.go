package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/ennaton/nine-core/internal/event"
	"github.com/ennaton/nine-core/internal/pipeline"
)

// These run against a real PostgreSQL, the compose one locally and a service
// container in CI, because every claim here is about what Postgres does and
// a fake would only repeat what the test author believed. The migration runs
// into a fresh database per test binary and drops it afterwards.
//
// NINE_TEST_MIGRATE_DSN points at a database the connecting role may create
// databases in. Unset, the tests that need a database are skipped and say so.

func ownerDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("NINE_TEST_MIGRATE_DSN")
	if dsn == "" {
		t.Skip("NINE_TEST_MIGRATE_DSN unset: no PostgreSQL to measure against")
	}
	return dsn
}

// fresh creates a throwaway database, migrates it, and returns a Store on
// it. It connects as the owner, which is more than nine_app has; the grant
// is exercised by the compose run in the artifact, not here.
func fresh(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	owner := ownerDSN(t)
	name := fmt.Sprintf("nine_core_test_%d", rand.Int63())
	admin, err := pgx.Connect(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, "DROP DATABASE "+name+" WITH (FORCE)")
		admin.Close(ctx)
	})
	dsn := withDatabase(owner, name)
	if err := Migrate(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

// withDatabase swaps the database name at the end of a URL style dsn.
func withDatabase(dsn, name string) string {
	i := strings.LastIndex(dsn, "/")
	q := strings.Index(dsn[i:], "?")
	if q < 0 {
		return dsn[:i+1] + name
	}
	return dsn[:i+1] + name + dsn[i+q:]
}

func sample(id string, at time.Time) event.AgentRun {
	return event.AgentRun{Tenant: "tenant-a", EventID: id, OccurredAt: at, Agent: 0, Outcome: 0, DurationMs: 10}
}

var inWeek = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

// The row CO2.2 asks for: the same event_id twice, one row.
func TestTheSameEventTwiceIsOneRow(t *testing.T) {
	s := fresh(t)
	ctx := context.Background()
	if err := s.Insert(ctx, sample("run-000001", inWeek)); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	err := s.Insert(ctx, sample("run-000001", inWeek))
	if !errors.Is(err, ErrNotInserted) {
		t.Fatalf("second insert returned %v, want ErrNotInserted", err)
	}
	if Classify(err) != pipeline.Done {
		t.Fatalf("a duplicate is Done, got %v", Classify(err))
	}
	n, err := s.Count(ctx, "tenant-a", "run-000001")
	if err != nil || n != 1 {
		t.Fatalf("rows = %d, err %v, want 1", n, err)
	}
}

// Deduplication is per tenant: two tenants may use the same event id.
func TestTheKeyIsPerTenant(t *testing.T) {
	s := fresh(t)
	ctx := context.Background()
	a := sample("run-000002", inWeek)
	b := a
	b.Tenant = "tenant-b"
	if err := s.Insert(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(ctx, b); err != nil {
		t.Fatalf("second tenant, same id: %v", err)
	}
}

// The three column key, stated in 0002: a redelivery carries the same
// occurred_at and conflicts; the same id with another timestamp does not.
// Written here so the limit is measured rather than remembered.
func TestTheSameIdWithAnotherTimestampIsAnotherRow(t *testing.T) {
	s := fresh(t)
	ctx := context.Background()
	if err := s.Insert(ctx, sample("run-000003", inWeek)); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(ctx, sample("run-000003", inWeek.Add(time.Hour))); err != nil {
		t.Fatalf("not covered by the key, so it inserts: %v", err)
	}
	if n, _ := s.Count(ctx, "tenant-a", "run-000003"); n != 2 {
		t.Fatalf("rows = %d, want 2: the key is three columns and this is its stated limit", n)
	}
}

// An event dated where no partition exists is 23514, and 0001 revision 3
// makes that Retry: the partition is a dependency CO3.2 has not supplied,
// not a fault in the message.
func TestAnEventOutsideThePartitionHorizonIsRetry(t *testing.T) {
	s := fresh(t)
	err := s.Insert(context.Background(), sample("run-000004", time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)))
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("want 23514, got %v", err)
	}
	// The half of the distinction the database supplies: this 23514 names no
	// constraint, which is what tells it apart from a check the table refuses.
	if pgErr.ConstraintName != "" {
		t.Fatalf("a routing failure named the constraint %q, so the split in Classify is wrong", pgErr.ConstraintName)
	}
	if got := Classify(err); got != pipeline.Retry {
		t.Fatalf("23514 classified as %v, want Retry", got)
	}
}

// The other 23514: a row the table's own check refuses. Written because the
// two share a code and revision 3 is only true while they are told apart.
func TestACheckTheTableRefusesIsFatal(t *testing.T) {
	s := fresh(t)
	e := sample("run-000006", inWeek)
	e.RepoHash = []byte{1, 2, 3} // not 32 bytes: events_repo_hash_is_sha256
	err := s.Insert(context.Background(), e)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("want 23514, got %v", err)
	}
	if pgErr.ConstraintName != "events_repo_hash_is_sha256" {
		t.Fatalf("constraint name is %q, want events_repo_hash_is_sha256", pgErr.ConstraintName)
	}
	if got := Classify(err); got != pipeline.Fatal {
		t.Fatalf("a named check violation classified as %v, want Fatal", got)
	}
}

// A database that is not there is the dependency, not the message.
func TestAConnectionRefusedIsRetry(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close() // nothing listens here now
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = Open(ctx, fmt.Sprintf("postgres://nine_app:x@127.0.0.1:%d/nine_core?connect_timeout=2", port)) // nine:allow-secret, a port nobody listens on
	if err == nil {
		t.Fatal("opened a store against a closed port")
	}
	if got := Classify(err); got != pipeline.Retry {
		t.Fatalf("connection refused classified as %v, want Retry: %v", got, err)
	}
}

// The SQLSTATE half of the table, one code per row, so a row added to 0001
// without a line here is a red build, and the closing rule is pinned.
func TestClassifyCodes(t *testing.T) {
	cases := map[string]pipeline.Outcome{
		"23505": pipeline.Done,  // the same event twice, through another unique index
		"40001": pipeline.Retry, // revision 2
		"57P01": pipeline.Retry, // admin shutdown
		"23514": pipeline.Retry, // revision 3, no partition for the row: no constraint named
		"08006": pipeline.Retry, // connection failure, the class
		"42P01": pipeline.Fatal, // undefined table: the schema is wrong, every message would be
		"22001": pipeline.Fatal, // a value too long: not in the table, the closing rule
		"XX000": pipeline.Fatal, // internal error: the closing rule
	}
	for code, want := range cases {
		if got := Classify(&pgconn.PgError{Code: code}); got != want {
			t.Errorf("%s: %v, want %v", code, got, want)
		}
	}
	// 23514 is two failures under one code, and only the constraint name
	// separates them. Revision 3 drew the line in prose; this is the line in
	// the code, so CO3 cannot add a check that quietly becomes a Retry.
	if got := Classify(&pgconn.PgError{Code: "23514", ConstraintName: "events_repo_hash_is_sha256"}); got != pipeline.Fatal {
		t.Errorf("a named check violation is %v, want Fatal", got)
	}
	if got := Classify(errors.New("something nobody mapped")); got != pipeline.Fatal {
		t.Errorf("an unmapped error is %v, want Fatal", got)
	}
	if got := Classify(context.DeadlineExceeded); got != pipeline.Retry {
		t.Errorf("a timeout is %v, want Retry", got)
	}
}

func TestHandlerAnswersDoneForBothARowAndADuplicate(t *testing.T) {
	s := fresh(t)
	h := Handler{Store: s, Log: slog.New(slog.DiscardHandler)}
	for i := 0; i < 2; i++ {
		o, err := h.Handle(context.Background(), sample("run-000005", inWeek))
		if o != pipeline.Done || err != nil {
			t.Fatalf("pass %d: %v %v", i, o, err)
		}
	}
}
