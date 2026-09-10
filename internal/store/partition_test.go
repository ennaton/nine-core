package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/ennaton/nine-core/internal/pipeline"
)

// freshDSN is fresh() without the Store: the partition maintainer takes a dsn
// because it connects as the owner and the pool in Store is nine_app's.
func freshDSN(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	owner := ownerDSN(t)
	name := fmt.Sprintf("nine_core_part_%d", rand.Int63())
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
	return dsn
}

// The row's criterion: a date whose partition did not exist takes an insert
// without an error, after the mechanism has run. It runs ahead of the clock,
// so the date the test picks is one the fixed block in 00001 never covered.
func TestAWeekWithNoPartitionAcceptsAnInsertOnceTheHorizonReachesIt(t *testing.T) {
	dsn := freshDSN(t)
	ctx := context.Background()
	// Far enough past the migration's twelve weeks that only the maintainer
	// can have created it.
	now := time.Date(2027, 3, 10, 0, 0, 0, 0, time.UTC)
	target := now.AddDate(0, 0, 21) // three weeks out, inside a span of four

	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = s.Insert(ctx, sample("before-the-horizon", target))
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("before the maintainer ran, the insert answered %v, want 23514", err)
	}
	if got := Classify(err); got != pipeline.Retry {
		t.Fatalf("that gap is %v, want Retry: the message waits for the partition", got)
	}

	created, err := EnsurePartitions(ctx, dsn, now, PartitionSpan{})
	if err != nil {
		t.Fatal(err)
	}
	if len(created) == 0 {
		t.Fatal("the maintainer created nothing where the horizon was empty")
	}
	if err := s.Insert(ctx, sample("after-the-horizon", target)); err != nil {
		t.Fatalf("after the maintainer ran, the insert answered %v, want no error", err)
	}
}

// Running it again creates nothing and raises nothing, because a schedule
// that has to be told whether it already ran is a schedule with state.
func TestTheMaintainerIsIdempotent(t *testing.T) {
	dsn := freshDSN(t)
	ctx := context.Background()
	now := time.Date(2027, 6, 2, 0, 0, 0, 0, time.UTC)

	first, err := EnsurePartitions(ctx, dsn, now, PartitionSpan{})
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 7 {
		t.Fatalf("first run created %d, want 7: two behind, the current week, four ahead", len(first))
	}
	second, err := EnsurePartitions(ctx, dsn, now, PartitionSpan{})
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 0 {
		t.Fatalf("second run created %v, want nothing", second)
	}
}

// Two maintainers at once. Without the advisory lock this is the measured
// "relation already exists": CREATE TABLE IF NOT EXISTS checks and creates in
// two steps and the second session loses the race.
func TestTwoMaintainersAtOnceDoNotCollide(t *testing.T) {
	dsn := freshDSN(t)
	now := time.Date(2027, 9, 8, 0, 0, 0, 0, time.UTC)
	var wg sync.WaitGroup
	errs := make([]error, 4)
	counts := make([]int, 4)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			created, err := EnsurePartitions(context.Background(), dsn, now, PartitionSpan{})
			errs[i], counts[i] = err, len(created)
		}(i)
	}
	wg.Wait()
	total := 0
	for i, err := range errs {
		if err != nil {
			t.Errorf("maintainer %d: %v", i, err)
		}
		total += counts[i]
	}
	// Whoever gets the lock first does the work and the rest find it done, so
	// the week is created exactly once however many run.
	if total != 7 {
		t.Errorf("four maintainers created %d partitions between them, want 7", total)
	}
}

// core#14's rule, enforced before any DDL: a default partition makes
// DETACH CONCURRENTLY impossible and swallows the rows the horizon misses.
func TestADefaultPartitionStopsTheMaintainer(t *testing.T) {
	dsn := freshDSN(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "CREATE TABLE events_default PARTITION OF events DEFAULT"); err != nil {
		t.Fatal(err)
	}
	conn.Close(ctx)

	created, err := EnsurePartitions(ctx, dsn, time.Date(2027, 12, 1, 0, 0, 0, 0, time.UTC), PartitionSpan{})
	if !errors.Is(err, ErrDefaultPartition) {
		t.Fatalf("returned %v, want ErrDefaultPartition", err)
	}
	if len(created) != 0 {
		t.Errorf("it created %v before refusing, and the refusal is meant to come first", created)
	}
}

// The bounds have to be Monday to Monday in UTC and meet exactly, or a week
// is either uncovered or claimed twice, and Postgres rejects the overlap.
func TestTheWeeksAreContiguousAndMatchTheMigration(t *testing.T) {
	dsn := freshDSN(t)
	ctx := context.Background()
	// A Wednesday, so the maintainer has to find the Monday itself.
	now := time.Date(2028, 2, 16, 13, 45, 0, 0, time.UTC)
	if _, err := EnsurePartitions(ctx, dsn, now, PartitionSpan{Ahead: 2, Behind: 1}); err != nil {
		t.Fatal(err)
	}
	parts, err := Partitions(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	// The four the maintainer just made, named the way the migration names them.
	for _, want := range []string{"events_w2028_06", "events_w2028_07", "events_w2028_08", "events_w2028_09"} {
		bound, ok := parts[want]
		if !ok {
			t.Fatalf("%s is missing: the naming has drifted from 00001_events.sql", want)
		}
		if bound == "DEFAULT" {
			t.Fatalf("%s is a default partition", want)
		}
	}
	// Monday boundaries: the week holding the Wednesday starts on the 14th.
	if got := parts["events_w2028_07"]; got != "FOR VALUES FROM ('2028-02-14 00:00:00+00') TO ('2028-02-21 00:00:00+00')" {
		t.Fatalf("bounds are %q, want Monday to Monday in UTC", got)
	}
}

// The horizon is a function of now and never of the data. This is the refusal
// the package comment measures: at 520 partitions the planner costs five times
// what it costs at 52, and an event may not decide which column the table
// lives in.
func TestAnEventOutsideTheHorizonCreatesNothing(t *testing.T) {
	dsn := freshDSN(t)
	ctx := context.Background()
	now := time.Date(2029, 4, 4, 0, 0, 0, 0, time.UTC)
	if _, err := EnsurePartitions(ctx, dsn, now, PartitionSpan{}); err != nil {
		t.Fatal(err)
	}
	before, err := Partitions(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// A client whose clock says 2199.
	err = s.Insert(ctx, sample("a-broken-clock", time.Date(2199, 1, 1, 0, 0, 0, 0, time.UTC)))
	if got := Classify(err); got != pipeline.Retry {
		t.Fatalf("classified as %v, want Retry", got)
	}
	after, err := Partitions(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("the insert changed the partition count from %d to %d", len(before), len(after))
	}
}
