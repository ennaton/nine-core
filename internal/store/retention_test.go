package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// CO3.3. The criterion is two sentences: a record of the drop is kept, and an
// active partition cannot be dropped by accident. Both are here, and so is the
// order that makes them true, because the order is what the measurements in
// docs/artifacts/2026-09-09-what-retention-must-do-to-a-partition.md decided.

// The first migration lays down twelve weeks from 31 August 2026, so a "now"
// deep inside that range has partitions on both sides of it.
var (
	insideWeek37 = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)   // a Tuesday
	nowInWeek43  = time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC) // seven weeks later
)

func conn(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	c, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

func rowsIn(t *testing.T, c *pgx.Conn, q string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := c.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

// seed writes n events into the week holding at, through the parent.
func seed(t *testing.T, c *pgx.Conn, at time.Time, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		_, err := c.Exec(context.Background(), `
			INSERT INTO events (tenant_id, event_id, occurred_at, agent, duration_ms, outcome)
			VALUES ($1, $2, $3, 0, 1, 0)`,
			"tenant-a", fmt.Sprintf("evt-%d-%d", at.Unix(), i), at)
		if err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
}

func TestAPastPartitionIsRecordedThenDropped(t *testing.T) {
	dsn := freshDSN(t)
	c := conn(t, dsn)
	ctx := context.Background()
	seed(t, c, insideWeek37, 5)

	dropped, err := Retain(ctx, dsn, nowInWeek43, 14*24*time.Hour)
	if err != nil {
		t.Fatalf("retain: %v", err)
	}
	if len(dropped) == 0 {
		t.Fatal("nothing was dropped, and week 37 is seven weeks behind a fourteen day promise")
	}

	// The record is the only thing left, so it has to be complete.
	var name string
	var rows int64
	var detached, droppedAt *time.Time
	err = c.QueryRow(ctx, `
		SELECT partition_name, rows_dropped, detached_at, dropped_at
		  FROM events_partition_drops WHERE rows_dropped > 0`).Scan(&name, &rows, &detached, &droppedAt)
	if err != nil {
		t.Fatalf("the drop left no record: %v", err)
	}
	if rows != 5 {
		t.Fatalf("the record says %d rows, want 5: the count is taken after the detach and is exact", rows)
	}
	if detached == nil || droppedAt == nil {
		t.Fatalf("record has detached_at=%v dropped_at=%v, want both set on a completed drop", detached, droppedAt)
	}
	if n := rowsIn(t, c, `SELECT count(*) FROM pg_class WHERE relname = $1`, name); n != 0 {
		t.Fatalf("%s still exists after the drop", name)
	}
	// And the rest of the table is untouched.
	if n := rowsIn(t, c, `SELECT count(*) FROM events`); n != 0 {
		t.Fatalf("events holds %d rows, want 0: only week 37 was seeded and it was dropped", n)
	}
}

// The board's sentence: an active partition cannot be dropped by accident.
func TestThePartitionHoldingNowIsNeverDropped(t *testing.T) {
	dsn := freshDSN(t)
	c := conn(t, dsn)
	ctx := context.Background()
	seed(t, c, insideWeek37, 3)

	// A promise longer than the table is old: nothing is past it.
	dropped, err := Retain(ctx, dsn, insideWeek37, 365*24*time.Hour)
	if err != nil {
		t.Fatalf("retain: %v", err)
	}
	if len(dropped) != 0 {
		t.Fatalf("dropped %v with a one year promise on a table six weeks wide", dropped)
	}
	if n := rowsIn(t, c, `SELECT count(*) FROM events`); n != 3 {
		t.Fatalf("events holds %d rows, want 3", n)
	}

	// And with a short promise, the week holding now still survives: its upper
	// bound is in the future, so it is not wholly past anything.
	if _, err := Retain(ctx, dsn, insideWeek37, time.Hour); err != nil {
		t.Fatalf("retain: %v", err)
	}
	if n := rowsIn(t, c, `SELECT count(*) FROM events`); n != 3 {
		t.Fatalf("the week holding now was dropped: events holds %d rows, want 3", n)
	}
}

// A partition that is only partly past the boundary keeps its rows. The
// interval is the resolution of the promise, and the promise is a floor.
func TestAPartitionStraddlingTheBoundarySurvives(t *testing.T) {
	dsn := freshDSN(t)
	c := conn(t, dsn)
	ctx := context.Background()
	seed(t, c, insideWeek37, 2)

	// Week 37 runs 7 to 14 September. A boundary inside it must not drop it.
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	if _, err := Retain(ctx, dsn, now, 2*24*time.Hour); err != nil {
		t.Fatalf("retain: %v", err)
	}
	if n := rowsIn(t, c, `SELECT count(*) FROM events`); n != 2 {
		t.Fatalf("a partition straddling the boundary was dropped: events holds %d rows, want 2", n)
	}
}

func TestADefaultPartitionStopsRetention(t *testing.T) {
	dsn := freshDSN(t)
	c := conn(t, dsn)
	ctx := context.Background()
	if _, err := c.Exec(ctx, `CREATE TABLE events_default PARTITION OF events DEFAULT`); err != nil {
		t.Fatal(err)
	}
	_, err := Retain(ctx, dsn, nowInWeek43, 14*24*time.Hour)
	if !errors.Is(err, ErrDefaultPartition) {
		t.Fatalf("retain returned %v, want ErrDefaultPartition: a default partition makes every concurrent detach fail", err)
	}
}

// A run cancelled between the two halves of a concurrent detach leaves the
// partition pending. The next run has to finish it rather than repeat it.
func TestAPendingDetachIsFinishedRatherThanRepeated(t *testing.T) {
	dsn := freshDSN(t)
	c := conn(t, dsn)
	ctx := context.Background()
	seed(t, c, insideWeek37, 4)

	name := "events_w2026_37"
	// Force the half detached state the way a cancelled run produces it: hold a
	// transaction open on the table so the concurrent detach cannot finish, and
	// cancel it.
	blocker := conn(t, dsn)
	if _, err := blocker.Exec(ctx, `BEGIN`); err != nil {
		t.Fatal(err)
	}
	if _, err := blocker.Exec(ctx, `SELECT count(*) FROM events`); err != nil {
		t.Fatal(err)
	}
	// On its own connection: cancelling a statement mid flight leaves the
	// connection unusable and pgx closes it, so the checks below would be
	// talking to a corpse if this shared one.
	doomed := conn(t, dsn)
	short, cancel := context.WithTimeout(ctx, 2*time.Second)
	_, _ = doomed.Exec(short, fmt.Sprintf(`ALTER TABLE events DETACH PARTITION %s CONCURRENTLY`, name))
	cancel()
	if _, err := blocker.Exec(ctx, `ROLLBACK`); err != nil {
		t.Fatal(err)
	}

	pending := rowsIn(t, c, `
		SELECT count(*) FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent
		 WHERE p.relname = 'events' AND i.inhdetachpending`)
	if pending != 1 {
		t.Skipf("the detach did not end up pending (%d), so there is nothing to finish here", pending)
	}

	dropped, err := Retain(ctx, dsn, nowInWeek43, 14*24*time.Hour)
	if err != nil {
		t.Fatalf("retain over a pending detach: %v", err)
	}
	if len(dropped) == 0 {
		t.Fatal("the pending partition was not finished and dropped")
	}
	if n := rowsIn(t, c, `SELECT count(*) FROM events_partition_drops WHERE rows_dropped = 4`); n != 1 {
		t.Fatalf("no record with the four rows that partition held")
	}
}

// The decision the pending case forced: one pending detach blocks every other
// detach on the table, so a partition left half detached that this run would
// not have dropped is a stop rather than something to finish. Finishing it
// would take live data out of the table, which is nobody's decision to make
// halfway through a retention run.
func TestAPendingDetachOutsideTheBoundaryStopsTheRun(t *testing.T) {
	dsn := freshDSN(t)
	c := conn(t, dsn)
	ctx := context.Background()
	seed(t, c, insideWeek37, 2)

	// Leave the week holding "now" pending, which retention would never drop.
	doomed := conn(t, dsn)
	blocker := conn(t, dsn)
	if _, err := blocker.Exec(ctx, `BEGIN`); err != nil {
		t.Fatal(err)
	}
	if _, err := blocker.Exec(ctx, `SELECT count(*) FROM events`); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 2*time.Second)
	_, _ = doomed.Exec(short, `ALTER TABLE events DETACH PARTITION events_w2026_43 CONCURRENTLY`)
	cancel()
	if _, err := blocker.Exec(ctx, `ROLLBACK`); err != nil {
		t.Fatal(err)
	}
	if rowsIn(t, c, `SELECT count(*) FROM pg_inherits i JOIN pg_class p ON p.oid=i.inhparent
	                  WHERE p.relname='events' AND i.inhdetachpending`) != 1 {
		t.Skip("the detach did not end up pending, so there is nothing to refuse")
	}

	_, err := Retain(ctx, dsn, nowInWeek43, 14*24*time.Hour)
	if !errors.Is(err, ErrPendingDetachElsewhere) {
		t.Fatalf("retain returned %v, want ErrPendingDetachElsewhere", err)
	}
	if n := rowsIn(t, c, `SELECT count(*) FROM events`); n != 2 {
		t.Fatalf("events holds %d rows, want 2: the run should have stopped before dropping anything", n)
	}
}
