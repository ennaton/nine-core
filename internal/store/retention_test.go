package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// CO3.3. The criterion is two sentences: a record of the drop is kept, and an
// active partition cannot be dropped by accident. Both are here, and so is the
// order that makes them true, because the order is what the measurements in
// docs/artifacts/2026-09-09-what-retention-must-do-to-a-partition.md decided.

// Time here is the real clock, because Retain now compares the caller's now
// against the database's and refuses a caller that runs ahead. That is the
// guard, so a test cannot step around it by inventing a future. Instead the
// partitions are created around the real now with the maintainer, which is what
// production does, and the weeks behind it are genuinely past.
const weeksBehind = 6

// olderWeek is a moment inside the week n weeks before this one, which is a
// week the retention boundary can legitimately be past.
func olderWeek(n int) time.Time {
	return time.Now().UTC().AddDate(0, 0, -7*n).Truncate(time.Hour)
}

// withHistory migrates a fresh database and then creates the weeks behind the
// current one, so there is something old enough to drop.
func withHistory(t *testing.T) string {
	t.Helper()
	dsn := freshDSN(t)
	if _, err := EnsurePartitions(context.Background(), dsn, time.Now(), PartitionSpan{Ahead: 1, Behind: weeksBehind}); err != nil {
		t.Fatalf("history: %v", err)
	}
	return dsn
}

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
	dsn := withHistory(t)
	c := conn(t, dsn)
	ctx := context.Background()
	old := olderWeek(4)
	seed(t, c, old, 5)

	dropped, err := Retain(ctx, dsn, time.Now(), 14*24*time.Hour)
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
	dsn := withHistory(t)
	c := conn(t, dsn)
	ctx := context.Background()
	seed(t, c, time.Now().UTC(), 3)

	// A promise longer than the table is old: nothing is past it.
	dropped, err := Retain(ctx, dsn, time.Now(), 3650*24*time.Hour)
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
	if _, err := Retain(ctx, dsn, time.Now(), time.Hour); err != nil {
		t.Fatalf("retain: %v", err)
	}
	if n := rowsIn(t, c, `SELECT count(*) FROM events`); n != 3 {
		t.Fatalf("the week holding now was dropped: events holds %d rows, want 3", n)
	}
}

// A partition that is only partly past the boundary keeps its rows. The
// interval is the resolution of the promise, and the promise is a floor.
func TestAPartitionStraddlingTheBoundarySurvives(t *testing.T) {
	dsn := withHistory(t)
	c := conn(t, dsn)
	ctx := context.Background()
	seed(t, c, time.Now().UTC(), 2)

	// Week 37 runs 7 to 14 September. A boundary inside it must not drop it.
	if _, err := Retain(ctx, dsn, time.Now(), 2*24*time.Hour); err != nil {
		t.Fatalf("retain: %v", err)
	}
	if n := rowsIn(t, c, `SELECT count(*) FROM events`); n != 2 {
		t.Fatalf("a partition straddling the boundary was dropped: events holds %d rows, want 2", n)
	}
}

func TestADefaultPartitionStopsRetention(t *testing.T) {
	dsn := withHistory(t)
	c := conn(t, dsn)
	ctx := context.Background()
	if _, err := c.Exec(ctx, `CREATE TABLE events_default PARTITION OF events DEFAULT`); err != nil {
		t.Fatal(err)
	}
	_, err := Retain(ctx, dsn, time.Now(), 14*24*time.Hour)
	if !errors.Is(err, ErrDefaultPartition) {
		t.Fatalf("retain returned %v, want ErrDefaultPartition: a default partition makes every concurrent detach fail", err)
	}
}

// A run cancelled between the two halves of a concurrent detach leaves the
// partition pending. The next run has to finish it rather than repeat it.
func TestAPendingDetachIsFinishedRatherThanRepeated(t *testing.T) {
	dsn := withHistory(t)
	c := conn(t, dsn)
	ctx := context.Background()
	old := olderWeek(4)
	seed(t, c, old, 4)

	name := partitionName(weekStart(old))
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
	// Cancelled by the server, not by the client: a context timeout races the
	// blocker's rollback, and when the rollback wins the detach completes and
	// the test silently tests nothing. statement_timeout kills the statement
	// while the blocker is still holding the table, every time.
	doomed := conn(t, dsn)
	if _, err := doomed.Exec(ctx, `SET statement_timeout = '1s'`); err != nil {
		t.Fatal(err)
	}
	_, _ = doomed.Exec(ctx, fmt.Sprintf(`ALTER TABLE events DETACH PARTITION %s CONCURRENTLY`, name))
	if _, err := blocker.Exec(ctx, `ROLLBACK`); err != nil {
		t.Fatal(err)
	}

	pending := rowsIn(t, c, `
		SELECT count(*) FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent
		 WHERE p.relname = 'events' AND i.inhdetachpending`)
	if pending != 1 {
		t.Fatalf("the detach left %d partitions pending, want 1: the setup did not produce the state under test", pending)
	}

	dropped, err := Retain(ctx, dsn, time.Now(), 14*24*time.Hour)
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
	dsn := withHistory(t)
	c := conn(t, dsn)
	ctx := context.Background()
	seed(t, c, time.Now().UTC(), 2)

	// Leave the week holding "now" pending, which retention would never drop.
	doomed := conn(t, dsn)
	if _, err := doomed.Exec(ctx, `SET statement_timeout = '1s'`); err != nil {
		t.Fatal(err)
	}
	blocker := conn(t, dsn)
	if _, err := blocker.Exec(ctx, `BEGIN`); err != nil {
		t.Fatal(err)
	}
	if _, err := blocker.Exec(ctx, `SELECT count(*) FROM events`); err != nil {
		t.Fatal(err)
	}
	_, _ = doomed.Exec(ctx, fmt.Sprintf(`ALTER TABLE events DETACH PARTITION %s CONCURRENTLY`, partitionName(weekStart(time.Now().UTC()))))
	if _, err := blocker.Exec(ctx, `ROLLBACK`); err != nil {
		t.Fatal(err)
	}
	if rowsIn(t, c, `SELECT count(*) FROM pg_inherits i JOIN pg_class p ON p.oid=i.inhparent
	                  WHERE p.relname='events' AND i.inhdetachpending`) != 1 {
		t.Fatal("the detach did not end up pending, so the setup did not produce the state under test")
	}

	_, err := Retain(ctx, dsn, time.Now(), 14*24*time.Hour)
	if !errors.Is(err, ErrPendingDetachElsewhere) {
		t.Fatalf("retain returned %v, want ErrPendingDetachElsewhere", err)
	}
	// Not through the parent: a partition with a pending detach is already out
	// of the parent's scans, which is measured here as events reading zero, and
	// is why ingest for that week is broken before retention ever runs. The
	// question this test asks is whether anything was destroyed, so it asks the
	// partition itself.
	name := partitionName(weekStart(time.Now().UTC()))
	if n := rowsIn(t, c, `SELECT count(*) FROM pg_class WHERE relname = $1`, name); n != 1 {
		t.Fatalf("%s no longer exists: the run dropped a partition it had refused to handle", name)
	}
	if n := rowsIn(t, c, fmt.Sprintf(`SELECT count(*) FROM %s`, name)); n != 2 {
		t.Fatalf("%s holds %d rows, want 2", name, n)
	}
	if n := rowsIn(t, c, `SELECT count(*) FROM events_partition_drops WHERE dropped_at IS NOT NULL`); n != 0 {
		t.Fatalf("%d partitions were dropped by a run that was supposed to stop", n)
	}
}

// The centrepiece, and it had no test until a mutation pointed that out: moving
// the record after the drop left every test green. So the drop is made to fail
// while the partition is held, and the question is what the record says then.
func TestTheRecordExistsBeforeThePartitionIsTouched(t *testing.T) {
	dsn := withHistory(t)
	c := conn(t, dsn)
	ctx := context.Background()
	old := olderWeek(4)
	seed(t, c, old, 3)
	name := partitionName(weekStart(old))

	// A reader holding the partition makes the concurrent detach wait, and the
	// context takes Retain out while it is waiting. What is under test is what
	// exists at that moment: the row, written before anything was touched.
	holder := conn(t, dsn)
	if _, err := holder.Exec(ctx, `BEGIN`); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, name)); err != nil {
		t.Fatal(err)
	}
	defer holder.Exec(context.Background(), `ROLLBACK`)

	short, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := Retain(short, dsn, time.Now(), 14*24*time.Hour); err == nil {
		t.Fatal("retain returned nil while the drop was held: the test did not reach the state under test")
	}

	var rows *int64
	var detached, dropped *time.Time
	err := c.QueryRow(ctx, `
		SELECT rows_dropped, detached_at, dropped_at FROM events_partition_drops
		 WHERE partition_name = $1`, name).Scan(&rows, &detached, &dropped)
	if err != nil {
		t.Fatalf("a partition was detached and the record does not exist: %v", err)
	}
	// Three nulls: recorded, and nothing done to it yet. That is the state the
	// first version of this code could not represent, because it recorded after
	// the detach and left the window in between with no row anywhere.
	if rows != nil || detached != nil || dropped != nil {
		t.Fatalf("record says rows=%v detached=%v dropped=%v, want all null on a run stopped at the detach", rows, detached, dropped)
	}
	if n := rowsIn(t, c, fmt.Sprintf(`SELECT count(*) FROM %s`, name)); n != 3 {
		t.Fatalf("%s holds %d rows, want 3: nothing should have been destroyed", name, n)
	}
}

// The boundary is exclusive at the top, so a partition whose upper bound is
// exactly the boundary holds only rows older than the promise and goes.
func TestAPartitionEndingExactlyAtTheBoundaryIsDropped(t *testing.T) {
	dsn := withHistory(t)
	c := conn(t, dsn)
	ctx := context.Background()
	old := olderWeek(3)
	seed(t, c, old, 2)

	now := time.Now().UTC()
	end := weekStart(old).AddDate(0, 0, 7) // the partition's exclusive upper bound
	dropped, err := Retain(ctx, dsn, now, now.Sub(end))
	if err != nil {
		t.Fatalf("retain: %v", err)
	}
	name := partitionName(weekStart(old))
	for _, d := range dropped {
		if d.Name == name {
			return
		}
	}
	t.Fatalf("%s ends exactly at the boundary and was kept; dropped %v", name, dropped)
}

func TestAClockAheadOfTheDatabaseStopsTheRun(t *testing.T) {
	dsn := withHistory(t)
	c := conn(t, dsn)
	ctx := context.Background()
	old := olderWeek(4)
	seed(t, c, old, 2)

	_, err := Retain(ctx, dsn, time.Now().Add(2*time.Hour), 14*24*time.Hour)
	if !errors.Is(err, ErrClockAhead) {
		t.Fatalf("retain returned %v, want ErrClockAhead: a fast host clock moves the boundary forward", err)
	}
	if n := rowsIn(t, c, `SELECT count(*) FROM events`); n != 2 {
		t.Fatalf("events holds %d rows, want 2: nothing should have been dropped", n)
	}
}

func TestKeepMustBePositive(t *testing.T) {
	dsn := withHistory(t)
	if _, err := Retain(context.Background(), dsn, time.Now(), 0); err == nil {
		t.Fatal("a keep of zero was accepted, which would make the boundary now and drop everything past it")
	}
}

// The lock had no test either: removing it left everything green, because
// nothing here ran two maintainers at once. Two Retains on one database is
// exactly how a week ends up detached by one and recreated by the other.
func TestTwoRetentionRunsAtOnceDoNotCollide(t *testing.T) {
	dsn := withHistory(t)
	c := conn(t, dsn)
	ctx := context.Background()
	old := olderWeek(4)
	seed(t, c, old, 3)
	name := partitionName(weekStart(old))

	// The first run is held inside its work by a reader on the partition, so
	// the second arrives while the lock is genuinely taken.
	holder := conn(t, dsn)
	if _, err := holder.Exec(ctx, `BEGIN`); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, name)); err != nil {
		t.Fatal(err)
	}

	first := make(chan error, 1)
	held, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	go func() { _, err := Retain(held, dsn, time.Now(), 14*24*time.Hour); first <- err }()

	// Wait for the lock to be taken, from the database rather than by sleeping.
	deadline := time.After(10 * time.Second)
	for {
		if rowsIn(t, c, `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'`) > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("the first run never took the advisory lock")
		case <-time.After(50 * time.Millisecond):
		}
	}

	_, err := Retain(ctx, dsn, time.Now(), 14*24*time.Hour)
	if err == nil {
		t.Fatal("the second run proceeded while the first held the lock")
	}
	if !strings.Contains(err.Error(), "holds the lock") {
		t.Fatalf("the second run failed with %v, want the lock message", err)
	}
	_, _ = holder.Exec(ctx, `ROLLBACK`)
	<-first
}
