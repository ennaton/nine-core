package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// The events table is partitioned by week and the weeks have to exist before
// the rows do. This is CO3.2: the mechanism that keeps the horizon ahead of
// the writes.
//
// It runs ahead of the clock and never behind the data. Creating a partition
// because an event asked for one would let a client with a wrong clock decide
// how many partitions this table has, and every query in the system pays for
// that: measured on PostgreSQL 16, planning the same one week query costs
// 2.03 ms over 12 partitions, 2.50 ms over 52, and 10.46 ms over 520. A
// handful of events dated across two centuries would put the table in the
// third column permanently. So an event outside the horizon stays what
// nine-docs/adr/0001 revision 3 makes it, a Retry that waits for the chain
// and parks with its date readable, and the horizon is a function of now.
//
// It is not in cmd/core. The consumer connects as nine_app, which owns
// nothing and holds SELECT and INSERT, and creating a partition is DDL. A
// consumer that could extend the schema would be a consumer holding the
// owner's password, which is the line CO2.2 drew.

// PartitionSpan is the horizon EnsurePartitions maintains, in weeks either
// side of the moment it runs.
type PartitionSpan struct {
	// Ahead is how many whole weeks past the current one must exist. Four is
	// the default: it survives a maintainer that has not run for three weeks,
	// which is longer than any outage that leaves the database up.
	Ahead int
	// Behind is how many weeks before the current one must exist, for an
	// agent that batched its events and reports late. Two by default.
	Behind int
}

func (s PartitionSpan) withDefaults() PartitionSpan {
	if s.Ahead <= 0 {
		s.Ahead = 4
	}
	if s.Behind <= 0 {
		s.Behind = 2
	}
	return s
}

// ErrDefaultPartition is the refusal core#14 asks for. A default partition
// silently swallows every row the horizon does not cover, which turns the
// missing week from a Retry into a row in the wrong place, and it makes
// retention impossible: measured there, DETACH CONCURRENTLY answers
// "cannot detach partitions concurrently when a default partition exists".
// So the maintainer looks for one before it does anything and stops loudly.
var ErrDefaultPartition = errors.New("events carries a default partition, which breaks retention and hides the horizon")

// weekStart is the Monday, in UTC, of the week holding t. The bounds have to
// match 00001_events.sql exactly or the same week would end up with two
// partitions under two names, and the second would be rejected as an overlap.
func weekStart(t time.Time) time.Time {
	t = t.UTC().Truncate(24 * time.Hour)
	offset := (int(t.Weekday()) + 6) % 7 // Monday is 0
	return t.AddDate(0, 0, -offset)
}

// partitionName is the name 00001_events.sql produced with
// to_char(date, 'IYYY_IW'): the ISO year and week, which is why the year in
// the name is the ISO year and not the calendar one.
func partitionName(weekStart time.Time) string {
	year, week := weekStart.ISOWeek()
	return fmt.Sprintf("events_w%d_%02d", year, week)
}

// EnsurePartitions creates every weekly partition the span asks for and
// returns the ones it had to create. It is safe to run at any time, from any
// number of processes: it is serialised on an advisory lock, because
// CREATE TABLE IF NOT EXISTS is not atomic and two maintainers racing on the
// same week get "relation already exists" rather than one of them yielding.
// Measured on PostgreSQL 16, that is exactly what two concurrent sessions got.
//
// Each creation takes AccessExclusiveLock on the parent, so the write path
// pauses for the length of the statement: measured at 2 to 24 ms per
// partition. That is the reason the span is small and the maintainer runs on
// a schedule rather than per message.
func EnsurePartitions(ctx context.Context, dsn string, now time.Time, span PartitionSpan) ([]string, error) {
	span = span.withDefaults()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("partitions: %w", err)
	}
	defer conn.Close(ctx)

	// One maintainer at a time. The key is a constant rather than a hash of a
	// string so it is greppable and cannot change under a Postgres upgrade.
	const lockKey = 0x6e696e6531 // "nine1"
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockKey); err != nil {
		return nil, fmt.Errorf("partitions: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", lockKey)
	}()

	if err := refuseDefaultPartition(ctx, conn); err != nil {
		return nil, err
	}

	var created []string
	start := weekStart(now).AddDate(0, 0, -7*span.Behind)
	for i := 0; i <= span.Behind+span.Ahead; i++ {
		from := start.AddDate(0, 0, 7*i)
		to := from.AddDate(0, 0, 7)
		name := partitionName(from)
		// Ask before creating. IF NOT EXISTS reports the same command tag
		// whether it created or skipped, so it cannot say what this run
		// actually did, and the run's own report is the thing an operator
		// reads. Under the advisory lock the two statements are one
		// decision.
		var exists bool
		if err := conn.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, name).Scan(&exists); err != nil {
			return created, fmt.Errorf("partitions: looking for %s: %w", name, err)
		}
		if exists {
			continue
		}
		// The name and the bounds are built here, not taken from a caller,
		// so there is no string from outside this function in the statement.
		if _, err := conn.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE %s PARTITION OF events FOR VALUES FROM ('%s') TO ('%s')`,
			name, from.Format(time.RFC3339), to.Format(time.RFC3339))); err != nil {
			return created, fmt.Errorf("partitions: creating %s: %w", name, err)
		}
		created = append(created, name)
	}
	return created, nil
}

// refuseDefaultPartition stops before any DDL when events carries a default.
func refuseDefaultPartition(ctx context.Context, conn *pgx.Conn) error {
	var name string
	err := conn.QueryRow(ctx, `
		SELECT c.relname
		  FROM pg_class c
		  JOIN pg_inherits i ON i.inhrelid = c.oid
		 WHERE i.inhparent = 'events'::regclass
		   AND pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT'
		 LIMIT 1`).Scan(&name)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil
	case err != nil:
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			return fmt.Errorf("partitions: the events table does not exist, run cmd/migrate first")
		}
		return fmt.Errorf("partitions: %w", err)
	}
	return fmt.Errorf("%w: %s", ErrDefaultPartition, name)
}

// Partitions lists the partitions of events with their bounds, newest first.
// It is what the maintainer prints and what a test reads.
func Partitions(ctx context.Context, dsn string) (map[string]string, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx, `
		SELECT c.relname, pg_get_expr(c.relpartbound, c.oid)
		  FROM pg_class c
		  JOIN pg_inherits i ON i.inhrelid = c.oid
		 WHERE i.inhparent = 'events'::regclass
		 ORDER BY 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var name, bound string
		if err := rows.Scan(&name, &bound); err != nil {
			return nil, err
		}
		out[name] = bound
	}
	return out, rows.Err()
}
