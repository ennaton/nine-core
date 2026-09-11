package store

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
)

// Retention drops the weekly partitions that are entirely older than the
// promise, and it is the one operation in this package that destroys data.
//
// The order is the whole design, and it comes from measurement rather than
// preference (nine-core/docs/artifacts/2026-09-09-what-retention-must-do-to-a-partition.md):
//
//  1. refuse if events carries a default partition, because a default partition
//     makes DETACH CONCURRENTLY fail outright and every run after it,
//  2. refuse any partition that is not wholly past the boundary, which is the
//     board's criterion: an active partition cannot be dropped by accident,
//  3. detach concurrently, or finish a detach a previous run left pending,
//     because a plain detach takes AccessExclusiveLock on the parent and stops
//     the write path for its duration,
//  4. count and record, while the partition is still a standalone table,
//  5. drop, and only then stamp the record.
//
// Step five is last for the reason nine-billing's V13 arrived at: a run that
// dies between the record and the drop leaves a row that says so, and a
// detached partition nobody recorded is an orphan invisible through the parent.
var ErrPartitionNotPast = errors.New("partition is not wholly past the retention boundary")

// ErrPendingDetachElsewhere is a partition left half detached by something that
// is not this run, and that this run must not finish. Postgres allows one
// pending detach per partitioned table, so it blocks every other detach:
// measured, detaching events_w2026_36 while events_w2026_37 is pending fails
// with 55000 naming the other partition. Finishing it would be the fix if the
// partition were ours to drop, and it is the wrong move otherwise, because
// FINALIZE takes live data out of the table. So a pending detach on a partition
// this run would not have dropped is a stop rather than a step.
var ErrPendingDetachElsewhere = errors.New("a partition outside the retention boundary is half detached")

// Dropped is one line of the record, returned so the caller can print what it
// did rather than asking the database what it just told it.
type Dropped struct {
	Name       string
	RangeStart time.Time
	RangeEnd   time.Time
	Rows       int64
}

// bounds parses what pg_get_expr renders for a range partition. The shape is
// fixed by Postgres, not by us: FOR VALUES FROM ('...') TO ('...').
var boundsRE = regexp.MustCompile(`^FOR VALUES FROM \('([^']+)'\) TO \('([^']+)'\)$`)

func parseBounds(expr string) (time.Time, time.Time, error) {
	m := boundsRE.FindStringSubmatch(expr)
	if m == nil {
		return time.Time{}, time.Time{}, fmt.Errorf("retention: cannot read partition bounds %q", expr)
	}
	const layout = "2006-01-02 15:04:05-07"
	from, err := time.Parse(layout, m[1])
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("retention: lower bound %q: %w", m[1], err)
	}
	to, err := time.Parse(layout, m[2])
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("retention: upper bound %q: %w", m[2], err)
	}
	return from.UTC(), to.UTC(), nil
}

// Retain drops every partition whose whole range is older than now minus keep,
// and returns what it dropped. Keep is the promise: 30 days of retention means
// keep = 30 * 24h, and a partition survives until its newest possible row is
// older than that, which is why the interval is the resolution of the promise.
func Retain(ctx context.Context, dsn string, now time.Time, keep time.Duration) ([]Dropped, error) {
	if keep <= 0 {
		return nil, fmt.Errorf("retention: keep must be positive, was %s", keep)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("retention: %w", err)
	}
	defer conn.Close(ctx)

	if err := refuseDefaultPartition(ctx, conn); err != nil {
		return nil, err
	}

	// One maintainer at a time, the same lock cmd/partition takes: creating and
	// dropping partitions of one table from two processes at once is how a week
	// ends up detached by one and recreated by the other.
	const lockKey = 0x6e696e6531 // "nine1", shared with EnsurePartitions
	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, lockKey).Scan(&locked); err != nil {
		return nil, fmt.Errorf("retention: %w", err)
	}
	if !locked {
		return nil, errors.New("retention: another partition maintainer holds the lock")
	}
	defer conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, lockKey)

	boundary := now.UTC().Add(-keep)
	candidates, err := pastPartitions(ctx, conn, boundary)
	if err != nil {
		return nil, err
	}

	// One pending detach blocks every other detach on the table, so whatever is
	// pending is dealt with before anything else. If it is one of ours the loop
	// below finishes it, and it goes first for that reason. If it is not, this
	// run stops: the alternative is finishing a detach nobody here decided on.
	if pending, err := pendingDetach(ctx, conn); err != nil {
		return nil, err
	} else if pending != "" {
		i := indexOf(candidates, pending)
		if i < 0 {
			return nil, fmt.Errorf("%w: %s, finish or reattach it by hand", ErrPendingDetachElsewhere, pending)
		}
		candidates = append([]candidate{candidates[i]}, append(candidates[:i:i], candidates[i+1:]...)...)
	}

	var out []Dropped
	for _, c := range candidates {
		d, err := dropOne(ctx, conn, c, now)
		if err != nil {
			return out, err
		}
		out = append(out, d)
	}
	return out, nil
}

// pendingDetach returns the partition left half detached, if there is one.
func pendingDetach(ctx context.Context, conn *pgx.Conn) (string, error) {
	var name string
	err := conn.QueryRow(ctx, `
		SELECT c.relname
		  FROM pg_class c
		  JOIN pg_inherits i ON i.inhrelid = c.oid
		 WHERE i.inhparent = 'events'::regclass AND i.inhdetachpending
		 LIMIT 1`).Scan(&name)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return "", nil
	case err != nil:
		return "", fmt.Errorf("retention: %w", err)
	}
	return name, nil
}

func indexOf(cs []candidate, name string) int {
	for i, c := range cs {
		if c.name == name {
			return i
		}
	}
	return -1
}

type candidate struct {
	name            string
	from, to        time.Time
	detachIsPending bool
}

// pastPartitions returns the partitions wholly older than the boundary, oldest
// first, and refuses on anything it cannot read. A partition whose upper bound
// is after the boundary is left alone, which includes the one holding now.
func pastPartitions(ctx context.Context, conn *pgx.Conn, boundary time.Time) ([]candidate, error) {
	rows, err := conn.Query(ctx, `
		SELECT c.relname, pg_get_expr(c.relpartbound, c.oid), i.inhdetachpending
		  FROM pg_class c
		  JOIN pg_inherits i ON i.inhrelid = c.oid
		 WHERE i.inhparent = 'events'::regclass
		 ORDER BY 1`)
	if err != nil {
		return nil, fmt.Errorf("retention: %w", err)
	}
	defer rows.Close()

	var out []candidate
	for rows.Next() {
		var name, expr string
		var pending bool
		if err := rows.Scan(&name, &expr, &pending); err != nil {
			return nil, fmt.Errorf("retention: %w", err)
		}
		from, to, err := parseBounds(expr)
		if err != nil {
			return nil, err
		}
		if to.After(boundary) {
			continue
		}
		out = append(out, candidate{name: name, from: from, to: to, detachIsPending: pending})
	}
	return out, rows.Err()
}

// dropOne carries one partition through the order above. It is deliberately not
// one transaction: DETACH CONCURRENTLY cannot run inside one, and the record has
// to be committed before the drop rather than with it.
func dropOne(ctx context.Context, conn *pgx.Conn, c candidate, now time.Time) (Dropped, error) {
	// A run that was cancelled between the two statements leaves the partition
	// half detached, and repeating the detach then fails with "already pending
	// detach". Measured on PostgreSQL 16: the recovery is FINALIZE, not another
	// detach, and a retention job that retries the detach fails forever.
	stmt := fmt.Sprintf(`ALTER TABLE events DETACH PARTITION %s CONCURRENTLY`, quoteIdent(c.name))
	if c.detachIsPending {
		stmt = fmt.Sprintf(`ALTER TABLE events DETACH PARTITION %s FINALIZE`, quoteIdent(c.name))
	}
	if _, err := conn.Exec(ctx, stmt); err != nil {
		return Dropped{}, fmt.Errorf("retention: detach %s: %w", c.name, err)
	}
	detachedAt := now.UTC()

	// Exact, because the partition is standalone now and nothing else reads it.
	var rows int64
	if err := conn.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, quoteIdent(c.name))).Scan(&rows); err != nil {
		return Dropped{}, fmt.Errorf("retention: count %s: %w", c.name, err)
	}

	var id int64
	err := conn.QueryRow(ctx, `
		INSERT INTO events_partition_drops
		    (partition_name, range_start, range_end, rows_dropped, detached_at)
		VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		c.name, c.from, c.to, rows, detachedAt).Scan(&id)
	if err != nil {
		return Dropped{}, fmt.Errorf("retention: record %s: %w", c.name, err)
	}

	if _, err := conn.Exec(ctx, fmt.Sprintf(`DROP TABLE %s`, quoteIdent(c.name))); err != nil {
		return Dropped{}, fmt.Errorf("retention: drop %s, recorded as detached and not dropped: %w", c.name, err)
	}
	if _, err := conn.Exec(ctx, `UPDATE events_partition_drops SET dropped_at = $1 WHERE id = $2`, now.UTC(), id); err != nil {
		return Dropped{}, fmt.Errorf("retention: stamp %s: %w", c.name, err)
	}
	return Dropped{Name: c.name, RangeStart: c.from, RangeEnd: c.to, Rows: rows}, nil
}

// quoteIdent is the only safe way to put a name this package read from the
// catalog into DDL, which cannot take a parameter. The names come from
// pg_class, so they are already what Postgres accepted, and this keeps a
// partition called "events_w2026_37"; anything stranger is still quoted rather
// than trusted.
func quoteIdent(s string) string {
	return pgx.Identifier{s}.Sanitize()
}
