package store

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

// Retention drops the weekly partitions that are entirely older than the
// promise, and it is the one operation in this package that destroys data.
//
// The order is the whole design, and it comes from measurement rather than
// preference (nine-core/docs/artifacts/2026-09-09-what-retention-must-do-to-a-partition.md
// and its 11 September addendum):
//
//  0. take the maintainer lock, then refuse a promise shorter than the horizon
//     keeps alive and a clock ahead of the database's,
//  1. refuse if events carries a default partition, because a default partition
//     makes DETACH CONCURRENTLY fail outright and every run after it,
//  2. refuse any partition that is not wholly past the boundary, which is the
//     board's criterion: an active partition cannot be dropped by accident,
//  2a. finish what a run that stopped after a detach left behind, because that
//     state is invisible to the search in step 2,
//  3. write the record, before anything is touched, which is also where a week
//     that was dropped once and is back is refused, because the record is the
//     only thing that remembers the drop: that refusal arrives per candidate,
//     not up front, so candidates ahead of it in the same run are already
//     gone when it stops,
//  4. detach concurrently, or finish a detach a previous run left pending,
//     because a plain detach takes AccessExclusiveLock on the parent and stops
//     the write path for its duration,
//  5. count and fill the record in, while the partition is standalone,
//  6. drop, and only then stamp the record.
//
// Step three is third because the first version of this had it fifth. Measured
// on a run killed between the detach and the record: the parent no longer held
// the rows, the detached table still held six, no row described it, and the
// next clean run returned nil having seen nothing, because a detached table is
// not inherited and nothing looks for it.
//
// Writing the record was only half of that. Until step 2a existed the row was
// written and never read, so a run that stopped after the detach left the same
// invisible partition with a note beside it that nothing acted on. The record
// is what the criterion asks for, and a record no run reads does not meet it.
//
// The lock is step zero for a reason measured on this branch. Step one reads
// pg_get_expr(relpartbound), which takes a lock on every partition and so waits
// behind a detach already running. With that read ahead of the lock, a second
// run spent seconds inside a check that refuses nothing before it could even
// find out that it was the second run. EnsurePartitions had the two the right
// way round; this now matches it. Nothing that can wait goes before the lock.

// lockTimeout is how long any one statement here waits for a lock before it
// gives up. Thirty seconds because a read still running that long on a
// partition older than the whole retention promise is not an ordinary read,
// and because the maintainer lock is held for the whole wait, so EnsurePartitions
// waits behind it. It is a variable rather than a constant so the test that
// proves the stop is resumable does not have to sit through it.
var lockTimeout = 30 * time.Second

// ErrKeepInsideHorizon is a promise shorter than the weeks the horizon
// maintainer keeps alive. Dropping one of those weeks means the maintainer
// recreates it empty, and a recreated week is the one nine-docs/adr/0002 says
// can hold an event counted twice.
var ErrKeepInsideHorizon = errors.New("the retention promise is inside the horizon the maintainer keeps alive")

// ErrClockAhead is a caller whose idea of now is ahead of the database's. The
// boundary is arithmetic on now, so a fast clock moves it forward and takes
// live partitions with it.
var ErrClockAhead = errors.New("the caller's clock is ahead of the database's")

// ErrWeekReturned is a week that was dropped once and is in the table again.
// The record still says it was dropped, so overwriting that row would leave a
// row vouching for a partition that exists. It is also the state nine-docs/adr/0002
// warns about from the other side: the unique index cannot see what went away
// with the partition, so a returned week may already hold an event counted
// twice, and that is a person's decision rather than a retention run's.
var ErrWeekReturned = errors.New("a week that was already dropped is in the table again")

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
	from, err := parseBound(m[1])
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("retention: lower bound %q: %w", m[1], err)
	}
	to, err := parseBound(m[2])
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("retention: upper bound %q: %w", m[2], err)
	}
	return from.UTC(), to.UTC(), nil
}

// parseBound takes what the server rendered, which depends on its TimeZone: an
// offset of whole hours comes out as +00 and a half hour one as +05:30. A single
// Go layout cannot take both, so both are tried and neither is guessed at.
func parseBound(s string) (time.Time, error) {
	for _, layout := range []string{"2006-01-02 15:04:05-07", "2006-01-02 15:04:05-07:00"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("neither a whole hour nor a half hour offset")
}

// Retain drops every partition whose whole range is older than now minus keep,
// and returns what it dropped. Keep is the promise: 30 days of retention means
// keep = 30 * 24h, and a partition survives until its newest possible row is
// older than that, which is why the interval is the resolution of the promise.
// Retain takes the horizon's span rather than assuming it. The floor below is
// the whole reason: a promise shorter than the weeks the maintainer keeps alive
// drops a week the maintainer then recreates empty. An earlier version of this
// hard coded the default span, and measured on -behind 6 with a promise at that
// default's floor it dropped three weeks the horizon was keeping, four rows
// among them, where the check it replaced had refused the same command. So the
// caller passes the span it runs the maintainer with, and passing a different
// one is the one way left to get this wrong.
func Retain(ctx context.Context, dsn string, now time.Time, keep time.Duration, span PartitionSpan) ([]Dropped, error) {
	if keep <= 0 {
		return nil, fmt.Errorf("retention: keep must be positive, was %s", keep)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("retention: %w", err)
	}
	defer conn.Close(ctx)

	// Neither half of a concurrent detach has a bound of its own, so a single
	// long read on a partition holds the run there for as long as it lasts, and
	// every EnsurePartitions run waits behind it for the maintainer lock.
	// Measured with a reader held open: FINALIZE was still waiting at 6.1s with
	// no timeout, and stopped at 2.1s with one. A detach stopped this way leaves
	// the partition pending, which is the state the next run already finishes,
	// so the timeout turns a silent hang into a stop that resumes itself.
	if _, err := conn.Exec(ctx, fmt.Sprintf(`SET lock_timeout = '%dms'`, lockTimeout.Milliseconds())); err != nil {
		return nil, fmt.Errorf("retention: %w", err)
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

	// The caller's clock decides what is past, and nothing else here checks it:
	// measured, a now 56 days ahead dropped seven partitions including the week
	// holding the real now, with no error. So it is compared against the clock
	// of the database that holds the data, and a caller running ahead is a stop.
	var dbNow time.Time
	if err := conn.QueryRow(ctx, `SELECT now()`).Scan(&dbNow); err != nil {
		return nil, fmt.Errorf("retention: %w", err)
	}
	if skew := now.UTC().Sub(dbNow.UTC()); skew > time.Minute {
		return nil, fmt.Errorf("%w: %s ahead of the database", ErrClockAhead, skew.Round(time.Second))
	}

	// The floor is the span the maintainer keeps behind, and no more: the oldest
	// week it keeps ends one week after its start, so a promise of that many
	// weeks already clears it. Walked across all seven weekdays at exactly this
	// value, the oldest maintained week survived every time.
	floor := time.Duration(span.withDefaults().Behind) * 7 * 24 * time.Hour
	if keep < floor {
		return nil, fmt.Errorf("%w: %s against the %s kept behind by -behind %d",
			ErrKeepInsideHorizon, keep, floor, span.withDefaults().Behind)
	}

	if err := refuseDefaultPartition(ctx, conn); err != nil {
		return nil, err
	}

	// Before any candidate is looked for, because a run that stopped after the
	// detach left something no candidate search can see.
	out, err := resumeUnfinished(ctx, conn, now)
	if err != nil {
		return out, err
	}

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
			return nil, fmt.Errorf("%w: %s, finish it with DETACH ... FINALIZE and then ATTACH it back, in that order", ErrPendingDetachElsewhere, pending)
		}
		candidates = append([]candidate{candidates[i]}, append(candidates[:i:i], candidates[i+1:]...)...)
	}

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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Oldest first by bounds rather than by name, because the guard reads bounds
	// and a name is a convention that a later week could break.
	sort.Slice(out, func(i, j int) bool { return out[i].from.Before(out[j].from) })
	return out, nil
}

// dropOne carries one partition through the order above. It is deliberately not
// one transaction: DETACH CONCURRENTLY cannot run inside one, and the record has
// to be committed before the drop rather than with it.
func dropOne(ctx context.Context, conn *pgx.Conn, c candidate, now time.Time) (Dropped, error) {
	// The record goes first, before anything is touched, and that is the whole
	// point of the order. The first version of this detached and then recorded,
	// and the window between the two was the one state nothing could see: the
	// parent no longer holds the rows, the detached table is not inherited so no
	// later run finds it, and there is no row anywhere. Measured on a run killed
	// there: parent 0 rows, the detached table still holding 6, records 0, and
	// the next clean run returning nil having seen nothing.
	//
	// ON CONFLICT so a run that resumes an unfinished one completes its row, and
	// the WHERE so it only resumes an unfinished one. A row that already says
	// dropped belongs to a week that came back, and updating it would leave the
	// record vouching for a partition that is there.
	var id int64
	err := conn.QueryRow(ctx, `
		INSERT INTO events_partition_drops (partition_name, range_start, range_end)
		VALUES ($1, $2, $3)
		ON CONFLICT (partition_name) DO UPDATE SET range_start = EXCLUDED.range_start
		 WHERE events_partition_drops.dropped_at IS NULL
		RETURNING id`, c.name, c.from, c.to).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return Dropped{}, fmt.Errorf("%w: %s, dropped once already and recreated since", ErrWeekReturned, c.name)
	}
	if err != nil {
		return Dropped{}, fmt.Errorf("retention: record %s: %w", c.name, err)
	}

	// A run that was cancelled between the two halves of a concurrent detach
	// leaves the partition half detached, and repeating the detach then fails
	// with "already pending detach". Measured on PostgreSQL 16: the recovery is
	// FINALIZE, not another detach.
	stmt := fmt.Sprintf(`ALTER TABLE events DETACH PARTITION %s CONCURRENTLY`, quoteIdent(c.name))
	if c.detachIsPending {
		stmt = fmt.Sprintf(`ALTER TABLE events DETACH PARTITION %s FINALIZE`, quoteIdent(c.name))
	}
	if _, err := conn.Exec(ctx, stmt); err != nil {
		return Dropped{}, fmt.Errorf("retention: detach %s: %w", c.name, err)
	}

	// Exact, because the partition is standalone now and nothing else reads it.
	var rows int64
	if err := conn.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, quoteIdent(c.name))).Scan(&rows); err != nil {
		return Dropped{}, fmt.Errorf("retention: count %s: %w", c.name, err)
	}
	if _, err := conn.Exec(ctx, `
		UPDATE events_partition_drops SET rows_dropped = $1, detached_at = $2 WHERE id = $3`,
		rows, now.UTC(), id); err != nil {
		return Dropped{}, fmt.Errorf("retention: record the count for %s: %w", c.name, err)
	}

	if _, err := conn.Exec(ctx, fmt.Sprintf(`DROP TABLE %s`, quoteIdent(c.name))); err != nil {
		return Dropped{}, fmt.Errorf("retention: drop %s, recorded as detached and not dropped: %w", c.name, err)
	}
	if _, err := conn.Exec(ctx, `UPDATE events_partition_drops SET dropped_at = $1 WHERE id = $2`, now.UTC(), id); err != nil {
		return Dropped{}, fmt.Errorf("retention: stamp %s: %w", c.name, err)
	}
	return Dropped{Name: c.name, RangeStart: c.from, RangeEnd: c.to, Rows: rows}, nil
}

// resumeUnfinished finishes what a run that stopped after the detach left
// behind, and it runs before anything else looks for a candidate because that
// state is invisible to every candidate search: a detached partition is not
// inherited, so pastPartitions and pendingDetach look straight past it, and the
// only thing that knows it ever existed is the record written before it was
// touched.
//
// Measured on this branch before it existed: a run stopped between the detach
// and the drop left the partition on disk with its rows, no longer inherited,
// and the next run returned nil having seen nothing. The record was there the
// whole time and nothing read it. This is what turns that record from a trace
// into the thing the criterion says it is.
//
// Reaching that state does not need a kill: cmd/partition runs on
// context.Background() and catches no signal, so Ctrl-C, a scheduler's SIGTERM,
// the OOM killer or a dropped connection all end the process in the same place,
// and since this package sets lock_timeout, so does a lock it could not get.
//
// Two shapes. The partition is still there, which means the drop never ran or
// never finished: count it if the count was never taken, then drop and stamp.
// Or it is gone and only the stamp is missing, which is a run that died between
// the two, and then the row is closed as it stands: a count nobody took stays
// null rather than becoming a zero somebody could read as a measurement.
func resumeUnfinished(ctx context.Context, conn *pgx.Conn, now time.Time) ([]Dropped, error) {
	type unfinished struct {
		id       int64
		name     string
		from, to time.Time
		rows     *int64
		counted  bool
		onDisk   bool
	}
	rows, err := conn.Query(ctx, `
		SELECT d.id, d.partition_name, d.range_start, d.range_end, d.rows_dropped,
		       d.detached_at IS NOT NULL, c.oid IS NOT NULL
		  FROM events_partition_drops d
		  LEFT JOIN pg_class c
		         ON c.relname = d.partition_name AND c.relnamespace = 'public'::regnamespace
		  LEFT JOIN pg_inherits i ON i.inhrelid = c.oid
		 WHERE d.dropped_at IS NULL AND i.inhrelid IS NULL
		 ORDER BY d.range_start`)
	if err != nil {
		return nil, fmt.Errorf("retention: look for an unfinished drop: %w", err)
	}
	var open []unfinished
	for rows.Next() {
		var u unfinished
		if err := rows.Scan(&u.id, &u.name, &u.from, &u.to, &u.rows, &u.counted, &u.onDisk); err != nil {
			return nil, fmt.Errorf("retention: %w", err)
		}
		open = append(open, u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("retention: %w", err)
	}
	rows.Close()

	var out []Dropped
	for _, u := range open {
		var n int64
		switch {
		case u.onDisk:
			n = 0
			if u.counted && u.rows != nil {
				n = *u.rows
			} else {
				if err := conn.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, quoteIdent(u.name))).Scan(&n); err != nil {
					return out, fmt.Errorf("retention: count the unfinished %s: %w", u.name, err)
				}
				if _, err := conn.Exec(ctx, `
					UPDATE events_partition_drops SET rows_dropped = $1, detached_at = $2 WHERE id = $3`,
					n, now.UTC(), u.id); err != nil {
					return out, fmt.Errorf("retention: record the count for the unfinished %s: %w", u.name, err)
				}
			}
			if _, err := conn.Exec(ctx, fmt.Sprintf(`DROP TABLE %s`, quoteIdent(u.name))); err != nil {
				return out, fmt.Errorf("retention: drop the unfinished %s: %w", u.name, err)
			}
		case u.rows != nil:
			n = *u.rows
		}
		if _, err := conn.Exec(ctx, `UPDATE events_partition_drops SET dropped_at = $1 WHERE id = $2`, now.UTC(), u.id); err != nil {
			return out, fmt.Errorf("retention: stamp the unfinished %s: %w", u.name, err)
		}
		out = append(out, Dropped{Name: u.name, RangeStart: u.from, RangeEnd: u.to, Rows: n})
	}
	return out, nil
}

// quoteIdent is the only safe way to put a name this package read from the
// catalog into DDL, which cannot take a parameter. The names come from
// pg_class, so they are already what Postgres accepted, and this keeps a
// partition called "events_w2026_37"; anything stranger is still quoted rather
// than trusted.
func quoteIdent(s string) string {
	return pgx.Identifier{s}.Sanitize()
}
