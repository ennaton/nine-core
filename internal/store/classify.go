package store

import (
	"context"
	"errors"
	"io/fs"
	"net"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/ennaton/nine-core/internal/pipeline"
)

func errNoRows() error { return pgx.ErrNoRows }

func migrationsFS() fs.FS {
	sub, err := fs.Sub(migrations, "migrations")
	if err != nil {
		panic(err) // the embed directive above guarantees the directory
	}
	return sub
}

// Classify is the mapping table of nine-docs/adr/0001, applied to what the
// insert returned. The rows here are the ones that table names; an error no
// row names is Fatal, the table's closing rule, because a guess dressed as a
// Retry delivers a bug to the parked topic looking like a dependency that was
// down.
//
// The reasoning is on the cause, never the symptom: pgx surfaces the first
// error of a transaction, and a single statement has no 25P02 to confuse it
// with.
func Classify(err error) pipeline.Outcome {
	switch {
	case err == nil, errors.Is(err, ErrNotInserted):
		return pipeline.Done
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		// The database did not answer in time. Nothing about the message is
		// wrong, and the message may or may not have been written; 0002 says
		// the consumer must not commit the offset, and it does not, because
		// Retry does not commit until the sink has it.
		return pipeline.Retry
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return classifyCode(pgErr.Code)
	}
	var netErr net.Error
	if errors.As(err, &netErr) || pgconn.SafeToRetry(err) {
		// Connection refused, reset, timed out: the dependency, not the message.
		return pipeline.Retry
	}
	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return pipeline.Retry
	}
	return pipeline.Fatal
}

// classifyCode is the SQLSTATE half of the table. Class 08 is the connection
// exception class as a whole; the rest are the individual codes 0001 names,
// and its revisions 2 and 3.
func classifyCode(code string) pipeline.Outcome {
	switch {
	case code == "23505":
		// The same event, arriving twice, through a unique index the insert
		// did not infer on. Already recorded.
		return pipeline.Done
	case code == "40001", code == "57P01", code == "23514":
		// 40001: two consumers raced above read committed (revision 2).
		// 57P01: admin shutdown, the dependency going away.
		// 23514 here is "no partition of relation events found for row":
		// the partition for that week does not exist yet, which is a
		// dependency CO3.2 supplies, not a fault in the message (revision 3).
		return pipeline.Retry
	case len(code) >= 2 && code[:2] == "08":
		return pipeline.Retry
	}
	return pipeline.Fatal
}
