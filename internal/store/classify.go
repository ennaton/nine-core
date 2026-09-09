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
		return classifyPg(pgErr)
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

// classifyPg is the SQLSTATE half of the table. Class 08 is the connection
// exception class as a whole; the rest are the individual codes 0001 names,
// and its revisions 2 and 3.
func classifyPg(e *pgconn.PgError) pipeline.Outcome {
	code := e.Code
	switch {
	case code == "23505":
		// The same event, arriving twice, through a unique index the insert
		// did not infer on. Already recorded.
		return pipeline.Done
	case code == "40001", code == "57P01":
		// 40001: two consumers raced above read committed (revision 2).
		// 57P01: admin shutdown, the dependency going away.
		return pipeline.Retry
	case code == "23514":
		// Two different failures share this code and they are not the same
		// answer, which @MustafaKemalV named on the review of revision 3:
		// the revision drew the line and the code did not, so it held only
		// as long as this table had no other check. Measured on PostgreSQL
		// 16, the two differ in what they report:
		//
		//   no partition of relation "ev" found for row   ConstraintName ""
		//   violates check constraint "ev_h_is_sha256"    ConstraintName "ev_h_is_sha256"
		//
		// A row with nowhere to go is the partition CO3.2 has not created,
		// a dependency, so Retry. A row a named constraint refuses is a row
		// this schema will never take, so Fatal by the closing rule. The
		// distinction is now the code's and not the prose's, and CO3 may add
		// a check without silently turning it into a Retry.
		if e.ConstraintName == "" {
			return pipeline.Retry
		}
		return pipeline.Fatal
	case len(code) >= 2 && code[:2] == "08":
		return pipeline.Retry
	}
	return pipeline.Fatal
}
