// Command partition keeps the weekly horizon of events ahead of the clock.
//
// It is separate from cmd/core for the same reason cmd/migrate is: creating a
// partition is DDL, the consumer connects as nine_app and owns nothing, and a
// consumer that could extend the schema would be one holding the owner's
// password. It is separate from cmd/migrate because a migration runs once at
// deploy and this runs on a schedule, weekly or nightly; running it more often
// costs nothing, since it creates nothing when the horizon is already there.
//
// It creates nothing in response to an event. An event dated outside the
// horizon is a Retry by nine-docs/adr/0001 revision 3, and it waits for the
// chain rather than deciding how many partitions this table has.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ennaton/nine-core/internal/store"
)

func main() {
	span := store.PartitionSpan{}
	flag.IntVar(&span.Ahead, "ahead", 4, "whole weeks past the current one that must exist")
	flag.IntVar(&span.Behind, "behind", 2, "weeks before the current one that must exist, for events reported late")
	list := flag.Bool("list", false, "print every partition and its bounds, and create nothing")
	// Retention lives here because partition DDL is one job and belongs to one
	// owner, and behind an explicit flag because this is the half that destroys
	// data: creating an empty partition costs milliseconds and can be run by
	// habit, dropping one cannot be undone. Zero means do nothing, so a
	// scheduled run that forgets the flag extends the horizon and drops nothing.
	retain := flag.Duration("retain", 0, "drop partitions wholly older than this, for example 720h; zero drops nothing")
	flag.Parse()

	dsn := os.Getenv("NINE_CORE_MIGRATE_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:15432/nine_core" // nine:allow-secret, the compose dev stack
	}
	ctx := context.Background()

	if err := run(ctx, os.Stdout, dsn, span, *list, *retain); err != nil {
		fail(err)
	}
}

// run is main's body with its output and its inputs passed in, so the order
// these two operations happen in has a test rather than a reading.
func run(ctx context.Context, out io.Writer, dsn string, span store.PartitionSpan, list bool, retain time.Duration) error {
	if list {
		parts, err := store.Partitions(ctx, dsn)
		if err != nil {
			return err
		}
		for name, bound := range parts {
			fmt.Fprintf(out, "%s  %s\n", name, bound)
		}
		fmt.Fprintf(out, "%d partitions\n", len(parts))
		return nil
	}

	// The horizon first, and the drop after it, because the flag used to return
	// before this. A scheduled job given -retain never extended the horizon, so
	// the command that exists to keep the table writable only ever destroyed:
	// once the last week ahead ran out, every insert into events would fail
	// with 23514 and no partition to take it. refuseDefaultPartition rules out
	// the catch-all that would otherwise hide it.
	created, err := store.EnsurePartitions(ctx, dsn, time.Now(), span)
	if err != nil {
		return err
	}
	if len(created) == 0 {
		fmt.Fprintln(out, "the horizon is already there, nothing created")
	}
	for _, name := range created {
		fmt.Fprintln(out, "created "+name)
	}

	if retain <= 0 {
		return nil
	}

	// The same span the maintainer runs with, so the floor inside store.Retain
	// is measured against the horizon this command actually keeps rather than
	// against a default nobody chose.
	dropped, err := store.Retain(ctx, dsn, time.Now(), retain, span)
	for _, d := range dropped {
		fmt.Fprintf(out, "dropped %s covering %s to %s, %d rows\n",
			d.Name, d.RangeStart.Format(time.DateOnly), d.RangeEnd.Format(time.DateOnly), d.Rows)
	}
	if err != nil {
		// Whatever was dropped before the failure is still dropped, and the
		// record says so; printing it first is how the operator knows where it
		// stopped rather than guessing from the error alone.
		return err
	}
	if len(dropped) == 0 {
		fmt.Fprintln(out, "nothing is wholly past the boundary, nothing dropped")
	}
	return nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
