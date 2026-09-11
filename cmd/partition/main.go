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

	if *list {
		parts, err := store.Partitions(ctx, dsn)
		if err != nil {
			fail(err)
		}
		for name, bound := range parts {
			fmt.Printf("%s  %s\n", name, bound)
		}
		fmt.Printf("%d partitions\n", len(parts))
		return
	}

	if *retain > 0 {
		dropped, err := store.Retain(ctx, dsn, time.Now(), *retain)
		if err != nil {
			fail(err)
		}
		for _, d := range dropped {
			fmt.Printf("dropped %s covering %s to %s, %d rows\n",
				d.Name, d.RangeStart.Format(time.DateOnly), d.RangeEnd.Format(time.DateOnly), d.Rows)
		}
		if len(dropped) == 0 {
			fmt.Println("nothing is wholly past the boundary, nothing dropped")
		}
		return
	}

	created, err := store.EnsurePartitions(ctx, dsn, time.Now(), span)
	if err != nil {
		fail(err)
	}
	if len(created) == 0 {
		fmt.Println("the horizon is already there, nothing created")
		return
	}
	for _, name := range created {
		fmt.Println("created " + name)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
