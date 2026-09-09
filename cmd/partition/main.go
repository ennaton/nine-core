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
