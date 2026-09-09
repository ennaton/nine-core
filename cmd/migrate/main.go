// Command migrate applies the events schema, as the owner. It is separate
// from cmd/core on purpose: the consumer connects as nine_app, which owns
// nothing, and a binary that can both migrate and consume is a binary that
// has to be handed the owner's password to do the second thing.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ennaton/nine-core/internal/store"
)

func main() {
	dsn := os.Getenv("NINE_CORE_MIGRATE_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:15432/nine_core" // nine:allow-secret, the compose dev stack
	}
	if err := store.Migrate(context.Background(), dsn); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("nine_core is at the latest migration")
}
