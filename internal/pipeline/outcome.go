// Package pipeline holds the contract every consumer in this repository is
// written against: what happened to a message, and what the consumer does next.
//
// It imports nothing from this module on purpose. A contract that drags a
// dependency behind it freezes that dependency too, and this one is meant to
// outlive the code around it.
package pipeline

import "context"

// Outcome is the consumer's answer to one message: not what went wrong, but what
// to do next. Every branch of the pipeline reads this and nothing else, so a
// value that is classified wrongly is a message stuck forever or a message lost.
type Outcome int

const (
	// Done means the log may advance. The message is accounted for.
	//
	// This includes the case where nothing was written. An idempotent insert
	// that conflicts writes zero rows and raises nothing, and that is Done: the
	// event is already recorded, by whoever won the race. Reading zero rows as a
	// failure and retrying is a consumer that redelivers the same message
	// forever against a database that will never change its answer.
	Done Outcome = iota

	// Retry means the message is fine and something underneath was not: a broker
	// that went away, a database that refused a connection, a timeout. The
	// message goes to the next delay in the chain and comes back.
	Retry

	// Poison means this message will never process, whatever the system does.
	// A payload that does not parse, a schema version nothing knows. Retrying is
	// wasted work and the only useful action is a person reading it.
	Poison

	// Fatal means the consumer itself is wrong and must stop: configuration that
	// does not hold, a schema mismatch that would corrupt every message rather
	// than this one. Continuing turns one broken deploy into a corrupted log.
	Fatal
)

// Handler processes one decoded message and says what happens to it.
//
// The payload is a type parameter rather than a named event type, so this file
// keeps its promise of importing nothing from this module. A handler binds the
// type it actually consumes.
type Handler[T any] interface {
	Handle(ctx context.Context, message T) (Outcome, error)
}
