package store

import (
	"context"
	"errors"
	"log/slog"

	"github.com/ennaton/nine-core/internal/event"
	"github.com/ennaton/nine-core/internal/pipeline"
)

// Handler is the consumer's handler: one insert, one classification. It is
// the transaction 0002 puts before the offset commit. A recorded event is a
// debug line, not an info one: at the volumes CO3.1 plans for, a line per
// event is the log, and the log should be for what is unusual.
type Handler struct {
	Store *Store
	Log   *slog.Logger
}

func (h Handler) Handle(ctx context.Context, e event.AgentRun) (pipeline.Outcome, error) {
	err := h.Store.Insert(ctx, e)
	outcome := Classify(err)
	switch {
	case errors.Is(err, ErrNotInserted):
		h.Log.Debug("event already recorded", "tenant", e.Tenant, "event_id", e.EventID)
		return outcome, nil
	case err == nil:
		h.Log.Debug("event recorded", "tenant", e.Tenant, "event_id", e.EventID)
		return outcome, nil
	}
	return outcome, err
}
