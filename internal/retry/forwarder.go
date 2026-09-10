package retry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ennaton/nine-core/internal/pipeline"
)

// Forwarder is CO4.4: the consumer.Sink that sends a record where 0001 says
// it goes. A Retry walks one tier down the chain and a record with no tier
// left is parked; a Poison goes to the dead letter topic on first sight.
//
// It produces synchronously, because the consumer commits the source offset
// as soon as Forward returns and a message that was neither written nor
// forwarded is a message that was lost. That is 0002's asymmetry applied one
// layer out.
type Forwarder struct {
	cl  *kgo.Client
	log *slog.Logger
	now func() time.Time
	// codeFor turns the handler's error into a value the vocabulary allows.
	// It is a field so the store's Classify does not have to be imported
	// here, which would point this package at the database.
	codeFor func(error) string
}

// NewForwarder produces onto the same brokers the consumer reads from.
func NewForwarder(brokers []string, log *slog.Logger, codeFor func(error) string) (*Forwarder, error) {
	if codeFor == nil {
		return nil, errors.New("retry: a forwarder needs a way to name a failure")
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		// The same idempotent producer ingest uses: a retried batch cannot
		// land twice on the delay topic.
		kgo.ProducerLinger(0),
	)
	if err != nil {
		return nil, fmt.Errorf("retry: %w", err)
	}
	if log == nil {
		log = slog.Default()
	}
	return &Forwarder{cl: cl, log: log, now: time.Now, codeFor: codeFor}, nil
}

func (f *Forwarder) Close() { f.cl.Close() }

// ErrOffChain is a record this forwarder will not move: it was read from
// somewhere the ladder does not name, so there is no next tier to compute. A
// replay from events.parked reading its own topic is the case that matters,
// and sending it round the chain again would be a loop nobody asked for.
var ErrOffChain = errors.New("retry: the record is not on the chain")

// Forward implements consumer.Sink. It answers only after the destination has
// acknowledged.
func (f *Forwarder) Forward(ctx context.Context, r *kgo.Record, o pipeline.Outcome, cause error) error {
	dest, marks, err := f.route(r, o, cause)
	if err != nil {
		return err
	}
	out := &kgo.Record{Topic: dest, Key: r.Key, Value: r.Value, Headers: r.Headers}
	marks.Apply(out)
	if err := f.cl.ProduceSync(ctx, out).FirstErr(); err != nil {
		return fmt.Errorf("retry: producing to %s: %w", dest, err)
	}
	// o.String(), not o: slog's JSON handler marshals an int type as an int,
	// so "outcome":3 is what a Stringer gets you here. Measured in the
	// end to end run, which is where it was noticed.
	f.log.Info("forwarded",
		"to", dest, "outcome", o.String(), "round", marks.Round,
		"failure_code", marks.FailureCode, "from", r.Topic, "partition", r.Partition, "offset", r.Offset)
	return nil
}

// route is the whole of 0001's decision 3, and it reads the chain rather than
// a number: a record is retried while the ladder has another rung, and parked
// when it does not.
func (f *Forwarder) route(r *kgo.Record, o pipeline.Outcome, cause error) (string, Marks, error) {
	now := f.now()
	code := f.codeFor(cause)
	switch o {
	case pipeline.Poison:
		m, err := Poison(r, eventID(r), code, now)
		return DLQ, m, err
	case pipeline.Retry:
		if !InChain(r.Topic) {
			return "", Marks{}, fmt.Errorf("%w: %s", ErrOffChain, r.Topic)
		}
		// Spent reads the round from the record and counts an unreadable one
		// as spent, so a record that cannot say how far it has come stops
		// here rather than going round forever.
		if next, ok := NextTopic(r.Topic); ok && !Spent(r, Tiers()) {
			m, err := Retry(r, eventID(r), code, now)
			return next, m, err
		}
		m, err := Park(r, eventID(r), code, now)
		return Parked, m, err
	}
	return "", Marks{}, fmt.Errorf("retry: %v is not an outcome this forwards", o)
}

// eventID is the record's id, from the header if it has one and from the
// payload if it does not. The first hop off events has no header yet, and
// writing the id there is what lets every later reader work without the body,
// which is the whole argument of adr/0003.
//
// A payload that does not parse has no id to find, and that is a poison
// record: the one case where the fallback fails is the one case the header
// exists for. The mark is then absent rather than empty, and CO5.1 is where
// that gap is either closed, by having the decoder surface the id from a
// partial parse, or written down as the limit of what a dead letter record
// can say.
func eventID(r *kgo.Record) string {
	if m, err := Read(r); err == nil && m.EventID != "" {
		return m.EventID
	}
	var body struct {
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(r.Value, &body); err != nil {
		return ""
	}
	return body.EventID
}
