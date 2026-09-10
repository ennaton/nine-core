// Package retry reads and writes the marks a record carries through the
// delay chain and into the parked or dead letter topics.
//
// The set is nine-docs/adr/0003, which is proposed rather than signed: the
// decision belongs to CO4 and CO4 is @MustafaKemalV's. This implements the
// shape as drafted, in one place, so a change on signature is a change here
// and nowhere else.
//
// Everything the operator or the replay tool needs lives in a header, because
// the body of a poison record does not parse and reading it is the thing that
// already failed. Nothing here writes a driver message into a header: a
// PostgreSQL DETAIL carries the failing row, nine-billing moved exactly that
// out of a column in V13, and a dead letter topic is read by more people than
// a table.
package retry

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// The seven names of adr/0003.
const (
	HeaderEventID       = "nine-event-id"
	HeaderRound         = "nine-retry-round"
	HeaderOutcome       = "nine-outcome"
	HeaderFailureCode   = "nine-failure-code"
	HeaderSourceTopic   = "nine-source-topic"
	HeaderFirstFailedAt = "nine-first-failed-at"
	HeaderParkedAt      = "nine-parked-at"
)

// The closed vocabulary of nine-outcome. A record read in isolation says
// which door it came through without looking at which topic it is on.
const (
	OutcomeRetryExhausted = "retry-exhausted"
	OutcomePoison         = "poison"
)

// CodeUnknown is what nine-failure-code carries when no vocabulary covers the
// failure. It is never the error's own text, which is decision 2 of 0003.
const CodeUnknown = "unknown"

// The closed vocabulary of nine-failure-code: a SQLSTATE, or one of these
// names for a failure that has none. Decision 2 of adr/0003 keeps driver text
// out of every header, and a rule that lives only in a paragraph holds until
// the first caller who has a message to hand and nothing else. So the rule is
// here, and a value outside the vocabulary is refused rather than trimmed.
var codeClasses = map[string]bool{
	CodeUnknown:   true,
	"timeout":     true, // the dependency did not answer in time
	"unreachable": true, // connection refused, reset, no route
	"decode":      true, // the payload is not agent_run.v1
	"schema":      true, // a version this consumer does not know
}

// sqlstate is five characters, digits and capitals, which is the whole of the
// class: 23514, 40001, 08006, XX000.
var sqlstate = regexp.MustCompile(`^[0-9A-Z]{5}$`)

// ErrInvalidFailureCode is a code that is neither a SQLSTATE nor a class this
// vocabulary names. The caller passed something else, and the something else
// a caller usually has is the driver's message.
var ErrInvalidFailureCode = errors.New("nine-failure-code is outside the vocabulary of adr/0003")

// ValidCode reports whether a value may travel in nine-failure-code.
func ValidCode(s string) bool { return codeClasses[s] || sqlstate.MatchString(s) }

// Marks is what a record carries. Times are UTC and serialise as RFC3339.
type Marks struct {
	EventID       string
	Round         int
	Outcome       string
	FailureCode   string
	SourceTopic   string
	FirstFailedAt time.Time
	ParkedAt      time.Time
}

// ErrUnreadableRound is a nine-retry-round nothing can parse.
//
// It is not zero and it is not an error to swallow. A round that reads as
// zero when it was really two puts the record back at the top of the chain,
// and a record that cannot say how many rounds it has spent is a record that
// can spend them forever. So an unreadable round counts as spent: Round
// returns this error and the caller parks. The failure that costs a person
// one look is better than the one that costs a topic a loop.
var ErrUnreadableRound = errors.New("nine-retry-round is not a number")

// Spent reports whether this record has used every tier of the chain, which
// is what sends it to events.parked. An unreadable round counts as spent, and
// that is the decision the error below exists for: read as zero it would go
// back to the top of the chain and could do that forever, so the answer that
// costs a person one look beats the one that costs a topic a loop.
func Spent(r *kgo.Record, tiers int) bool {
	n, err := Round(r)
	if err != nil {
		return true
	}
	return n >= tiers
}

// Round is how many delay tiers this record has already been through. A
// record straight off events carries no such header and has spent none.
func Round(r *kgo.Record) (int, error) {
	raw, ok := header(r, HeaderRound)
	if !ok {
		return 0, nil
	}
	n, err := strconv.Atoi(string(raw))
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%w: %q", ErrUnreadableRound, raw)
	}
	return n, nil
}

// header returns one header's value. Kafka allows a key more than once; the
// first is taken, because a record with two of the same name was written by
// something this system does not have and guessing between them is worse than
// being consistent about it.
func header(r *kgo.Record, key string) ([]byte, bool) {
	for _, h := range r.Headers {
		if h.Key == key {
			return h.Value, true
		}
	}
	return nil, false
}

// Read pulls the marks off a record. A record straight off events carries
// none of them and comes back as a zero Marks, which is not an error: this is
// how it looks the first time it fails.
func Read(r *kgo.Record) (Marks, error) {
	round, err := Round(r)
	if err != nil {
		return Marks{}, err
	}
	m := Marks{Round: round, SourceTopic: r.Topic}
	if v, ok := header(r, HeaderEventID); ok {
		m.EventID = string(v)
	}
	if v, ok := header(r, HeaderOutcome); ok {
		m.Outcome = string(v)
	}
	if v, ok := header(r, HeaderFailureCode); ok {
		m.FailureCode = string(v)
	}
	if v, ok := header(r, HeaderSourceTopic); ok {
		m.SourceTopic = string(v)
	}
	if m.FirstFailedAt, err = readTime(r, HeaderFirstFailedAt); err != nil {
		return Marks{}, err
	}
	if m.ParkedAt, err = readTime(r, HeaderParkedAt); err != nil {
		return Marks{}, err
	}
	return m, nil
}

func readTime(r *kgo.Record, key string) (time.Time, error) {
	v, ok := header(r, key)
	if !ok {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, string(v))
	if err != nil {
		return time.Time{}, fmt.Errorf("%s: %w", key, err)
	}
	return t.UTC(), nil
}

// Next is the marks a record carries on its way to the next topic: the round
// spent, the failure that spent it, and the moment the trouble started.
//
// FirstFailedAt is the record's own if it has one and now if it does not,
// which is the whole reason it is a header. adr/0003 measured that a
// forwarded record's timestamp is the moment it was forwarded, so after five
// minutes and an hour the record's own clock is an hour out from the failure
// it is about.
func Next(r *kgo.Record, eventID, failureCode, outcome string, now time.Time) (Marks, error) {
	prev, err := Read(r)
	if err != nil {
		return Marks{}, err
	}
	now = now.UTC().Truncate(time.Second)
	m := Marks{
		EventID:       eventID,
		Round:         prev.Round + 1,
		Outcome:       outcome,
		FailureCode:   failureCode,
		SourceTopic:   r.Topic,
		FirstFailedAt: prev.FirstFailedAt,
	}
	if m.EventID == "" {
		m.EventID = prev.EventID
	}
	if m.FailureCode == "" {
		m.FailureCode = CodeUnknown
	}
	if !ValidCode(m.FailureCode) {
		// Refused rather than replaced with "unknown": a caller holding a
		// driver message has a bug, and quietly writing "unknown" over it
		// leaves the bug and loses the evidence of it.
		return Marks{}, fmt.Errorf("%w: %q", ErrInvalidFailureCode, m.FailureCode)
	}
	if m.FirstFailedAt.IsZero() {
		m.FirstFailedAt = now
	}
	if outcome == OutcomeRetryExhausted || outcome == OutcomePoison {
		m.ParkedAt = now
	}
	return m, nil
}

// Headers renders the marks in adr/0003's order. A zero time is left out
// rather than written as a zero: a header that is present and meaningless is
// worse than one that is absent, because only the second is obvious.
func (m Marks) Headers() []kgo.RecordHeader {
	hs := []kgo.RecordHeader{
		{Key: HeaderEventID, Value: []byte(m.EventID)},
		{Key: HeaderRound, Value: []byte(strconv.Itoa(m.Round))},
		{Key: HeaderOutcome, Value: []byte(m.Outcome)},
		{Key: HeaderFailureCode, Value: []byte(m.FailureCode)},
		{Key: HeaderSourceTopic, Value: []byte(m.SourceTopic)},
	}
	if !m.FirstFailedAt.IsZero() {
		hs = append(hs, kgo.RecordHeader{Key: HeaderFirstFailedAt, Value: []byte(m.FirstFailedAt.UTC().Format(time.RFC3339))})
	}
	if !m.ParkedAt.IsZero() {
		hs = append(hs, kgo.RecordHeader{Key: HeaderParkedAt, Value: []byte(m.ParkedAt.UTC().Format(time.RFC3339))})
	}
	return hs
}

// Apply puts the marks on a record, replacing any it already carries, so a
// forwarded record never leaves with two rounds on it.
func (m Marks) Apply(r *kgo.Record) {
	mine := map[string]bool{}
	for _, h := range m.Headers() {
		mine[h.Key] = true
	}
	kept := r.Headers[:0:0]
	for _, h := range r.Headers {
		if !mine[h.Key] && !isNineHeader(h.Key) {
			kept = append(kept, h)
		}
	}
	r.Headers = append(kept, m.Headers()...)
}

// isNineHeader keeps a stale mark from surviving a hop that did not rewrite
// it: a nine-parked-at from an earlier trip would say a record stopped moving
// while it is moving.
func isNineHeader(key string) bool {
	switch key {
	case HeaderEventID, HeaderRound, HeaderOutcome, HeaderFailureCode,
		HeaderSourceTopic, HeaderFirstFailedAt, HeaderParkedAt:
		return true
	}
	return false
}
