package retry

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ennaton/nine-core/internal/pipeline"
)

// The chain in Go and the chain in the compose script are two descriptions of
// one ladder, which is the disagreement adr/0001 was written to end. This
// reads the script rather than trusting a comment about it.
func TestTheChainMatchesTheTopicScript(t *testing.T) {
	path := "../../../platform/compose/kafka/topics.sh"
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("nine-platform is not checked out beside this repository: %v", err)
	}
	declared := map[string]bool{}
	for _, m := range regexp.MustCompile(`(?m)^create\s+(\S+)`).FindAllStringSubmatch(string(raw), -1) {
		declared[m[1]] = true
	}
	for _, topic := range append(append([]string{}, Chain...), Parked, DLQ) {
		if !declared[topic] {
			t.Errorf("%s is in the Go chain and not in topics.sh, so one of the two is wrong", topic)
		}
	}
	if got := Tiers(); got != 2 {
		t.Errorf("the chain allows %d rounds, and adr/0001 decided two", got)
	}
}

func routes(t *testing.T) *Forwarder {
	t.Helper()
	return &Forwarder{
		log:     slog.New(slog.DiscardHandler),
		now:     func() time.Time { return at },
		codeFor: func(err error) string { return "08006" },
	}
}

// The whole of 0001 decision 3 as a table, read off the chain rather than off
// a number passed in at the call site.
func TestWhereEachOutcomeGoes(t *testing.T) {
	f := routes(t)
	cases := []struct {
		name     string
		topic    string
		round    string
		outcome  pipeline.Outcome
		wantDest string
		wantRnd  int
		wantMark string
	}{
		{"first failure off events", "events", "", pipeline.Retry, "events.retry-5m", 1, ""},
		{"failed the first tier", "events.retry-5m", "1", pipeline.Retry, "events.retry-1h", 2, ""},
		{"failed the last tier", "events.retry-1h", "2", pipeline.Retry, Parked, 2, OutcomeRetryExhausted},
		{"poison off events", "events", "", pipeline.Poison, DLQ, 0, OutcomePoison},
		{"poison on a delay topic", "events.retry-5m", "1", pipeline.Poison, DLQ, 1, OutcomePoison},
		{"an unreadable round", "events.retry-5m", "later", pipeline.Retry, Parked, 2, OutcomeRetryExhausted},
	}
	for _, c := range cases {
		r := &kgo.Record{Topic: c.topic, Value: []byte(`{"event_id":"run-1"}`)}
		if c.round != "" {
			r.Headers = []kgo.RecordHeader{{Key: HeaderRound, Value: []byte(c.round)}}
		}
		dest, m, err := f.route(r, c.outcome, errors.New("the dependency"))
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if dest != c.wantDest {
			t.Errorf("%s: goes to %s, want %s", c.name, dest, c.wantDest)
		}
		if m.Round != c.wantRnd {
			t.Errorf("%s: round %d, want %d", c.name, m.Round, c.wantRnd)
		}
		if m.Outcome != c.wantMark {
			t.Errorf("%s: outcome %q, want %q", c.name, m.Outcome, c.wantMark)
		}
	}
}

// The record that cannot say how far it came is parked, and parked saying
// why. Until CO4.4 was written this could not happen at all: Spent sent it
// here and Read refused to build the marks, so the rule that exists to stop a
// loop stopped the consumer instead.
func TestAnUnreadableRoundIsParkedAndSaysSo(t *testing.T) {
	f := routes(t)
	r := &kgo.Record{Topic: "events.retry-5m", Value: []byte(`{"event_id":"run-1"}`),
		Headers: []kgo.RecordHeader{{Key: HeaderRound, Value: []byte("later")}}}
	dest, m, err := f.route(r, pipeline.Retry, errors.New("the dependency"))
	if err != nil {
		t.Fatalf("routing it gave %v, and this is the record parking exists for", err)
	}
	if dest != Parked {
		t.Errorf("it went to %s, want %s", dest, Parked)
	}
	if m.FailureCode != CodeUnreadableRound {
		t.Errorf("it says %q stopped it, want %q: the dependency did not", m.FailureCode, CodeUnreadableRound)
	}
	if m.Round != Tiers() {
		t.Errorf("parked at round %d, want %d, which is the routing decision already taken", m.Round, Tiers())
	}
	if m.EventID != "run-1" {
		t.Errorf("the id is %q: it is in the body even when the header is not readable", m.EventID)
	}
}

// A record read from somewhere the ladder does not name has no next tier, and
// computing one would send a replay round the chain again.
func TestARecordOffTheChainIsRefused(t *testing.T) {
	f := routes(t)
	r := &kgo.Record{Topic: Parked, Value: []byte(`{"event_id":"run-1"}`)}
	if _, _, err := f.route(r, pipeline.Retry, errors.New("x")); !errors.Is(err, ErrOffChain) {
		t.Fatalf("routing a parked record gave %v, want ErrOffChain", err)
	}
}

// A record still moving carries five marks and one that has stopped carries
// seven, which is adr/0003 revision 1. The empty nine-outcome this replaces
// was a header that was present and said nothing.
func TestAMovingRecordCarriesFiveMarksAndAStoppedOneSeven(t *testing.T) {
	f := routes(t)
	moving := &kgo.Record{Topic: "events", Value: []byte(`{"event_id":"run-1"}`)}
	_, m, err := f.route(moving, pipeline.Retry, errors.New("x"))
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]string{}
	for _, h := range m.Headers() {
		if h.Value == nil || len(h.Value) == 0 {
			t.Errorf("%s is present and empty", h.Key)
		}
		names[h.Key] = string(h.Value)
	}
	if len(names) != 5 {
		t.Fatalf("a moving record carries %d marks: %v", len(names), names)
	}
	if _, ok := names[HeaderOutcome]; ok {
		t.Error("a record on its way to another tier carries a verdict")
	}
	if _, ok := names[HeaderParkedAt]; ok {
		t.Error("a record that is still moving says it has stopped")
	}

	stopped := &kgo.Record{Topic: "events.retry-1h", Value: []byte(`{"event_id":"run-1"}`),
		Headers: []kgo.RecordHeader{{Key: HeaderRound, Value: []byte("2")}, {Key: HeaderFirstFailedAt, Value: []byte("2026-09-10T11:00:00Z")}}}
	_, sm, err := f.route(stopped, pipeline.Retry, errors.New("x"))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, h := range sm.Headers() {
		seen[h.Key] = true
	}
	for _, want := range []string{HeaderEventID, HeaderRound, HeaderOutcome, HeaderFailureCode,
		HeaderSourceTopic, HeaderFirstFailedAt, HeaderParkedAt} {
		if !seen[want] {
			t.Errorf("a parked record does not carry %s, and adr/0003 says all seven", want)
		}
	}
	if len(seen) != 7 {
		t.Errorf("a parked record carries %d marks, want 7", len(seen))
	}
}

// The id is found in the payload on the first hop, which is what lets every
// later reader work without the body. A body that does not parse has none,
// and that limit is the one CO5.1 inherits.
func TestTheEventIdComesFromTheBodyOnTheFirstHop(t *testing.T) {
	if got := eventID(&kgo.Record{Value: []byte(`{"event_id":"run-7","agent":"cursor"}`)}); got != "run-7" {
		t.Errorf("id from a payload is %q, want run-7", got)
	}
	withHeader := &kgo.Record{Value: []byte(`{"event_id":"from-body"}`),
		Headers: []kgo.RecordHeader{{Key: HeaderEventID, Value: []byte("from-header")}}}
	if got := eventID(withHeader); got != "from-header" {
		t.Errorf("id is %q, want the header to win", got)
	}
	if got := eventID(&kgo.Record{Value: []byte(`{not json`)}); got != "" {
		t.Errorf("a poison payload gave the id %q, want none", got)
	}
}

// End to end on a broker: the record lands on the next tier carrying what the
// next reader needs, and a second failure there moves it on again.
func TestARecordWalksTheChain(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1),
		kfake.SeedTopics(1, append(append([]string{}, Chain...), Parked, DLQ)...))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	brokers := c.ListenAddrs()

	f, err := NewForwarder(brokers, slog.New(slog.DiscardHandler), func(error) string { return "08006" })
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	f.now = func() time.Time { return at }

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rec := &kgo.Record{Topic: "events", Key: []byte("tenant-a"), Value: []byte(`{"event_id":"run-9"}`)}
	for _, want := range []string{"events.retry-5m", "events.retry-1h", Parked} {
		if err := f.Forward(ctx, rec, pipeline.Retry, errors.New("the dependency")); err != nil {
			t.Fatal(err)
		}
		got := readOne(t, brokers, want)
		m, err := Read(got)
		if err != nil {
			t.Fatal(err)
		}
		if m.EventID != "run-9" {
			t.Fatalf("on %s the id is %q", want, m.EventID)
		}
		if !strings.HasPrefix(string(got.Value), `{"event_id":"run-9"`) {
			t.Fatalf("on %s the payload changed", want)
		}
		rec = got
	}
	final, err := Read(rec)
	if err != nil {
		t.Fatal(err)
	}
	if final.Round != 2 || final.Outcome != OutcomeRetryExhausted {
		t.Fatalf("parked as round %d, outcome %q, want 2 and %s", final.Round, final.Outcome, OutcomeRetryExhausted)
	}
	if final.SourceTopic != "events.retry-1h" {
		t.Fatalf("the parked record says it came from %s", final.SourceTopic)
	}
}

func readOne(t *testing.T, brokers []string, topic string) *kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var out *kgo.Record
	for out == nil {
		fs := cl.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			t.Fatalf("nothing arrived on %s", topic)
		}
		fs.EachRecord(func(r *kgo.Record) { out = r })
	}
	return out
}
