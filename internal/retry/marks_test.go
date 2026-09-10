package retry

import (
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var at = time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)

// A record straight off events has never been retried, and the absence of the
// header is what says so. Reading it as anything else would put a first
// failure at round one and cost the record a tier.
func TestARecordOffEventsHasSpentNoRounds(t *testing.T) {
	r := &kgo.Record{Topic: "events"}
	n, err := Round(r)
	if err != nil || n != 0 {
		t.Fatalf("round %d, err %v, want 0 and nil", n, err)
	}
	m, err := Read(r)
	if err != nil {
		t.Fatal(err)
	}
	if m.Round != 0 || m.EventID != "" || !m.FirstFailedAt.IsZero() {
		t.Fatalf("a bare record read as %+v", m)
	}
	if m.SourceTopic != "events" {
		t.Fatalf("source topic %q, want the topic it was read from", m.SourceTopic)
	}
}

// The row's criterion, at the count the chain actually has. Each hop adds
// one, and the marks a record leaves with are the marks the next reader sees.
func TestTheRoundRisesByOnePerHop(t *testing.T) {
	r := &kgo.Record{Topic: "events", Value: []byte(`{}`)}
	topics := []string{"events.retry-5m", "events.retry-1h"}
	for i, next := range topics {
		m, err := Next(r, "run-1", "08006", "", at.Add(time.Duration(i)*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		if m.Round != i+1 {
			t.Fatalf("hop %d produced round %d, want %d", i, m.Round, i+1)
		}
		m.Apply(r)
		r.Topic = next
		// What the next reader sees, from the headers alone.
		got, err := Round(r)
		if err != nil || got != i+1 {
			t.Fatalf("after hop %d the record reads as round %d, err %v", i, got, err)
		}
	}
	// The chain is two tiers, so the shipped ceiling is two. The board's
	// criterion says three, from the ladder adr/0001 removed.
	if n, _ := Round(r); n != 2 {
		t.Fatalf("after both tiers the round is %d, want 2", n)
	}
}

// The reason the first failure is a header. adr/0003 measured that a
// forwarded record's own timestamp is the moment of the forward, so the time
// of the failure survives only if it is carried, and it must not be rewritten
// on the way.
func TestTheFirstFailureSurvivesEveryHop(t *testing.T) {
	r := &kgo.Record{Topic: "events"}
	first, err := Next(r, "run-1", "08006", "", at)
	if err != nil {
		t.Fatal(err)
	}
	first.Apply(r)
	r.Topic = "events.retry-5m"

	later, err := Next(r, "run-1", "08006", OutcomeRetryExhausted, at.Add(65*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !later.FirstFailedAt.Equal(at) {
		t.Fatalf("first failure moved to %s, want %s", later.FirstFailedAt, at)
	}
	if later.ParkedAt.Equal(later.FirstFailedAt) {
		t.Fatal("parked at and first failed at are the same, and the gap between them is the point")
	}
	if later.ParkedAt.Sub(later.FirstFailedAt) != 65*time.Minute {
		t.Fatalf("the record says it waited %s, want 65m", later.ParkedAt.Sub(later.FirstFailedAt))
	}
}

// A round nothing can parse counts as spent, not as zero. Read as zero, the
// record goes back to the top of the chain and can do that forever.
func TestAnUnreadableRoundIsAnErrorRatherThanAZero(t *testing.T) {
	for _, bad := range []string{"", "two", "-1", "1.5", "٣"} {
		r := &kgo.Record{Topic: "events.retry-5m", Headers: []kgo.RecordHeader{{Key: HeaderRound, Value: []byte(bad)}}}
		if _, err := Round(r); !errors.Is(err, ErrUnreadableRound) {
			t.Errorf("round %q gave %v, want ErrUnreadableRound", bad, err)
		}
		if _, err := Read(r); !errors.Is(err, ErrUnreadableRound) {
			t.Errorf("reading %q gave %v, want ErrUnreadableRound", bad, err)
		}
	}
}

// Applying marks twice leaves one of each, so a record does not arrive
// carrying two rounds for a reader to choose between.
func TestApplyReplacesRatherThanAppends(t *testing.T) {
	r := &kgo.Record{Topic: "events", Headers: []kgo.RecordHeader{
		{Key: "traceparent", Value: []byte("00-abc-def-01")},
		{Key: HeaderRound, Value: []byte("1")},
		{Key: HeaderParkedAt, Value: []byte("2026-01-01T00:00:00Z")},
	}}
	m, err := Next(r, "run-1", "23514", "", at)
	if err != nil {
		t.Fatal(err)
	}
	m.Apply(r)

	seen := map[string]int{}
	for _, h := range r.Headers {
		seen[h.Key]++
	}
	for key, n := range seen {
		if n != 1 {
			t.Errorf("%s appears %d times", key, n)
		}
	}
	if seen["traceparent"] != 1 {
		t.Error("a header this package does not own was dropped")
	}
	// The stale mark is gone rather than carried: this hop is not a parking.
	if _, ok := header(r, HeaderParkedAt); ok {
		t.Error("a nine-parked-at from an earlier trip survived a hop that did not park the record")
	}
	if n, _ := Round(r); n != 2 {
		t.Errorf("round is %d after one hop from 1, want 2", n)
	}
}

// Decision 2 of adr/0003, as an assertion rather than a sentence. A caller
// with a driver message and nothing else is exactly who this refuses.
func TestNoHeaderCarriesDriverText(t *testing.T) {
	r := &kgo.Record{Topic: "events"}
	m, err := Next(r, "run-1", "", OutcomePoison, at)
	if err != nil {
		t.Fatal(err)
	}
	if m.FailureCode != CodeUnknown {
		t.Fatalf("an unnamed failure carries %q, want %q", m.FailureCode, CodeUnknown)
	}
	detail := `ERROR: new row violates check constraint "events_repo_hash_is_sha256" DETAIL: Failing row contains (t, a, 2026-09-08, \x0102)`
	for _, bad := range []string{detail, "connection refused", "23514: no partition", "unknown error", ""} {
		if bad == "" {
			continue
		}
		if _, err := Next(r, "run-1", bad, OutcomePoison, at); !errors.Is(err, ErrInvalidFailureCode) {
			t.Errorf("code %.40q gave %v, want ErrInvalidFailureCode", bad, err)
		}
	}
	for _, good := range []string{"23514", "40001", "08006", "XX000", "timeout", "unreachable", "decode", "schema", CodeUnknown} {
		if _, err := Next(r, "run-1", good, OutcomePoison, at); err != nil {
			t.Errorf("code %q was refused: %v", good, err)
		}
	}
}

// An unreadable round counts as spent, so a record that cannot say how far it
// has come does not start again.
func TestAnUnreadableRoundCountsAsSpent(t *testing.T) {
	const tiers = 2
	// A header that is absent and one that is present and empty are not the
	// same record: the first has never been retried, the second cannot say.
	cases := []struct {
		name    string
		headers []kgo.RecordHeader
		want    bool
	}{
		{"no header at all", nil, false},
		{"round 0", []kgo.RecordHeader{{Key: HeaderRound, Value: []byte("0")}}, false},
		{"round 1", []kgo.RecordHeader{{Key: HeaderRound, Value: []byte("1")}}, false},
		{"round 2, both tiers spent", []kgo.RecordHeader{{Key: HeaderRound, Value: []byte("2")}}, true},
		{"round 3, past the end", []kgo.RecordHeader{{Key: HeaderRound, Value: []byte("3")}}, true},
		{"present and empty", []kgo.RecordHeader{{Key: HeaderRound, Value: []byte("")}}, true},
		{"not a number", []kgo.RecordHeader{{Key: HeaderRound, Value: []byte("two")}}, true},
		{"negative", []kgo.RecordHeader{{Key: HeaderRound, Value: []byte("-1")}}, true},
	}
	for _, c := range cases {
		r := &kgo.Record{Topic: "events.retry-5m", Headers: c.headers}
		if got := Spent(r, tiers); got != c.want {
			t.Errorf("%s: spent is %v, want %v", c.name, got, c.want)
		}
	}
}

// The whole set survives a real broker round trip, since a header that Kafka
// drops is a decision that exists only on paper.
func TestTheMarksSurviveTheBroker(t *testing.T) {
	brokers := brokerForTest(t)
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics("marks"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	src := &kgo.Record{Topic: "events"}
	m, err := Next(src, "run-42", "40001", OutcomeRetryExhausted, at)
	if err != nil {
		t.Fatal(err)
	}
	out := &kgo.Record{Topic: "marks", Key: []byte("tenant-a"), Value: []byte(`{"a":1}`)}
	m.Apply(out)
	if err := cl.ProduceSync(ctxFor(t), out).FirstErr(); err != nil {
		t.Fatal(err)
	}
	fetches := cl.PollRecords(ctxFor(t), 1)
	var got *kgo.Record
	fetches.EachRecord(func(r *kgo.Record) { got = r })
	if got == nil {
		t.Fatal("nothing came back")
	}
	back, err := Read(got)
	if err != nil {
		t.Fatal(err)
	}
	if back.EventID != "run-42" || back.Round != 1 || back.Outcome != OutcomeRetryExhausted ||
		back.FailureCode != "40001" || back.SourceTopic != "events" ||
		!back.FirstFailedAt.Equal(at) || !back.ParkedAt.Equal(at) {
		t.Fatalf("came back as %+v", back)
	}
}
