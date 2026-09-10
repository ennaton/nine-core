package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ennaton/nine-core/internal/pipeline"
)

// The broker is kfake, franz-go's in process cluster. It speaks the group
// protocol, so two members really do split partitions and really do rebalance;
// what it does not prove is the real broker's timing, which the artifact for
// CO2.1 measures against the compose stack once.

const topic = "events"

func cluster(t *testing.T, partitions int32) []string {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(partitions, topic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c.ListenAddrs()
}

func produce(t *testing.T, brokers []string, n int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	recs := make([]*kgo.Record, n)
	for i := range recs {
		recs[i] = &kgo.Record{Topic: topic, Key: []byte(fmt.Sprintf("tenant-%d", i%3)), Value: []byte(fmt.Sprintf("event-%d", i))}
	}
	if err := cl.ProduceSync(context.Background(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// committed sums the group's committed offsets over every partition, which is
// the number of records the group has acknowledged. It is read from the
// broker, not from the consumer, because the claim is about what the broker
// would hand to the next member.
func committed(t *testing.T, brokers []string, group string) int64 {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	offsets, err := kadm.NewClient(cl).FetchOffsets(context.Background(), group)
	if err != nil {
		t.Fatal(err)
	}
	var sum int64
	offsets.Each(func(o kadm.OffsetResponse) { sum += o.At })
	return sum
}

type decoded struct{ tenant, id string }

func decode(key, value []byte) (decoded, error) {
	if len(value) == 0 {
		return decoded{}, errors.New("empty")
	}
	return decoded{tenant: string(key), id: string(value)}, nil
}

// answer is a handler scripted per event id, Done unless told otherwise.
type answer struct {
	mu   sync.Mutex
	seen []string
	by   map[string]pipeline.Outcome
}

func (a *answer) Handle(_ context.Context, m decoded) (pipeline.Outcome, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.seen = append(a.seen, m.id)
	if o, ok := a.by[m.id]; ok {
		return o, errors.New("scripted")
	}
	return pipeline.Done, nil
}

func (a *answer) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.seen)
}

// assignments follows what each member currently holds, from the same slog
// lines the operator would read: an assignment adds, a revocation removes.
// Cooperative rebalancing tells a joining member "assigned nothing" first and
// hands it partitions in a second round, so the history is what matters, not
// the first line.
type assignments struct {
	mu      sync.Mutex
	holds   map[string]map[int32]bool
	revoked int
}

func (a *assignments) holding(member string) []int32 {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []int32
	for p := range a.holds[member] {
		out = append(out, p)
	}
	return out
}

func (a *assignments) logger(member string) *slog.Logger {
	return slog.New(&capture{member: member, a: a})
}

type capture struct {
	member string
	a      *assignments
	attrs  []slog.Attr
}

func (c *capture) Enabled(context.Context, slog.Level) bool { return true }
func (c *capture) WithGroup(string) slog.Handler            { return c }
func (c *capture) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &capture{member: c.member, a: c.a, attrs: append(append([]slog.Attr{}, c.attrs...), attrs...)}
}
func (c *capture) Handle(_ context.Context, r slog.Record) error {
	var parts []int32
	r.Attrs(func(at slog.Attr) bool {
		if at.Key == "partitions" {
			parts, _ = at.Value.Any().([]int32)
		}
		return true
	})
	c.a.mu.Lock()
	defer c.a.mu.Unlock()
	if c.a.holds == nil {
		c.a.holds = map[string]map[int32]bool{}
	}
	if c.a.holds[c.member] == nil {
		c.a.holds[c.member] = map[int32]bool{}
	}
	switch r.Message {
	case "partitions assigned":
		for _, p := range parts {
			c.a.holds[c.member][p] = true
		}
	case "partitions revoked":
		c.a.revoked++
		for _, p := range parts {
			delete(c.a.holds[c.member], p)
		}
	}
	return nil
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestTwoMembersSplitThePartitionsAndCommitOnlyByHand(t *testing.T) {
	brokers := cluster(t, 3)
	produce(t, brokers, 30)

	as := &assignments{}
	h := &answer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	start := func(name string) {
		c, err := New(Config{Brokers: brokers, Group: "core", Log: as.logger(name), Session: 6 * time.Second}, decode, h)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer c.Close()
			if err := c.Run(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	// The second member joins only once the first holds the whole topic, so
	// the rebalance is forced rather than folded into one initial join.
	start("a")
	waitFor(t, "a holds the whole topic", func() bool { return len(as.holding("a")) == 3 })
	start("b")

	waitFor(t, "30 records handled", func() bool { return h.count() >= 30 })
	waitFor(t, "30 offsets committed", func() bool { return committed(t, brokers, "core") == 30 })

	// The rebalance is done when the two members hold every partition once
	// between them and both hold something. Read before they leave: leaving
	// revokes everything, which is correct and not what this measures.
	split := func() bool {
		held := map[int32]int{}
		for _, member := range []string{"a", "b"} {
			parts := as.holding(member)
			if len(parts) == 0 {
				return false
			}
			for _, p := range parts {
				held[p]++
			}
		}
		for p := int32(0); p < 3; p++ {
			if held[p] != 1 {
				return false
			}
		}
		return true
	}
	waitFor(t, "the topic split between a and b", split)
	as.mu.Lock()
	revoked := as.revoked
	as.mu.Unlock()
	cancel()
	wg.Wait()

	if revoked == 0 {
		t.Error("no revocation was logged before the split, so a gave nothing up and b took nothing")
	}
	if h.count() != 30 {
		t.Errorf("handled %d records, want 30: a record was delivered twice or not at all", h.count())
	}
}

func TestFatalStopsWithoutCommittingItsOffset(t *testing.T) {
	brokers := cluster(t, 1)
	produce(t, brokers, 5)

	h := &answer{by: map[string]pipeline.Outcome{"event-2": pipeline.Fatal}}
	c, err := New(Config{Brokers: brokers, Group: "core", Log: slog.New(slog.DiscardHandler)}, decode, h)
	if err != nil {
		t.Fatal(err)
	}
	err = c.Run(context.Background())
	c.Close()
	if !errors.Is(err, ErrStopped) {
		t.Fatalf("Run returned %v, want a stop", err)
	}
	// The two Done records before the Fatal one are committed, the Fatal one
	// and everything after it are not: they come back to the next member.
	if got := committed(t, brokers, "core"); got != 2 {
		t.Fatalf("committed %d, want 2", got)
	}

	// A fresh member sees event-2 again. Nothing was lost by stopping.
	h2 := &answer{}
	c2, err := New(Config{Brokers: brokers, Group: "core", Log: slog.New(slog.DiscardHandler)}, decode, h2)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = c2.Run(ctx) }()
	waitFor(t, "redelivery", func() bool { return h2.count() >= 3 })
	cancel()
	c2.Close()
	if h2.seen[0] != "event-2" {
		t.Fatalf("first redelivered record is %s, want event-2", h2.seen[0])
	}
}

func TestRetryWithNoSinkStops(t *testing.T) {
	brokers := cluster(t, 1)
	produce(t, brokers, 2)
	h := &answer{by: map[string]pipeline.Outcome{"event-0": pipeline.Retry}}
	c, err := New(Config{Brokers: brokers, Group: "core", Log: slog.New(slog.DiscardHandler)}, decode, h)
	if err != nil {
		t.Fatal(err)
	}
	err = c.Run(context.Background())
	c.Close()
	if !errors.Is(err, ErrStopped) {
		t.Fatalf("Run returned %v, want a stop: a Retry with nowhere to go must not be dropped", err)
	}
	if got := committed(t, brokers, "core"); got != 0 {
		t.Fatalf("committed %d, want 0", got)
	}
}

// The seam of nine-docs/adr/0002: the handler has returned Done for the whole
// batch, the offsets are not yet committed, and the process dies here. The
// hook stands in for the kill. Nothing is committed and every record comes back.
func TestCommitHookRefusingLeavesTheBatchForTheNextMember(t *testing.T) {
	brokers := cluster(t, 1)
	produce(t, brokers, 4)
	h := &answer{}
	crash := func(context.Context, []*kgo.Record) error { return errors.New("killed between the two commits") }
	c, err := New(Config{Brokers: brokers, Group: "core", Log: slog.New(slog.DiscardHandler)}, decode, h, WithCommitHook[decoded](crash))
	if err != nil {
		t.Fatal(err)
	}
	err = c.Run(context.Background())
	c.Close()
	if !errors.Is(err, ErrStopped) {
		t.Fatalf("Run returned %v, want a stop", err)
	}
	if h.count() != 4 {
		t.Fatalf("handler saw %d records before the crash, want 4", h.count())
	}
	if got := committed(t, brokers, "core"); got != 0 {
		t.Fatalf("committed %d after the crash, want 0", got)
	}
	h2 := &answer{}
	c2, err := New(Config{Brokers: brokers, Group: "core", Log: slog.New(slog.DiscardHandler)}, decode, h2)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = c2.Run(ctx) }()
	waitFor(t, "all four redelivered", func() bool { return h2.count() >= 4 })
	cancel()
	c2.Close()
	waitFor(t, "four committed by the second member", func() bool { return committed(t, brokers, "core") == 4 })
}

func TestARecordThatDoesNotDecodeIsPoison(t *testing.T) {
	brokers := cluster(t, 1)
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.ProduceSync(context.Background(), &kgo.Record{Topic: topic, Key: []byte("t"), Value: nil}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	cl.Close()
	forwarded := make(chan pipeline.Outcome, 1)
	sink := sinkFunc(func(_ context.Context, _ *kgo.Record, o pipeline.Outcome) error { forwarded <- o; return nil })
	h := &answer{}
	c, err := New(Config{Brokers: brokers, Group: "core", Log: slog.New(slog.DiscardHandler)}, decode, h, WithSink[decoded](sink))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = c.Run(ctx) }()
	select {
	case o := <-forwarded:
		if o != pipeline.Poison {
			t.Fatalf("forwarded as %v, want Poison", o)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("nothing forwarded")
	}
	waitFor(t, "poison committed after forward", func() bool { return committed(t, brokers, "core") == 1 })
	cancel()
	c.Close()
	if h.count() != 0 {
		t.Fatal("the handler saw a record that did not decode")
	}
}

type sinkFunc func(context.Context, *kgo.Record, pipeline.Outcome) error

func (f sinkFunc) Forward(ctx context.Context, r *kgo.Record, o pipeline.Outcome) error {
	return f(ctx, r, o)
}

// What CO2.4 needs from the seam, in @MustafaKemalV's words on 0002: it is
// not enough that a test can stop the process between the database commit
// and the offset commit, the process has to tell the test it is standing
// there, or the test falls back to timing and a test that reads timing lies
// one day. The hook is that signal: the test blocks in it, and while blocked
// it reads the state of the window from the broker, not from a clock.
func TestTheSeamTellsTheTestWhereItStands(t *testing.T) {
	brokers := cluster(t, 1)
	produce(t, brokers, 3)
	h := &answer{}
	atSeam := make(chan struct{})
	release := make(chan struct{})
	hook := func(context.Context, []*kgo.Record) error {
		close(atSeam)
		<-release
		return nil
	}
	c, err := New(Config{Brokers: brokers, Group: "core", Log: slog.New(slog.DiscardHandler)}, decode, h, WithCommitHook[decoded](hook))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	select {
	case <-atSeam:
	case <-time.After(30 * time.Second):
		t.Fatal("the seam was never reached")
	}
	// Standing in the window: the handler has returned for every record, the
	// broker has been told nothing. This is the state CO2.4 kills the process in.
	if h.count() != 3 {
		t.Fatalf("handler returned for %d records, want 3", h.count())
	}
	if got := committed(t, brokers, "core"); got != 0 {
		t.Fatalf("committed %d while standing before the offset commit, want 0", got)
	}
	close(release)
	waitFor(t, "the commit after the seam", func() bool { return committed(t, brokers, "core") == 3 })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	c.Close()
}

// Found on review of #12 by @MustafaKemalV. A SIGTERM that lands while the
// handlers are inside a batch cancels the context the commit was about to use,
// so every handler finishes, answers Done, and the commit fails on
// "context canceled": nothing lost, but a whole batch redelivered and a non
// zero exit on every rolling restart that lands mid batch. 0002 says the
// offset commit failing while the process is alive is not a failure of the
// message. The commit has to outlive the cancellation.
func TestCancelInsideABatchStillCommitsWhatWasEarned(t *testing.T) {
	brokers := cluster(t, 1)
	produce(t, brokers, 3)
	inside := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	h := handlerFunc(func(context.Context, decoded) (pipeline.Outcome, error) {
		once.Do(func() { close(inside) })
		<-release
		return pipeline.Done, nil
	})
	c, err := New(Config{Brokers: brokers, Group: "core", Log: slog.New(slog.DiscardHandler)}, decode, h)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	<-inside
	cancel()
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v on a cancel that landed inside a batch, want nil", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return")
	}
	c.Close()
	if got := committed(t, brokers, "core"); got != 3 {
		t.Fatalf("committed %d after a cancel inside the batch, want 3: the handlers had finished", got)
	}
}

// Found on the same review. AllowRebalance was called on three explicit paths
// and none of them is a defer, so a panic in the decoder, the handler, the
// Sink or the hook unwinds past all three and Close blocks forever: the
// process that should have crashed hangs instead. The panic still propagates;
// what this asserts is that Close returns after it.
func TestAPanicInTheLoopDoesNotWedgeClose(t *testing.T) {
	brokers := cluster(t, 1)
	produce(t, brokers, 1)
	hook := func(context.Context, []*kgo.Record) error { panic("a bug in the hook") }
	c, err := New(Config{Brokers: brokers, Group: "core", Log: slog.New(slog.DiscardHandler)}, decode, &answer{}, WithCommitHook[decoded](hook))
	if err != nil {
		t.Fatal(err)
	}
	recovered := make(chan any, 1)
	go func() {
		defer func() { recovered <- recover() }()
		_ = c.Run(context.Background())
	}()
	select {
	case r := <-recovered:
		if r == nil {
			t.Fatal("the panic did not propagate out of Run")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run neither returned nor panicked")
	}
	closed := make(chan struct{})
	go func() { c.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return within 10s after a panic in the loop")
	}
}

type handlerFunc func(context.Context, decoded) (pipeline.Outcome, error)

func (f handlerFunc) Handle(ctx context.Context, m decoded) (pipeline.Outcome, error) {
	return f(ctx, m)
}

// A shutdown that lands while the handler is inside its transaction makes
// the handler fail on a cancelled context, and that failure is the
// shutdown's, not the record's. Found on the self review of CO2.2: the
// store's insert on a cancelled context classified as Fatal and Run
// returned an error on every restart that landed inside a handler.
func TestCancelInsideAHandlerIsAShutdownNotAFatal(t *testing.T) {
	brokers := cluster(t, 1)
	produce(t, brokers, 3)
	ctx, cancel := context.WithCancel(context.Background())
	var n int
	h := handlerFunc(func(ctx context.Context, m decoded) (pipeline.Outcome, error) {
		n++
		if n == 2 {
			cancel()
			<-ctx.Done()
			return pipeline.Fatal, ctx.Err() // what a store answers on a cancelled context
		}
		return pipeline.Done, nil
	})
	c, err := New(Config{Brokers: brokers, Group: "core", Log: slog.New(slog.DiscardHandler)}, decode, h)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Run(ctx); err != nil {
		t.Fatalf("Run returned %v on a cancel inside a handler, want nil", err)
	}
	c.Close()
	if got := committed(t, brokers, "core"); got != 1 {
		t.Fatalf("committed %d, want 1: the record before the cancel was earned, the cancelled one was not", got)
	}
}

// CO4.2. A record on a delay topic is not handed over before it has sat there
// for the delay. The shipped delays are five minutes and one hour, from
// nine-docs/adr/0001, and a test that waited either would not be a test. This
// bounds the mechanism at three seconds and the shipped numbers stay unproven,
// which is the trade Kemal named on nine-billing#32 and it is the same one.
func TestADelayTopicIsNotReadBeforeItsTime(t *testing.T) {
	brokers := cluster(t, 1)
	produce(t, brokers, 1)
	const delay = 3 * time.Second

	h := &answer{}
	c, err := New(Config{Brokers: brokers, Group: "core", Log: slog.New(slog.DiscardHandler), Delay: delay}, decode, h)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	produced := time.Now()
	go func() { _ = c.Run(ctx) }()

	// Not before its time. A second is a third of the delay: if the gate were
	// missing the record would already be handled, which is what the run
	// without a delay below shows.
	time.Sleep(time.Second)
	if n := h.count(); n != 0 {
		t.Fatalf("%d records handled after 1s of a %s delay", n, delay)
	}
	if got := committed(t, brokers, "core"); got != 0 {
		t.Fatalf("committed %d while holding the record, want 0", got)
	}

	waitFor(t, "the record after it ripens", func() bool { return h.count() == 1 })
	waited := time.Since(produced)
	if waited < delay {
		t.Fatalf("handled after %s, which is less than the %s delay", waited, delay)
	}
	waitFor(t, "the offset after the record", func() bool { return committed(t, brokers, "core") == 1 })
	cancel()
	c.Close()
}

// The same topic with no delay: the record is handled at once. Without this,
// the test above would pass on a consumer that had simply stopped working.
func TestWithNoDelayTheSameRecordIsHandledAtOnce(t *testing.T) {
	brokers := cluster(t, 1)
	produce(t, brokers, 1)
	h := &answer{}
	c, err := New(Config{Brokers: brokers, Group: "core", Log: slog.New(slog.DiscardHandler)}, decode, h)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()
	waitFor(t, "the record", func() bool { return h.count() == 1 })
	cancel()
	c.Close()
}

// Order survives the wait. A held partition is taken up again at exactly the
// record it stopped at, so nothing is skipped and nothing arrives twice, and
// the later records on that partition are not read past the held one.
func TestAHeldPartitionResumesAtTheRecordItStoppedAt(t *testing.T) {
	brokers := cluster(t, 1)
	produce(t, brokers, 5)
	h := &answer{}
	c, err := New(Config{Brokers: brokers, Group: "core", Log: slog.New(slog.DiscardHandler), Delay: 2 * time.Second}, decode, h)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()
	waitFor(t, "all five, once they ripen", func() bool { return h.count() >= 5 })
	cancel()
	c.Close()

	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.seen) != 5 {
		t.Fatalf("handled %d records, want 5 exactly: %v", len(h.seen), h.seen)
	}
	for i, id := range h.seen {
		if want := fmt.Sprintf("event-%d", i); id != want {
			t.Fatalf("record %d is %s, want %s: the resume did not land where the hold stopped", i, id, want)
		}
	}
	if got := committed(t, brokers, "core"); got != 5 {
		t.Fatalf("committed %d, want 5", got)
	}
}

// The clock is the record's own timestamp, so a record that has already sat
// on the topic for longer than the delay is ripe on arrival and waits for
// nothing. Measured by producing with a timestamp in the past, which is what
// a consumer restarting after an outage reads.
func TestARecordOlderThanTheDelayIsRipeOnArrival(t *testing.T) {
	brokers := cluster(t, 1)
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	old := &kgo.Record{Topic: topic, Key: []byte("t"), Value: []byte("event-old"),
		Timestamp: time.Now().Add(-time.Hour)}
	if err := cl.ProduceSync(context.Background(), old).FirstErr(); err != nil {
		t.Fatal(err)
	}
	cl.Close()

	h := &answer{}
	c, err := New(Config{Brokers: brokers, Group: "core", Log: slog.New(slog.DiscardHandler), Delay: 30 * time.Second}, decode, h)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	go func() { _ = c.Run(ctx) }()
	waitFor(t, "the old record", func() bool { return h.count() == 1 })
	if took := time.Since(start); took > 10*time.Second {
		t.Fatalf("an hour old record waited %s behind a 30s delay", took)
	}
	cancel()
	c.Close()
}

// The reason the wait is a pause and a seek rather than a sleep. A consumer
// sleeping out a five minute delay inside its batch holds the partition for
// five minutes, and with BlockRebalanceOnPoll it holds every rebalance too, so
// a deploy waits behind a retry topic. Here the delay is a minute and the
// second member has to arrive in seconds.
func TestAHeldPartitionDoesNotHoldTheRebalance(t *testing.T) {
	brokers := cluster(t, 3)
	produce(t, brokers, 30)
	as := &assignments{}
	h := &answer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	start := func(name string) {
		c, err := New(Config{Brokers: brokers, Group: "core", Log: as.logger(name),
			Session: 6 * time.Second, Delay: time.Minute}, decode, h)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer c.Close()
			if err := c.Run(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	start("a")
	waitFor(t, "a holds the whole topic", func() bool { return len(as.holding("a")) == 3 })
	// Every partition is now paused behind a minute of delay. Nothing is
	// being handled and nothing will be for a minute.
	if n := h.count(); n != 0 {
		t.Fatalf("%d records handled behind a one minute delay", n)
	}

	joined := time.Now()
	start("b")
	waitFor(t, "the topic split while every partition is held", func() bool {
		return len(as.holding("a")) > 0 && len(as.holding("b")) > 0 &&
			len(as.holding("a"))+len(as.holding("b")) == 3
	})
	if took := time.Since(joined); took > 30*time.Second {
		t.Fatalf("the rebalance took %s behind a one minute delay, which means the wait held it", took)
	}
	if n := h.count(); n != 0 {
		t.Fatalf("%d records were handled early, and the delay is the point", n)
	}
	cancel()
	wg.Wait()
}
