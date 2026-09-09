// Package consumer reads a topic as a member of a consumer group and hands
// every record to a pipeline.Handler, committing offsets by hand and only for
// the records it has accounted for: a Done from the handler, or a Retry or
// Poison the Sink has acknowledged. A record it cannot account for stops the
// consumer with that record and everything after it uncommitted.
//
// The order is fixed by nine-docs/adr/0002: whatever the handler does with a
// message, its transaction commits first, and the offset for that message is
// committed after the handler has returned. Between the two sits CommitHook,
// so a test can stop the process in exactly that window (CO2.4) rather than
// hoping to land in it.
//
// What this package does not do. It does not write to the database, which is
// the handler's job (CO2.2). It does not produce to the retry, parked or dead
// letter topics: a Retry or Poison outcome is handed to a Sink, which CO4
// implements, and until a Sink is wired those two outcomes stop the consumer
// rather than losing the message. Refuse, never guess: the rule outcome.go
// states for Unknown holds here for an outcome nobody has given a home.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ennaton/nine-core/internal/pipeline"
)

// Decoder turns one record into the handler's message type. The key travels
// separately because ingest puts the tenant there and nowhere in the value.
// A decoder that returns an error makes the record Poison: nothing about
// waiting changes bytes that do not parse.
type Decoder[T any] func(key, value []byte) (T, error)

// Sink is where a record goes when the handler answers Retry or Poison. CO4
// implements it as a producer to the next delay topic, events.parked, or
// events.dlq. Forward returns only after the destination has acknowledged,
// because the source offset is committed right after it returns and a message
// that was neither written nor forwarded is a message that was lost.
type Sink interface {
	Forward(ctx context.Context, r *kgo.Record, o pipeline.Outcome) error
}

// CommitHook runs after the handler has returned for every record in a batch
// and before their offsets are committed. This is the crash window of
// nine-docs/adr/0002, made into a place a test can stand. A hook that returns
// an error leaves the batch uncommitted and stops the consumer; the records
// come back on the next start, which is the whole point.
type CommitHook func(ctx context.Context, acked []*kgo.Record) error

// Config is what a consumer needs to join. Brokers and Group are required;
// the rest have defaults a dev stack is happy with.
type Config struct {
	Brokers []string
	Group   string
	Topic   string        // default "events"
	MaxPoll int           // records per poll, default 100; bounds how long a rebalance waits
	Log     *slog.Logger  // default slog.Default()
	Session time.Duration // default 10s; kept small so a test sees a rebalance in seconds
	// CommitTimeout bounds the offset commit, which runs on a context that
	// outlives the caller's: a shutdown that lands inside a batch must not
	// turn earned offsets into a failed commit. Default 10s.
	CommitTimeout time.Duration
}

func (c Config) withDefaults() Config {
	if c.Topic == "" {
		c.Topic = "events"
	}
	if c.MaxPoll <= 0 {
		c.MaxPoll = 100
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
	if c.Session <= 0 {
		c.Session = 10 * time.Second
	}
	if c.CommitTimeout <= 0 {
		c.CommitTimeout = 10 * time.Second
	}
	return c
}

type Consumer[T any] struct {
	cl      *kgo.Client
	cfg     Config
	decode  Decoder[T]
	handler pipeline.Handler[T]
	sink    Sink
	hook    CommitHook
}

type Option[T any] func(*Consumer[T])

// WithSink gives Retry and Poison somewhere to go. Without it they stop the
// consumer, see the package comment.
func WithSink[T any](s Sink) Option[T] { return func(c *Consumer[T]) { c.sink = s } }

// WithCommitHook installs the seam between the handler and the offset commit.
func WithCommitHook[T any](h CommitHook) Option[T] { return func(c *Consumer[T]) { c.hook = h } }

// New joins the group. It returns before any partition is assigned; Run does
// the reading. Rebalances are logged from the client's own callbacks, because
// "the rebalance shows in the log" is CO2.1's acceptance criterion and a log
// line written from the loop would only show what the loop believed.
func New[T any](cfg Config, decode Decoder[T], h pipeline.Handler[T], opts ...Option[T]) (*Consumer[T], error) {
	if len(cfg.Brokers) == 0 || cfg.Group == "" {
		return nil, errors.New("consumer: brokers and group are required")
	}
	cfg = cfg.withDefaults()
	c := &Consumer[T]{cfg: cfg, decode: decode, handler: h}
	for _, o := range opts {
		o(c)
	}
	log := cfg.Log.With("group", cfg.Group, "topic", cfg.Topic)
	// The cooperative protocol calls these with nothing in them on the rounds
	// where a member keeps what it has, and a line saying "revoked: null" is
	// noise an operator has to learn to skip. Only a change is logged.
	on := func(level slog.Level, msg string) func(context.Context, *kgo.Client, map[string][]int32) {
		return func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			if parts := m[cfg.Topic]; len(parts) > 0 {
				log.Log(context.Background(), level, msg, "partitions", parts)
			}
		}
	}
	assigned := on(slog.LevelInfo, "partitions assigned")
	revoked := on(slog.LevelInfo, "partitions revoked")
	lost := on(slog.LevelWarn, "partitions lost")
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		// Offsets move only when this package says so. That is the row.
		kgo.DisableAutoCommit(),
		// A partition cannot be taken away in the middle of a batch: the
		// rebalance waits until the batch is committed and AllowRebalance is
		// called. The cost is bounded by MaxPoll records of handler time.
		kgo.BlockRebalanceOnPoll(),
		kgo.SessionTimeout(cfg.Session),
		kgo.OnPartitionsAssigned(assigned),
		kgo.OnPartitionsRevoked(revoked),
		kgo.OnPartitionsLost(lost),
	)
	if err != nil {
		return nil, fmt.Errorf("consumer: %w", err)
	}
	c.cl = cl
	c.cfg.Log = log
	return c, nil
}

// Close leaves the group. Anything uncommitted is redelivered to whoever gets
// the partition next, which is the safe direction.
func (c *Consumer[T]) Close() { c.cl.Close() }

// ErrStopped is wrapped by every error Run returns for a reason of its own,
// as opposed to the context ending.
var ErrStopped = errors.New("consumer stopped")

// Run polls until ctx ends, and returns nil then, after committing whatever
// the batch in flight had earned: a cancellation that lands while handlers
// are running is a shutdown, not a failure of the records they finished. It
// returns an error wrapping ErrStopped when a record makes it refuse to
// continue: a Fatal or Unknown outcome, a Retry or Poison with no Sink, a Sink
// or hook that failed, or an offset commit that failed. In every one of those
// the offsets from that record on stay where they were, so the records come
// back.
//
// One batch, in order: every record on every fetched partition goes through
// decode and the handler; the accounted ones are collected. Then the hook,
// then one synchronous commit of the collected records, then the rebalance is
// allowed. Time O(n) in records per batch, memory O(n) for the
// acknowledgements.
func (c *Consumer[T]) Run(ctx context.Context) error {
	for {
		fetches := c.cl.PollRecords(ctx, c.cfg.MaxPoll)
		if done, err := c.batch(ctx, fetches); done {
			return err
		}
	}
}

// batch is one poll's worth of work, in its own function so that the one
// thing every path must do is a defer rather than a call on each path: every
// PollRecords blocks the next rebalance until AllowRebalance, and Close waits
// for it. Three explicit calls covered three paths and a panic in the decoder,
// the handler, the Sink or the hook unwound past all of them, which turned a
// process that should have crashed into one that hung in Close.
func (c *Consumer[T]) batch(ctx context.Context, fetches kgo.Fetches) (done bool, err error) {
	defer c.cl.AllowRebalance()
	if fetches.IsClientClosed() {
		return true, nil
	}
	acked, stop := c.process(ctx, fetches)
	if err := c.commit(acked); err != nil {
		return true, err
	}
	if stop != nil {
		return true, stop
	}
	// Checked after the commit on purpose: a cancellation that arrived while
	// the handlers were inside the batch has already been honoured by the
	// handlers themselves. What they finished is committed above, and then
	// the loop ends. Records the poll returned that were not reached stay
	// uncommitted and come back to the next member.
	return ctx.Err() != nil, nil
}

// process runs the handler over one batch and returns the records to commit,
// plus the reason to stop if there is one. It stops at the first record it
// cannot account for and leaves the rest of the batch untouched: those offsets
// are not committed, so the records are redelivered.
func (c *Consumer[T]) process(ctx context.Context, fetches kgo.Fetches) (acked []*kgo.Record, stop error) {
	fetches.EachError(func(t string, p int32, err error) {
		// A cancelled poll reports itself as a fetch error on partition -1.
		// That is the shutdown the caller asked for, not a broker problem,
		// and a WARN on every clean stop is a line an operator learns to
		// ignore, which is the wrong lesson to teach about this log.
		if errors.Is(err, context.Canceled) {
			return
		}
		c.cfg.Log.Warn("fetch error", "partition", p, "err", err)
	})
	iter := fetches.RecordIter()
	for !iter.Done() {
		r := iter.Next()
		if err := c.one(ctx, r); err != nil {
			return acked, err
		}
		acked = append(acked, r)
	}
	return acked, nil
}

// one takes a record to an outcome and returns nil when its offset may be
// committed. Every non nil error is a reason to stop; none of them loses the
// record, because the offset is not committed.
func (c *Consumer[T]) one(ctx context.Context, r *kgo.Record) error {
	msg, err := c.decode(r.Key, r.Value)
	outcome := pipeline.Poison
	if err == nil {
		outcome, err = c.handler.Handle(ctx, msg)
	}
	log := c.cfg.Log.With("partition", r.Partition, "offset", r.Offset)
	switch outcome {
	case pipeline.Done:
		return nil
	case pipeline.Retry, pipeline.Poison:
		if c.sink == nil {
			return fmt.Errorf("%w: %v with no sink to forward to, partition %d offset %d", ErrStopped, outcome, r.Partition, r.Offset)
		}
		log.Info("forwarding", "outcome", outcome, "err", err)
		if ferr := c.sink.Forward(ctx, r, outcome); ferr != nil {
			return fmt.Errorf("%w: forward failed, partition %d offset %d: %v", ErrStopped, r.Partition, r.Offset, ferr)
		}
		return nil
	case pipeline.Fatal:
		return fmt.Errorf("%w: fatal at partition %d offset %d: %v", ErrStopped, r.Partition, r.Offset, err)
	default:
		return fmt.Errorf("%w: handler answered Unknown at partition %d offset %d", ErrStopped, r.Partition, r.Offset)
	}
}

// commit runs the hook, then commits the highest acknowledged offset per
// partition in one synchronous request. Nothing is committed if the hook
// refuses, and a commit that fails is a reason to stop rather than to carry
// on: advancing past an uncommitted write is the one thing 0002 forbids.
//
// It runs on its own context, bounded by CommitTimeout, and not on the
// caller's. The caller's context is cancelled by a shutdown, and a shutdown
// that lands while handlers are inside the batch would otherwise cancel the
// commit of work those handlers have finished: measured, three handlers
// answered Done and zero offsets were committed. Nothing was lost, but every
// rolling restart that landed mid batch redelivered a batch and exited non
// zero, which 0002 names as the one commit failure that is not the
// message's fault.
func (c *Consumer[T]) commit(acked []*kgo.Record) error {
	if len(acked) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.CommitTimeout)
	defer cancel()
	if c.hook != nil {
		if err := c.hook(ctx, acked); err != nil {
			return fmt.Errorf("%w: commit hook refused: %v", ErrStopped, err)
		}
	}
	if err := c.cl.CommitRecords(ctx, acked...); err != nil {
		return fmt.Errorf("%w: offset commit failed: %v", ErrStopped, err)
	}
	return nil
}
