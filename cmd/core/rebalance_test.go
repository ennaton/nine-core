package main

import (
	"bufio"
	"context"
	"io"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// CO2.5. A message in flight while a partition changes hands.
//
// The window is the same one CO2.4 kills in: the handlers have written and the
// offsets are not committed. Here the first member stands in it, on purpose,
// while a second member joins and asks for a share of the partitions. The
// question the row asks is whether that costs anything: an event processed
// twice, or an event nobody ends up owning.
//
// The seam's pause mode is what makes the moment reachable. The consumer
// announces the window on stdout and blocks on stdin, so the test knows where
// it stands without a clock, and the second member's arrival is read from the
// broker's own view of the group rather than assumed after a sleep.
//
// BlockRebalanceOnPoll is the mechanism under test: a partition may not be
// taken from a member that is still inside a batch it has written.

const eventsInRebalance = 8

func TestAPartitionThatMovesMidWindowCostsNothing(t *testing.T) {
	owner := stackDSN(t)
	brokers := strings.Split(envOr("NINE_KAFKA_BROKERS", "localhost:19092"), ",")
	ctx := context.Background()

	db, appDSN := scratchDatabase(t, owner)
	topic, group := scratchTopic(t, brokers, 2)
	produce(t, brokers, topic, eventsInRebalance)

	bin := build(t, true) // the tagged binary: only it can stand in the window

	// The first member, held in the window by the pause mode.
	first := exec.Command(bin)
	first.Env = childEnv(appDSN, brokers, topic, group, "pause")
	stdin, err := first.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := first.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	first.Stderr = io.Discard
	if err := first.Start(); err != nil {
		t.Fatalf("start the first member: %v", err)
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.Wait() }()

	waitForLine(t, stdout, "nine-fault-reached: AFTER_DB_COMMIT")
	held, member := assignment(t, brokers, group)
	t.Logf("at the seam the only member holds %d partitions", held)
	if held != 2 {
		t.Fatalf("the first member holds %d partitions at the seam, want 2", held)
	}
	rows := countRows(t, ctx, db)
	committed := committedOffset(t, brokers, topic, group)
	t.Logf("first member is in the window: rows=%d committed=%d", rows, committed)
	if rows == 0 {
		t.Fatal("no rows at the seam: the window is after the write, so the writes should be there")
	}
	if committed != 0 {
		t.Fatalf("committed=%d at the seam, want 0: the window is before the offset commit", committed)
	}

	// The second member joins while the first one stands there. Its arrival is
	// read from the group, not waited out.
	second := exec.Command(bin)
	second.Env = childEnv(appDSN, brokers, topic, group, "")
	second.Stdin = nil
	second.Stdout, second.Stderr = io.Discard, io.Discard
	if err := second.Start(); err != nil {
		t.Fatalf("start the second member: %v", err)
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.Wait() }()
	waitForMembers(t, brokers, group, 2)

	// The mechanism, and the reason this test exists rather than the outcome,
	// which the idempotent insert would give either way: a partition may not be
	// taken from a member that is standing inside a batch it has written.
	// BlockRebalanceOnPoll holds the rebalance until the poll returns, so the
	// first member still holds everything it held while the second one waits.
	// What the block actually does, and it is not what it sounds like: the
	// coordinator has the second member's request and cannot finish, because the
	// first has not rejoined. It is inside the poll, and the poll is inside the
	// batch it has already written. So the group is stuck between generations,
	// and the assignments read as nothing on both sides rather than as a handover.
	if state := describe(t, brokers, group)[group].State; state == "Stable" {
		if still, _ := assignmentOf(t, brokers, group, member); still < held {
			t.Fatalf("the group is Stable and the first member is down to %d of %d partitions while it stands inside the batch: the rebalance did not wait for it", still, held)
		}
		t.Fatalf("the group reached Stable while the first member was held inside a written batch")
	} else {
		t.Logf("the group is %s while the first member is held, which is the rebalance waiting on it", state)
	}

	// Nothing moved while the first member was inside the batch: the offsets it
	// has not committed are still uncommitted, and it is still alive.
	if got := committedOffset(t, brokers, topic, group); got != committed {
		t.Fatalf("committed went from %d to %d while the first member was in the window", committed, got)
	}
	select {
	case err := <-firstDone:
		t.Fatalf("the first member left while it was supposed to be held: %v", err)
	default:
	}

	// Release it. Now the rebalance may proceed and both members drain.
	if _, err := io.WriteString(stdin, "go\n"); err != nil {
		t.Fatalf("release the first member: %v", err)
	}

	waitForCommitted(t, brokers, topic, group, eventsInRebalance)
	_ = first.Process.Signal(syscall.SIGTERM)
	_ = second.Process.Signal(syscall.SIGTERM)
	<-firstDone
	<-secondDone

	rows = countRows(t, ctx, db)
	committed = committedOffset(t, brokers, topic, group)
	t.Logf("after the handover: rows=%d committed=%d", rows, committed)
	if rows != eventsInRebalance {
		t.Fatalf("rows = %d, want %d: a message in flight was lost or processed twice", rows, eventsInRebalance)
	}
	if committed != eventsInRebalance {
		t.Fatalf("committed = %d, want %d", committed, eventsInRebalance)
	}
}

// waitForLine reads until the consumer announces the seam. It is a blocking
// read: the test learns where the process stands from the process, not from a
// timer, and a process that never gets there fails the test rather than passing
// it late.
func waitForLine(t *testing.T, r io.Reader, want string) {
	t.Helper()
	found := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			if strings.Contains(sc.Text(), want) {
				found <- sc.Text()
				return
			}
		}
		found <- ""
	}()
	select {
	case line := <-found:
		if line == "" {
			t.Fatalf("the consumer never printed %q", want)
		}
	case <-time.After(90 * time.Second):
		t.Fatalf("no %q in 90s", want)
	}
}

// assignment returns how many partitions the group's single member holds, and
// its member id, read from the broker's own view.
func assignment(t *testing.T, brokers []string, group string) (int, string) {
	t.Helper()
	gs := describe(t, brokers, group)
	g := gs[group]
	if len(g.Members) != 1 {
		t.Fatalf("the group holds %d members, want 1 at the seam", len(g.Members))
	}
	m := g.Members[0]
	a, _ := m.Assigned.AsConsumer()
	n := 0
	for _, t := range a.Topics {
		n += len(t.Partitions)
	}
	return n, m.MemberID
}

// assignmentOf returns how many partitions one named member holds now.
func assignmentOf(t *testing.T, brokers []string, group, member string) (int, string) {
	t.Helper()
	gs := describe(t, brokers, group)
	for _, m := range gs[group].Members {
		if m.MemberID != member {
			continue
		}
		a, _ := m.Assigned.AsConsumer()
		n := 0
		for _, tp := range a.Topics {
			n += len(tp.Partitions)
		}
		return n, "still a member"
	}
	return 0, "no longer a member of the group"
}

func describe(t *testing.T, brokers []string, group string) kadm.DescribedGroups {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	gs, err := kadm.NewClient(cl).DescribeGroups(context.Background(), group)
	if err != nil {
		t.Fatalf("describe group: %v", err)
	}
	return gs
}

func waitForMembers(t *testing.T, brokers []string, group string, n int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	deadline := time.After(90 * time.Second)
	for {
		gs, err := adm.DescribeGroups(context.Background(), group)
		if err == nil {
			if g, ok := gs[group]; ok && len(g.Members) >= n {
				t.Logf("the group holds %d members", len(g.Members))
				return
			}
		}
		select {
		case <-deadline:
			t.Fatalf("the group did not reach %d members in 90s", n)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func waitForCommitted(t *testing.T, brokers []string, topic, group string, want int64) {
	t.Helper()
	deadline := time.After(120 * time.Second)
	for {
		if got := committedOffset(t, brokers, topic, group); got >= want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("the group did not commit %d in 120s (at %d)", want, committedOffset(t, brokers, topic, group))
		case <-time.After(200 * time.Millisecond):
		}
	}
}
