package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ennaton/nine-core/internal/store"
)

// CO2.4. The window nine-docs/adr/0002 decided: the database transaction has
// committed and the offset has not. The process is killed there, restarted, and
// the ledger of events must hold one row per event rather than two.
//
// The window is forced rather than waited for, through the fault point of
// docs/artifacts/2026-09-08-the-seam-the-crash-tests-need.md: the tagged binary
// exits 97 inside the commit hook, which runs after every handler in the batch
// has returned and before CommitRecords. No clock decides anything here: the
// test waits on the process, and reads both sides of the window from Postgres
// and from the broker rather than from the consumer.
//
// It needs the compose stack, the same one internal/store's tests need.
// NINE_TEST_MIGRATE_DSN is the owner, NINE_KAFKA_BROKERS the broker.

const eventsInTest = 5

func TestACrashBetweenTheTwoCommitsLeavesNoDuplicate(t *testing.T) {
	owner := stackDSN(t)
	brokers := strings.Split(envOr("NINE_KAFKA_BROKERS", "localhost:19092"), ",")
	ctx := context.Background()

	db, appDSN := scratchDatabase(t, owner)
	topic, group := scratchTopic(t, brokers)
	produce(t, brokers, topic, eventsInTest)

	plain := build(t, false)
	faulty := build(t, true)

	// The crash. Exit 97 is the fault point and nothing else: any other code
	// would mean the process left for a reason this test did not arrange.
	code := runBinary(t, faulty, childEnv(appDSN, brokers, topic, group, "exit"), 0)
	if code != 97 {
		t.Fatalf("the tagged binary left with %d, want 97 from the fault point", code)
	}

	rows := countRows(t, ctx, db)
	committed := committedOffset(t, brokers, topic, group)
	t.Logf("after the crash: rows=%d committed=%d", rows, committed)
	if rows != eventsInTest {
		t.Fatalf("rows after the crash = %d, want %d: the handlers had returned, so the writes are durable", rows, eventsInTest)
	}
	if committed != 0 {
		t.Fatalf("committed offsets after the crash = %d, want 0: the crash is before the offset commit", committed)
	}

	// The restart. The same records are delivered again, because nothing said
	// they were read.
	runBinary(t, plain, childEnv(appDSN, brokers, topic, group, ""), eventsInTest)

	rows = countRows(t, ctx, db)
	committed = committedOffset(t, brokers, topic, group)
	t.Logf("after the restart: rows=%d committed=%d", rows, committed)
	if rows != eventsInTest {
		t.Fatalf("rows after the restart = %d, want %d: the redelivery was written a second time", rows, eventsInTest)
	}
	if committed != eventsInTest {
		t.Fatalf("committed offsets after the restart = %d, want %d", committed, eventsInTest)
	}
}

func stackDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("NINE_TEST_MIGRATE_DSN")
	if dsn != "" {
		return dsn
	}
	// The same rule internal/store follows: a skip is right on a laptop with
	// no stack and wrong in CI, where it reads exactly like a pass.
	if os.Getenv("CI") != "" {
		t.Fatal("NINE_TEST_MIGRATE_DSN is unset in CI: this test would be skipped and the crash window would go unproven")
	}
	t.Skip("NINE_TEST_MIGRATE_DSN unset: no stack to crash against")
	return ""
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// scratchDatabase migrates a database of its own and returns a connection to it
// plus the dsn the consumer uses, which is nine_app rather than the owner.
func scratchDatabase(t *testing.T, owner string) (*pgx.Conn, string) {
	t.Helper()
	ctx := context.Background()
	name := fmt.Sprintf("nine_core_co24_%d", time.Now().UnixNano())

	admin, err := pgx.Connect(ctx, owner)
	if err != nil {
		t.Fatalf("connect as owner: %v", err)
	}
	// The consumer connects as nine_app, which the compose stack creates in
	// init.sql and a bare service container does not. Creating it here rather
	// than requiring it keeps the test the same test in both places.
	var exists bool
	if err := admin.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'nine_app')").Scan(&exists); err != nil {
		t.Fatalf("look for nine_app: %v", err)
	}
	if !exists {
		if _, err := admin.Exec(ctx, "CREATE ROLE nine_app LOGIN PASSWORD 'nine_app_dev'"); err != nil { // nine:allow-secret, a throwaway role in a throwaway database
			t.Fatalf("create nine_app: %v", err)
		}
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		_ = admin.Close(context.Background())
	})

	ownerHere := swapDatabase(owner, name)
	if err := store.Migrate(ctx, ownerHere); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	conn, err := pgx.Connect(ctx, ownerHere)
	if err != nil {
		t.Fatalf("connect to the scratch database: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	return conn, swapUser(swapDatabase(owner, name), "nine_app", "nine_app_dev") // nine:allow-secret, the compose dev stack
}

func swapDatabase(dsn, name string) string {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		panic(err)
	}
	cfg.Database = name
	return dsnOf(cfg)
}

func swapUser(dsn, user, pass string) string {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		panic(err)
	}
	cfg.User, cfg.Password = user, pass
	return dsnOf(cfg)
}

func dsnOf(cfg *pgx.ConnConfig) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
}

func scratchTopic(t *testing.T, brokers []string) (string, string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	name := fmt.Sprintf("co24-%d", time.Now().UnixNano())
	if _, err := adm.CreateTopic(context.Background(), 1, 1, nil, name); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	t.Cleanup(func() { _, _ = adm.DeleteTopics(context.Background(), name) })
	return name, name + "-group"
}

func produce(t *testing.T, brokers []string, topic string, n int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(topic))
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer cl.Close()
	for i := 0; i < n; i++ {
		body, err := json.Marshal(map[string]any{
			"event_id":    fmt.Sprintf("co24-run-%06d", i),
			"agent":       "claude-code",
			"occurred_at": "2026-09-08T12:00:00Z",
			"duration_ms": 10,
			"outcome":     "success",
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := cl.ProduceSync(context.Background(),
			&kgo.Record{Key: []byte("tenant-a"), Value: body}).FirstErr(); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}
}

func build(t *testing.T, tagged bool) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "core")
	args := []string{"build", "-o", out}
	if tagged {
		args = append(args, "-tags", "faultinject")
	}
	args = append(args, ".")
	cmd := exec.Command("go", args...)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, b)
	}
	return out
}

func childEnv(dsn string, brokers []string, topic, group, fault string) []string {
	e := append(os.Environ(),
		"NINE_CORE_DSN="+dsn,
		"NINE_KAFKA_BROKERS="+strings.Join(brokers, ","),
		"NINE_TOPIC="+topic,
		"NINE_CONSUMER_GROUP="+group,
	)
	if fault != "" {
		e = append(e, "NINE_FAULT_AFTER_DB_COMMIT="+fault)
	}
	return e
}

// runBinary runs core and returns its exit code. With untilCommitted above
// zero it waits for the group to reach that offset and then asks the process to
// stop, which is what a restart looks like from outside.
func runBinary(t *testing.T, bin string, environ []string, untilCommitted int64) int {
	t.Helper()
	cmd := exec.Command(bin)
	cmd.Env = environ
	cmd.Stdin = nil
	var out strings.Builder
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", bin, err)
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	if untilCommitted > 0 {
		brokers, topic, group := fromEnv(environ)
		deadline := time.After(60 * time.Second)
		for {
			if committedOffset(t, brokers, topic, group) >= untilCommitted {
				_ = cmd.Process.Signal(syscall.SIGTERM)
				break
			}
			select {
			case err := <-done:
				t.Fatalf("the binary left before committing: %v\n%s", err, out.String())
			case <-deadline:
				t.Fatalf("the group did not reach offset %d in 60s\n%s", untilCommitted, out.String())
			case <-time.After(200 * time.Millisecond):
			}
		}
	}

	err := <-done
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("wait: %v\n%s", err, out.String())
	}
	t.Logf("binary %s left with %d", filepath.Base(bin), code)
	return code
}

func fromEnv(environ []string) ([]string, string, string) {
	var brokers []string
	var topic, group string
	for _, kv := range environ {
		switch {
		case strings.HasPrefix(kv, "NINE_KAFKA_BROKERS="):
			brokers = strings.Split(strings.TrimPrefix(kv, "NINE_KAFKA_BROKERS="), ",")
		case strings.HasPrefix(kv, "NINE_TOPIC="):
			topic = strings.TrimPrefix(kv, "NINE_TOPIC=")
		case strings.HasPrefix(kv, "NINE_CONSUMER_GROUP="):
			group = strings.TrimPrefix(kv, "NINE_CONSUMER_GROUP=")
		}
	}
	return brokers, topic, group
}

func countRows(t *testing.T, ctx context.Context, db *pgx.Conn) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRow(ctx, "SELECT count(*) FROM events").Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	return n
}

// committedOffset asks the broker, not the consumer. Both sides of the window
// are read from the systems that hold them.
func committedOffset(t *testing.T, brokers []string, topic, group string) int64 {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer cl.Close()
	resp, err := kadm.NewClient(cl).FetchOffsets(context.Background(), group)
	if err != nil {
		t.Fatalf("fetch offsets: %v", err)
	}
	var total int64
	resp.Each(func(o kadm.OffsetResponse) {
		if o.Topic == topic && o.Err == nil {
			total += o.At
		}
	})
	return total
}
