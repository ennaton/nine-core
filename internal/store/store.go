// Package store is the events table: its migrations, the idempotent insert,
// and the mapping from what Postgres said to what the consumer does next.
//
// The insert is the load bearing part of nine-docs/adr/0002. The database
// commits first and the offset second, so a redelivered message must write
// nothing and raise nothing: ON CONFLICT DO NOTHING on the three column key
// the partitioned table forces, at read committed, where a concurrent insert
// of the same key is a quiet zero rows and not a serialization failure.
package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver goose runs on
	"github.com/pressly/goose/v3"

	"github.com/ennaton/nine-core/internal/event"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrate applies every migration in migrations/ to the database at dsn, as
// whoever dsn connects as. That is the owner, never nine_app: the app role
// inserts and reads, it does not own tables, which is what keeps row level
// security honest in the repositories that use it.
func Migrate(ctx context.Context, dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	defer db.Close()
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrationsFS())
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	if _, err := p.Up(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}

// Store is a pool against nine_core, connected as the application role.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects and pings, so a wrong dsn fails at startup and not on the
// first message.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// ErrNotInserted is not an error the caller acts on; it is how Insert says
// "already recorded, by whoever won the race" without a second return value
// that every caller would have to remember to read.
var ErrNotInserted = errors.New("already recorded")

const insertSQL = `
INSERT INTO events (
    tenant_id, event_id, occurred_at, agent, agent_version, session_id, repo_hash,
    duration_ms, outcome, error_kind, model, tokens_in, tokens_out, cost_micros,
    files_touched, lines_added, lines_removed, tool_calls
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
ON CONFLICT (tenant_id, event_id, occurred_at) DO NOTHING
RETURNING event_id`

// Insert writes one event. It returns nil when the row was written and
// ErrNotInserted when the same key was already there, which is Done either
// way; anything else is Postgres or the network speaking and goes to
// Classify. The RETURNING clause is what 0002 asks for: the rollup, when it
// exists, runs in this transaction only when a row came back. There is no
// rollup yet, so this is a single statement and its own transaction.
func (s *Store) Insert(ctx context.Context, e event.AgentRun) error {
	var id string
	err := s.pool.QueryRow(ctx, insertSQL,
		e.Tenant, e.EventID, e.OccurredAt, e.Agent, e.AgentVersion, e.SessionID, e.RepoHash,
		e.DurationMs, e.Outcome, e.ErrorKind, e.Model, e.TokensIn, e.TokensOut, e.CostMicros,
		e.FilesTouched, e.LinesAdded, e.LinesRemoved, e.ToolCalls,
	).Scan(&id)
	if errors.Is(err, errNoRows()) {
		return ErrNotInserted
	}
	return err
}

// Count is for tests and operators: rows for one tenant and event id, across
// every partition.
func (s *Store) Count(ctx context.Context, tenant, eventID string) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE tenant_id = $1 AND event_id = $2`, tenant, eventID).Scan(&n)
	return n, err
}
