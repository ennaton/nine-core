// Package event decodes agent_run.v1 from the log into the shape the table
// stores.
//
// ingest validated the event before it reached Kafka, so this package does
// not validate again; it decodes strictly, because a message that does not
// decode is Poison and the decoder is where that answer is given. The struct
// mirrors contracts/events/agent_run.v1.json, vendored here the way ingest
// vendors it, and agent_run_test.go fails the build if the two drift.
package event

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// AgentRun is one row of events, with the tenant that ingest put in the
// record key rather than in the value.
type AgentRun struct {
	Tenant       string
	EventID      string
	OccurredAt   time.Time
	Agent        int16
	AgentVersion *string
	SessionID    []byte
	RepoHash     []byte
	DurationMs   int64
	Outcome      int16
	ErrorKind    *int16
	Model        *string
	TokensIn     *int64
	TokensOut    *int64
	CostMicros   *int64
	FilesTouched *int64
	LinesAdded   *int64
	LinesRemoved *int64
	ToolCalls    *int64
}

// wire is the JSON shape, field for field the contract. It is separate from
// AgentRun so the stored shape (positional enums, bytes) is not confused with
// the transmitted one (strings, hex).
type wire struct {
	EventID      string  `json:"event_id"`
	Agent        string  `json:"agent"`
	AgentVersion *string `json:"agent_version"`
	SessionID    *string `json:"session_id"`
	RepoHash     *string `json:"repo_hash"`
	OccurredAt   string  `json:"occurred_at"`
	DurationMs   int64   `json:"duration_ms"`
	Outcome      string  `json:"outcome"`
	ErrorKind    *string `json:"error_kind"`
	Model        *string `json:"model"`
	TokensIn     *int64  `json:"tokens_in"`
	TokensOut    *int64  `json:"tokens_out"`
	CostMicros   *int64  `json:"cost_micros"`
	FilesTouched *int64  `json:"files_touched"`
	LinesAdded   *int64  `json:"lines_added"`
	LinesRemoved *int64  `json:"lines_removed"`
	ToolCalls    *int64  `json:"tool_calls"`
}

// The closed sets, in the contract's order. The stored value is the index,
// so this order is part of the stored data: a value may be appended and
// never moved. The test pins it against the vendored schema.
var (
	Agents     = []string{"claude-code", "cursor", "codex", "copilot", "aider", "other"}
	Outcomes   = []string{"success", "error", "cancelled", "timeout"}
	ErrorKinds = []string{"rate_limit", "context_limit", "tool_failure", "network", "auth", "user_abort", "unknown"}
)

// ErrUnknownValue is a value the closed set does not hold: a schema version
// this consumer does not know, which nine-docs/adr/0001 makes Poison.
var ErrUnknownValue = errors.New("value outside the contract's enum")

func index(set []string, v string) (int16, error) {
	for i, s := range set {
		if s == v {
			return int16(i), nil
		}
	}
	return 0, fmt.Errorf("%w: %q", ErrUnknownValue, v)
}

// Decode turns one record into a row. Every error it returns means the
// record will never decode, whatever the system does, which is the
// definition of Poison.
func Decode(key, value []byte) (AgentRun, error) {
	var w wire
	dec := json.NewDecoder(bytes.NewReader(value))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&w); err != nil {
		return AgentRun{}, fmt.Errorf("decode: %w", err)
	}
	if len(key) == 0 {
		return AgentRun{}, errors.New("decode: record has no key, and the key is the tenant")
	}
	if w.EventID == "" {
		return AgentRun{}, errors.New("decode: event_id missing")
	}
	at, err := time.Parse(time.RFC3339Nano, w.OccurredAt)
	if err != nil {
		return AgentRun{}, fmt.Errorf("decode occurred_at: %w", err)
	}
	r := AgentRun{
		Tenant: string(key), EventID: w.EventID, OccurredAt: at.UTC(),
		AgentVersion: w.AgentVersion, DurationMs: w.DurationMs, Model: w.Model,
		TokensIn: w.TokensIn, TokensOut: w.TokensOut, CostMicros: w.CostMicros,
		FilesTouched: w.FilesTouched, LinesAdded: w.LinesAdded, LinesRemoved: w.LinesRemoved, ToolCalls: w.ToolCalls,
	}
	if r.Agent, err = index(Agents, w.Agent); err != nil {
		return AgentRun{}, fmt.Errorf("decode agent: %w", err)
	}
	if r.Outcome, err = index(Outcomes, w.Outcome); err != nil {
		return AgentRun{}, fmt.Errorf("decode outcome: %w", err)
	}
	if w.ErrorKind != nil {
		k, err := index(ErrorKinds, *w.ErrorKind)
		if err != nil {
			return AgentRun{}, fmt.Errorf("decode error_kind: %w", err)
		}
		r.ErrorKind = &k
	}
	if r.SessionID, err = sha256Bytes(w.SessionID); err != nil {
		return AgentRun{}, fmt.Errorf("decode session_id: %w", err)
	}
	if r.RepoHash, err = sha256Bytes(w.RepoHash); err != nil {
		return AgentRun{}, fmt.Errorf("decode repo_hash: %w", err)
	}
	return r, nil
}

// sha256Bytes turns the contract's 64 hex characters into the 32 bytes the
// table stores, half the width, which is where the 17 percent in the
// partition measurement came from.
func sha256Bytes(s *string) ([]byte, error) {
	if s == nil {
		return nil, nil
	}
	b, err := hex.DecodeString(*s)
	if err != nil {
		return nil, err
	}
	if len(b) != 32 {
		return nil, fmt.Errorf("%d bytes, want 32", len(b))
	}
	return b, nil
}
