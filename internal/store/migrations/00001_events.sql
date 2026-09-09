-- +goose Up

-- The raw event log. One row per agent_run.v1, the shape measured in
-- docs/artifacts/2026-08-28-events-partition-interval.md: closed sets as
-- smallint, hashes as bytea, 436 bytes a row with both indexes, weekly range
-- partitions on occurred_at because retention is promised in days and a
-- partition is the unit of deletion.
--
-- The unique key is three columns, not two. A unique index on a partitioned
-- table has to contain the partition column, Postgres refuses it otherwise
-- (nine-docs/adr/0002 measured the refusal), so deduplication is
-- (tenant_id, event_id, occurred_at) and the insert in store.go infers on
-- exactly those. A redelivered message carries the same bytes and conflicts;
-- a client that sends one event_id with two timestamps is not covered, and
-- that is stated in 0002 rather than hidden here.
--
-- The enums are positional: agent 0 is the first value in the contract's
-- enum array, and so on. Appending a value is the only change the contract
-- may make; event/agent_run_test.go pins the order against the vendored
-- schema so a reorder fails the build instead of relabelling stored rows.
CREATE TABLE events (
    tenant_id     text        NOT NULL,
    event_id      text        NOT NULL,
    occurred_at   timestamptz NOT NULL,
    agent         smallint    NOT NULL,
    agent_version text,
    session_id    bytea,
    repo_hash     bytea,
    duration_ms   bigint      NOT NULL,
    outcome       smallint    NOT NULL,
    error_kind    smallint,
    model         text,
    tokens_in     bigint,
    tokens_out    bigint,
    cost_micros   bigint,
    files_touched bigint,
    lines_added   bigint,
    lines_removed bigint,
    tool_calls    bigint,
    CONSTRAINT events_session_id_is_sha256 CHECK (session_id IS NULL OR octet_length(session_id) = 32),
    CONSTRAINT events_repo_hash_is_sha256  CHECK (repo_hash  IS NULL OR octet_length(repo_hash)  = 32)
) PARTITION BY RANGE (occurred_at);

CREATE UNIQUE INDEX events_tenant_event ON events (tenant_id, event_id, occurred_at);
CREATE INDEX events_tenant_time ON events (tenant_id, occurred_at DESC);

-- Twelve weekly partitions from the first Monday of the plan, 31 August 2026.
-- This is a horizon, not a mechanism: CO3.2 is the mechanism that creates the
-- next week's partition, and until it lands an event dated outside this range
-- raises 23514, which nine-docs/adr/0001 maps to Retry: the partition is the
-- dependency that is not there yet. Monday boundaries at UTC, so a week is
-- the same week in every partition name.
-- +goose StatementBegin
DO $$
DECLARE
    week_start date := DATE '2026-08-31';
    i int;
BEGIN
    FOR i IN 0..11 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS events_w%s PARTITION OF events FOR VALUES FROM (%L) TO (%L)',
            to_char(week_start + i * 7, 'IYYY_IW'),
            (week_start + i * 7)::timestamptz,
            (week_start + (i + 1) * 7)::timestamptz
        );
    END LOOP;
END $$;
-- +goose StatementEnd

-- The consumer connects as nine_app, which owns nothing and can only insert
-- and read. The role is created by the platform's init.sql, not here; a
-- scratch database without it still migrates, so the grant is conditional.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'nine_app') THEN
        GRANT SELECT, INSERT ON events TO nine_app;
    END IF;
END $$;
-- +goose StatementEnd

-- +goose Down
DROP TABLE events;
