-- +goose Up

-- What a dropped partition leaves behind. Retention is the one operation here
-- that destroys data on purpose, and the rows are the only thing that can say
-- it happened: after the drop there is nothing left to count, nothing to date,
-- and nothing to tell a replay tool where the boundary is.
--
-- nine-docs/adr/0002 is why the last part matters. Deduplication is a unique
-- index and an index cannot see a row that went away with its partition, so the
-- retention boundary is also the boundary of the idempotency guarantee. A
-- replay from events.parked that crosses it either stops the consumer or counts
-- an event twice, and this table is where the replay tool reads the line.
--
-- dropped_at is nullable on purpose, the same shape reconciliation_runs arrived
-- at in nine-billing: the row is written after the partition is detached and
-- before it is dropped, so a run that dies in between leaves a row that says
-- so. A detached, undropped partition is invisible through the parent and
-- would otherwise be an orphan nobody is looking for.
CREATE TABLE events_partition_drops (
    id             BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    partition_name TEXT        NOT NULL,
    range_start    TIMESTAMPTZ NOT NULL,
    range_end      TIMESTAMPTZ NOT NULL,
    -- Exact, not an estimate. The count is taken after the detach, when the
    -- partition is a standalone table nothing else reads: measured on
    -- PostgreSQL 16, 200,000 rows in 0.11 s with a write open on the parent,
    -- which is the reason the earlier design's reltuples estimate is not needed.
    rows_dropped   BIGINT      NOT NULL,
    detached_at    TIMESTAMPTZ NOT NULL,
    dropped_at     TIMESTAMPTZ
);

CREATE INDEX events_partition_drops_range_idx ON events_partition_drops (range_end DESC);

-- The replay tool reads this as nine_app. It never writes: retention is DDL and
-- runs as the owner, like every other partition operation.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'nine_app') THEN
        GRANT SELECT ON events_partition_drops TO nine_app;
    END IF;
END $$;
-- +goose StatementEnd

-- +goose Down
DROP TABLE events_partition_drops;
