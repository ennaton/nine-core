# What retention must do to a partition, and the order it must do it in

`CO3.3` drops old partitions of `events`. The table does not exist yet and no
board row creates it, so this is not the implementation. It is the set of things
the implementation cannot choose freely, measured against a table built to the
`CO3.1` shape on PostgreSQL 16 rather than reasoned from the documentation.

The short version: detach concurrently, and make the retry `FINALIZE` rather than
another detach.

## A plain detach stops ingest for as long as it runs

`ALTER TABLE ... DETACH PARTITION` takes `AccessExclusiveLock` on the parent and
on the partition. Measured, the lock modes held by the detaching session:

```
ev      -> AccessExclusiveLock
ev_w36  -> AccessExclusiveLock
```

That lock conflicts with the ordinary insert path. With a detach open in one
session, an insert in another dies rather than waits:

```
SET lock_timeout = '2s';
INSERT INTO ev VALUES (...);
ERROR:  canceling statement due to lock timeout
```

So a retention job written the obvious way takes the write path down every time
it runs, on a service whose whole job is to accept a continuous stream.

## Concurrently does not block new writes, and waits for the old ones

`DETACH PARTITION ... CONCURRENTLY` waits for transactions that were already open
when it started, and lets new work through while it waits. Measured against a
transaction that held the table for five seconds: the detach returned in five
seconds, and an insert issued in the middle of that window succeeded.

It cannot run inside a transaction block, which is worth knowing before the job
is written as one.

## An interrupted concurrent detach leaves the partition half attached

This is the part that decides the shape of the retry. Cancelling a concurrent
detach, which a lock timeout or a deploy will do eventually, does not roll the
operation back:

```
ERROR:  canceling statement due to lock timeout
```

```
ev_w36 inhdetachpending=true
ev_w37 inhdetachpending=false
```

And the obvious retry, running the same statement again, does not recover:

```
ERROR:  partition "ev_w36" already pending detach in partitioned table "public.ev"
HINT:  Use ALTER TABLE ... DETACH PARTITION ... FINALIZE to complete the pending
       detach operation.
```

So the job has to read `pg_inherits.inhdetachpending` before it decides what to
run. A retry that repeats the detach fails forever and leaves the partition half
attached forever, which is the state where the table has neither the rows nor the
space back.

## The guard reads bounds, not names

A partition's range is available as an expression, so "is this one still active"
is a comparison rather than a match on a name that a later convention could
change:

```
SELECT c.relname, pg_get_expr(c.relpartbound, c.oid)
  FROM pg_class c
  JOIN pg_inherits i ON i.inhrelid = c.oid
  JOIN pg_class p ON p.oid = i.inhparent
 WHERE p.relname = 'events';
```

```
ev_w36  FOR VALUES FROM ('2026-08-31 00:00:00+00') TO ('2026-09-07 00:00:00+00')
ev_w37  FOR VALUES FROM ('2026-09-07 00:00:00+00') TO ('2026-09-14 00:00:00+00')
```

Two refusals fall out of that, and the board's criterion is the first one: a
partition whose range contains `now()` is never dropped, and neither is one whose
upper bound is newer than the retention boundary. Both are arithmetic on the
bounds above.

## The record is written before the drop, because afterwards there is nothing to count

The board asks for a record of the drop. Its useful fields are the ones that stop
existing at the moment they would be interesting: the partition's name and range,
and how many rows it held. A count taken after `DROP TABLE` has nothing to count,
so the record is written first and the drop follows it.

## Dropping a partition ends deduplication for the events in it

`nine-docs/adr/0002` measured this and it belongs here too, because retention is
the thing that causes it. The unique constraint cannot see a row that went away
with its partition. Redelivering a message whose partition has been dropped
raises `23514, no partition of relation "ev" found for row`, and if `CO3.2` has
recreated that partition in the meantime, the insert succeeds and the event is
counted a second time.

So the retention boundary is also the boundary of the idempotency guarantee, and
a replay from `events.parked` that crosses it either stops the consumer or double
counts. The record of the drop is what lets the replay tool know where that line
is, which is a second reason for it to exist.

## The order

Read the bounds, refuse an active partition, write the record with the row count,
detach concurrently or finalize a pending detach, then drop the detached table.

## What is not measured here

Whether dropping the already detached table takes any lock on the parent. It
should not, since the table is independent by then, and that is the reason for
detaching first rather than dropping outright, but I did not measure it and it is
not written here as though I had.

And the table itself. `nine-core` holds no SQL and no migration tool, and no board
row creates `events`. Until that has an owner, this is a design that cannot be run.
