# What retention must do to a partition, and the order it must do it in

`CO3.3` drops old partitions of `events`. The table exists as of `CO2.2`, `nine-core#15`, which was the gap this file first
named as unowned, so this is the design rather than the implementation. It is the set of things
the implementation cannot choose freely, measured against a table built to the
`CO3.1` shape on PostgreSQL 16 rather than reasoned from the documentation.

The short version: detach concurrently, and make the retry `FINALIZE` rather than
another detach.

The scratch table is called `ev` throughout, partitioned by range on `at`, which
is `events` partitioned on `occurred_at` with shorter names. The blocks below
carry the values that came back, reformatted to read; the error strings are
verbatim and the rest is not a terminal capture.

## A plain detach stops ingest for as long as it runs

`ALTER TABLE ... DETACH PARTITION` takes `AccessExclusiveLock` on the parent and
on the partition. Measured, the lock modes held by the detaching session:

```
ev      -> AccessExclusiveLock
ev_w36  -> AccessExclusiveLock
```

That lock conflicts with the ordinary insert path. An insert issued while the
detach is open blocks for as long as the detach runs, and dies only if its
session carries a lock timeout. Both halves were measured, and the first
statement of this artifact had them backwards: my `SET lock_timeout = '2s'` is
what killed the insert, not the detach.

```
with lock_timeout = 2s     ERROR:  canceling statement due to lock timeout
with no lock_timeout       waited 2.57 s for the detach, then succeeded
```

The second line is @canakyuz's measurement, on the review of this file. The
conclusion is the same either way and the mechanism is not: a retention job
written the obvious way takes the write path down for its duration, on a service
whose whole job is to accept a continuous stream.

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
 WHERE p.relname = 'ev';
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

## A default partition breaks all of this, so `events` must never have one

Measured by @canakyuz on the review of this file, and it is the finding that
decides whether the design works at all:

```
CREATE TABLE ev_default PARTITION OF ev DEFAULT;
ALTER TABLE ev DETACH PARTITION ev_w36 CONCURRENTLY;
ERROR:  cannot detach partitions concurrently when a default partition exists
```

A default partition is the first thing a careful person adds when `CO3.2` is late
with next week's, and from that moment every retention run ends in that error. So
the rule is that `events` carries no default partition, and the job checks for one
before it starts and refuses loudly rather than discovering it in a log.

The first migration is on the right side of this already: `CO2.2` creates twelve
weekly partitions and no default, and an event outside the horizon raises `23514`,
which `nine-docs/adr/0001` revision 3 maps to `Retry` rather than to a stopped
consumer.

## Dropping the detached table does not touch the parent

The reason for detaching first rather than dropping outright, measured by
@canakyuz rather than assumed here, which is how the first version of this file
left it:

```
DROP TABLE on the detached table:  AccessExclusiveLock on the table, its toast
                                   table and its index, and on nothing else
insert on ev during the drop:      0.08 s
```

So the last step of the order costs the write path nothing.

## The record carries the bounds exactly and the row count as an estimate

`count(*)` on a partition about to be dropped is a full scan, and at the volumes
`CO3.1` measured that is seven million rows a week at a million events a day,
and seven hundred thousand at a hundred thousand. The record does not need it to be exact: what the replay tool reads is the
boundary, and the boundary is the partition's range, which is exact and free.

So the record carries the range as it comes from `relpartbound`, and the row count
from `pg_class.reltuples`, written down as an estimate and named as one. On the
scratch table `reltuples` after `ANALYZE` matched `count(*)` exactly, which is
worth nothing as a guarantee: it is an estimate that is stale between analyzes,
and a number that says how many rows were dropped is an audit line, not an
invariant. If an exact count is ever wanted, it is a deliberate scan and not the
default cost of every retention run.
