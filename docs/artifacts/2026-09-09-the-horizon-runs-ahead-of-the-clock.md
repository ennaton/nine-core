# The horizon runs ahead of the clock, never behind the data

`CO3.2` creates the weekly partitions of `events` before the rows need them.
The row's criterion is that an insert to a date with no partition raises no
error. There are two mechanisms that satisfy that sentence and only one of
them is safe, so this says which, and why, with the measurements that decided
it. Measured on PostgreSQL 16.15 against a table built to the `CO3.1` shape.

## The mechanism that satisfies the sentence literally is the one that must be refused

The literal reading is that the insert itself creates the partition it needs,
by a trigger or by the consumer catching `23514`. That makes the event decide
how many partitions this table has, and every query in the system pays for
the answer:

| Partitions | Planning time, same one week query |
|---|---|
| 12 | 2.03 ms |
| 52 | 2.50 ms |
| 520 | 10.46 ms |

Twelve to fifty two is the cost of the interval `CO3.1` chose and it is
already paid. Fifty two to five hundred and twenty is five times the planning
cost of every query, forever, and a client with a wrong clock reaches it
without trying: events dated a week apart across two centuries are ten
thousand partitions, and nothing in the pipeline would refuse them, because
each one is a valid `agent_run.v1` with a timestamp the schema allows.

It would also need the consumer to hold DDL rights. `cmd/core` connects as
`nine_app`, which owns nothing and holds `SELECT` and `INSERT`, and that is
the line `CO2.2` drew.

So the horizon is a function of `now()`. An event outside it stays what
`nine-docs/adr/0001` revision 3 makes it: a `Retry`, which waits five minutes,
then an hour, and then parks with its date readable by a person. A message
whose partition will never exist ends up somewhere a human looks, rather than
silently enlarging the table.

## What the criterion means, then

A date whose partition did not exist takes an insert without an error once the
maintainer has reached it. `TestAWeekWithNoPartitionAcceptsAnInsertOnceTheHorizonReachesIt`
is that sentence: the same insert answers `23514` before the run and nothing
after it, and the classification in between is `Retry` rather than a stop.

## Creating a partition stops the write path, briefly

`CREATE TABLE ... PARTITION OF` takes `AccessExclusiveLock` on the parent, the
same lock a plain `DETACH` takes:

```
ev        AccessExclusiveLock
ev_w2     AccessExclusiveLock
```

Measured with the creation held open in a transaction, an insert on the parent
waited 2.37 s, which is the whole time the transaction was held. The statement
itself is short:

```
CREATE TABLE ... PARTITION OF   24.062 ms   (first, cold)
                                 2.216 ms
                                 4.661 ms
```

So the write path pauses by milliseconds per partition created, and only when
one is actually created. That is why the span is small and the schedule is
weekly rather than per message: seven partitions on a first run, none on every
run after it.

## `IF NOT EXISTS` is not a lock

Two sessions creating the same partition at the same time:

```
ERROR:  relation "ev_w4" already exists
```

`CREATE TABLE IF NOT EXISTS` looks and then creates, and the two steps are not
one. So the maintainer holds an advisory lock for the length of its run,
`pg_advisory_lock(0x6e696e6531)`, and two of them are a wait rather than an
error. `TestTwoMaintainersAtOnceDoNotCollide` runs four at once against an
empty horizon and asserts they create seven partitions between them, not
seven each and not an error.

Under that lock the existence check moves out of the `CREATE` and into a
`to_regclass` before it, because `IF NOT EXISTS` reports the same command tag
whether it created or skipped, and the run's report is what an operator reads.

## The default partition, refused before any DDL

`core#14` measured that a default partition makes `DETACH PARTITION ...
CONCURRENTLY` impossible, which ends retention. It also hides this mechanism's
own failure: with a default in place, an event outside the horizon lands in it
instead of raising `23514`, so the missing week stops being visible and the
rows end up in a partition that can never be dropped.

So the maintainer looks for one before it does anything and stops:

```
$ partition
events carries a default partition, which breaks retention and hides the horizon: events_default
exit=1
```

## Against the compose stack

```
$ migrate                       nine_core is at the latest migration
$ partition -list | tail -1     12 partitions          <- the fixed block in 00001
$ partition                     created events_w2026_35
$ partition                     the horizon is already there, nothing created
```

Only one week was missing, because `00001_events.sql` created weeks 36 to 47
from a date written into the migration and today is week 37. That fixed block
is a horizon that expires, and this is what replaces it: the migration is left
alone, since a migration that has been applied is not edited, and every week
after 47 comes from the maintainer.

## What this does not decide

**When it runs.** The binary is a one shot and idempotent, so a weekly cron, a
nightly one, or a step in the deploy all work. There is no scheduler here and
nothing in the repository schedules it yet.

**The span.** Four weeks ahead and two behind are defaults with a reason
rather than a measurement: four ahead survives a maintainer that has not run
for three weeks, two behind covers an agent that batched its events. If a real
lateness distribution ever exists, `BE1` is where it comes from and these two
numbers should follow it.

**Retention.** `CO3.3` drops the other end and `core#14` is its design. The
two meet at one rule this file enforces and that one requires: `events` never
carries a default partition.
