# The events partition interval

CO3.1. Weekly, not monthly. The decision, the arithmetic behind it, and the
measurement that settled it.

**Measured on** Postgres 16.15 from `nine-platform`, port 15432, in a scratch
database that was dropped afterwards. The row width came from building the table
and filling it, not from adding up type sizes on paper.

## The decision

`events` is partitioned by range on `occurred_at`, one partition per week.

## What a row actually costs

`agent_run.v1` has seventeen fields. Two shapes were built and filled with 200,000
representative rows each, then measured with the two indexes the table will carry
(`(tenant_id, occurred_at DESC)` and a unique `(tenant_id, event_id)`).

| Shape | Heap | Indexes | Total | Bytes per row |
|---|---|---|---|---|
| Closed sets as `text`, hashes as hex `text` | 71 MB | 29 MB | 100 MB | **526** |
| Closed sets as `smallint`, hashes as `bytea` | 54 MB | 29 MB | 83 MB | **436** |

The tighter shape costs 17 percent less on disk for the same data. `agent`,
`outcome` and `error_kind` are closed enums in the contract, and `session_id` and
`repo_hash` are SHA-256, which is 32 bytes as `bytea` and 64 characters as hex.
Everything below uses **436 bytes per row**, the tight shape, because that is the
shape the table should have.

## Rows and size per partition

There is no measured event volume yet: the load contract is BE1 and it is not
written. So the table is a function of volume rather than a single number, and it
names the volume each answer assumes.

| Events per day | Weekly rows | Weekly size | Monthly rows | Monthly size |
|---|---|---|---|---|
| 10,000 | 70,000 | 0.03 GB | 300,000 | 0.13 GB |
| 100,000 | 700,000 | 0.31 GB | 3,000,000 | 1.31 GB |
| 1,000,000 | 7,000,000 | 3.05 GB | 30,000,000 | 13.08 GB |
| 10,000,000 | 70,000,000 | 30.52 GB | 300,000,000 | 130.80 GB |

Size alone does not force the choice until roughly one million events a day, where
monthly partitions reach thirteen gigabytes and vacuum, index builds and drops
start to be felt. Below that both intervals are comfortable, so size is not the
argument. The two arguments that follow hold at every volume.

## Why weekly, in order of force

### 1. Retention is promised in days, and deletion is a partition drop

The plan states raw events are kept for N days, rollups indefinitely, and that
deletion works by dropping partitions rather than row by row. Those two sentences
together fix the interval: **the partition interval is the resolution of the
retention promise.**

With monthly partitions, guaranteeing thirty days of retention means holding
between thirty and sixty, because a partition cannot be dropped until its newest
row is older than the promise. The average is forty five, so roughly half again as
much raw data as the promise implies, and a privacy commitment that is
systematically looser than it reads.

With weekly partitions the same guarantee holds between thirty and thirty seven
days. The overshoot drops from thirty days to seven.

The alternative, deleting rows inside a partition to honour the promise exactly,
gives up the reason for partitioning in the first place.

### 2. The query shape wastes a monthly partition

The dashboard's natural windows are the last seven days and the last thirty. The
seven day case was measured on 52 weekly partitions against 12 monthly ones, same
data, same query:

```
52 weekly partitions, 7 day query
  Seq Scan on ev_w_8      rows=10080   Rows Removed by Filter: 0
  Planning 0.707 ms   Execution 0.834 ms

12 monthly partitions, same query
  Seq Scan on ev_m_2      rows=10080   Rows Removed by Filter: 34560
  Planning 0.175 ms   Execution 1.464 ms
```

Pruning picks exactly one partition either way, so the difference is not pruning,
it is what is inside the partition it picked. The monthly one reads a whole month
and throws away 77 percent of it.

The cost of weekly is visible in the same numbers and is worth stating plainly:
planning takes 0.53 ms longer with 52 partitions than with 12. That cost is
**constant**, paid once per query and unchanged by data volume. The wasted scan is
**proportional**, and grows with every row added. At this tiny volume weekly is
already ahead on total time, 1.54 ms against 1.64 ms, and the gap only opens.

### 3. The escalation path is downward, not upward

If volume grows past roughly ten million events a day, weekly partitions reach
thirty gigabytes and the next move is **daily**, not monthly. Choosing monthly now
would mean crossing weekly on the way down later, which is a migration nobody
needs. Weekly is on the path; monthly is not.

## What would change this

Monthly would be right if retention were promised in months rather than days
**and** the common query window were a calendar month. Neither is true today. If
the retention rule is ever restated in months, this decision should be reopened,
and this file is the arithmetic to reopen it with.

## What this does not decide

- **CO3.2, automatic partition creation.** Weekly means fifty two partitions a
  year rather than twelve, so the mechanism that creates the next period's
  partition stops being optional. That is the next task and it is the direct cost
  of this decision.
- **The column types.** The 436 byte figure assumes closed sets stored as
  `smallint` and hashes as `bytea`. If the table is built with `text` everywhere,
  every number above rises by 17 percent per row and the thresholds move in.
- **Retention length.** N is still unwritten. This decision holds for any N stated
  in days, which is why it does not wait for the number.

## Reproduce

```sql
-- row width, two shapes, 200k rows each, with both indexes
SELECT relname, pg_size_pretty(pg_total_relation_size(oid)),
       round(pg_total_relation_size(oid)::numeric/200000,1) AS bytes_per_row
  FROM pg_class WHERE relname IN ('events_naive','events_tight');

-- pruning and waste, 52 weekly against 12 monthly, same 300k rows
EXPLAIN (ANALYZE, SUMMARY)
SELECT count(*) FROM ev_w WHERE occurred_at >= '2026-03-02' AND occurred_at < '2026-03-09';
```

## Promote this

This is dated working output, so it lives here. The partition interval is an
architectural decision with alternatives and a reason, which is what an ADR is
for. When `nine-docs` grows an ADR structure this should be rewritten there and
this file deleted rather than copied.
