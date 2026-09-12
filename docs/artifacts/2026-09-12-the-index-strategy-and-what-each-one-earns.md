# Every query against events, and what each index earns

`CO3.4`. The row asks for an `EXPLAIN` per query and for unused indexes to go.
This is the first half in full, and the second half's answer is that none of the
four is unused, which is a conclusion rather than an omission: each one is named
here with the query that chose it.

**Measured on** PostgreSQL 16 in the compose stack, a database migrated from
`00001` and `00002`, fourteen weekly partitions, 200,000 rows across four tenants
and twenty one days, `ANALYZE` run before every plan. `EXPLAIN (ANALYZE, BUFFERS,
COSTS OFF, TIMING OFF)`, so the numbers are buffers rather than milliseconds:
timings on a laptop under Docker say more about the laptop.

## The queries that exist

Three touch `events`, three touch `events_partition_drops`. There are no others:
`grep` for `FROM events`, `INTO events` and `UPDATE events` outside tests returns
exactly these.

### 1. The idempotent insert, `store.go`

```
Insert on events
  Conflict Resolution: NOTHING
  Conflict Arbiter Indexes: events_tenant_event
  Tuples Inserted: 1   Conflicting Tuples: 0
  Buffers: shared hit=104
```

`events_tenant_event` is the arbiter, which is the whole of `nine-docs/adr/0002`
arriving in a plan: the three column key the partitioned table forces is what the
conflict is resolved against.

### 2. `Count`, one tenant and one event id

```
Aggregate
  ->  Append
        ->  Index Only Scan using events_w2026_34_tenant_id_event_id_occurred_at_idx
              Index Cond: ((tenant_id = 'tenant-2') AND (event_id = 'evt-100000'))
              Heap Fetches: 0
        ->  ... the same, once per partition
  Buffers: shared hit=11
```

An index only scan in every partition, and no pruning, because the query carries
no `occurred_at` and pruning is on the partition key. Eleven buffers here is
cheap and it is the wrong number to remember: the cost grows with the number of
partitions rather than with the rows. At a year of weekly retention that is
fifty two probes for one answer.

That is acceptable for what this is. `Count` says in its own comment that it is
for tests and operators. It is recorded because the shape must not travel: a read
path that asks for one event id without a time window pays the whole horizon, and
`AP` should carry a window or an exact `occurred_at`.

### 3. The read the API will do, tenant and a time window

```
Limit
  ->  Append
        Subplans Removed: 3
        ->  Index Scan using events_w2026_47_tenant_id_occurred_at_idx
              Index Cond: ((tenant_id = 'tenant-2') AND (occurred_at >= now() - '3 days'))
  Buffers: shared hit=38
```

`Subplans Removed: 3` is the pruning the shape above could not have. This is the
only plan that uses `events_tenant_time`, and no code in `nine-core` runs it
today: the index exists for a reader that has not been written. That is worth
writing down precisely because a later count of callers would find zero and
remove it.

### 4, 5 and 6. `events_partition_drops`

The insert is `ON CONFLICT (partition_name)`, so the unique index is its arbiter,
and the two updates are by primary key. The query that matters is the one the
replay tool will run, and `nine-docs/adr/0002` is why it exists at all: the
retention boundary is also the boundary of the deduplication guarantee.

```
SELECT max(range_end) FROM events_partition_drops
  Result
    ->  Index Only Scan using events_partition_drops_range_idx
  Buffers: shared hit=3
```

## What each index earns

| Index | Chosen by | Keep |
|---|---|---|
| `events_tenant_event` | the insert, as conflict arbiter | yes, it is the key `0002` requires |
| `events_tenant_time` | the tenant plus window read, with pruning | yes, and no caller yet: see below |
| `events_partition_drops_name_key` | the record upsert, as arbiter | yes |
| `events_partition_drops_range_idx` | the boundary query | yes, measured below |

**`events_tenant_time` has no caller in `nine-core` and stays.** The plan above is
the shape `AP` will read, it prunes, and the alternative is that the first read
path arrives without an index and is measured as slow for a reason that was
decided here. This row is the justification, so that a later audit sees a decision
rather than a leftover.

**`events_partition_drops_range_idx` was the one I expected to remove, and the
measurement said otherwise.** The table takes one row per week ever dropped, so it
is 56 kB after ten years, and an index on a table that small looks like ceremony.
At 520 rows, which is that decade:

```
ORDER BY range_end DESC LIMIT 5   with the index:  3 buffers
                                  without it:     10 buffers, top-N heapsort
max(range_end)                    with the index:  3 buffers, index only scan
```

Three against ten is not a large number, and the replay tool asks this question
on every run rather than once. It stays.

## What this measurement does not say

The index sizes here are a property of the data I generated, not of production.
`events_tenant_event` came out at 16 MB and `events_tenant_time` at 1488 kB on the
same 200,000 rows, and the difference is B-tree deduplication: four distinct
tenants and twenty one distinct days deduplicate almost perfectly, while 200,000
distinct event ids do not deduplicate at all. Real tenant and time cardinality
will move both. `CO3.1` is where the size arithmetic lives, and it measured the
row rather than the index.

Nothing here is a latency figure. Buffers are comparable between plans on one
machine; milliseconds under Docker on a laptop are not comparable to anything.
