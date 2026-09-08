# CO2.1: two instances split the topic, and the rebalance is in the log

8 September 2026, against the compose broker from `nine-platform` (Kafka 3.9,
one node, `events` with six partitions), `cmd/core` built from the commit that
adds it. The criterion: two instances started, the rebalance visible in the
log, the partitions shared out.

## The run

```
NINE_KAFKA_BROKERS=localhost:19092 NINE_CONSUMER_GROUP=co2-1-proof-3 ./core   # A
sleep 6; same command                                                         # B
sleep 12; twelve events produced with kafka-console-producer, keys tenant-0..3
sleep 6; SIGTERM B; sleep 8; SIGTERM A
```

The group was new, so A first read the twenty four events earlier runs had
left on the topic. Those lines are dropped below; the twelve produced in this
run carry the prefix `run3-`.

## Instance A

```
14:07:46.564 core joining            group=co2-1-proof-3 topic=events
14:07:49.600 partitions assigned     [0,1,2,3,4,5]
14:07:53.115 partitions revoked      [4,5,3]              <- B has joined
             event x6: run3-evt-2 6 10 1 5 9  (tenant-2 and tenant-1, partitions 0 and 1)
14:08:15.175 partitions assigned     [3,4,5]              <- B has left
14:08:20.315 core leaving
14:08:20.315 partitions revoked      [0,1,2,3,4,5]
```

## Instance B

```
14:07:52.559 core joining            group=co2-1-proof-3 topic=events
14:07:53.624 partitions assigned     [3,4,5]
             event x6: run3-evt-3 7 11 4 8 12  (tenant-3 and tenant-0, partitions 3 and 4)
14:08:12.309 core leaving
14:08:12.309 partitions revoked      [3,4,5]
```

## Committed offsets after both left

`kafka-consumer-groups.sh --describe --group co2-1-proof-3`:

| partition | committed | log end | lag |
|---|---|---|---|
| 0 | 6 | 6 | 0 |
| 1 | 6 | 6 | 0 |
| 3 | 8 | 8 | 0 |
| 4 | 6 | 6 | 0 |

Partitions 2 and 5 held no messages: four tenant keys hash to four
partitions. Automatic commit is off in the client, so every committed offset
above was written by `CommitRecords` after the handler answered `Done`.

## What this shows, and what it does not

Two members, one rebalance each way, six partitions held by exactly one
member at every moment the log records, twelve events handled once each and
by the member that held the partition. It does not show the crash window:
that is CO2.4, and the seam it needs, `CommitHook`, is in place and covered
by `consumer_test.go` against kfake.
