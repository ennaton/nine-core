# A delay topic is a pause and a seek, not a sleep

`CO4.2` holds a record on a retry topic until it has waited out that topic's
delay. The obvious way is to sleep until it ripens, and the obvious way costs
something nobody would choose to pay. This says what, with the measurement,
and what the mechanism is instead.

## Sleeping would hold the rebalance for the length of the delay

The consumer passes `BlockRebalanceOnPoll`, which is `CO2.1`'s decision: a
partition cannot be taken away in the middle of a batch, so the batch commits
what it earned before it lets go. That makes the batch's own duration the
window a rebalance waits for, and a batch that sleeps out a five minute delay
is a rebalance, a deploy and a scale up that all wait five minutes behind a
retry topic.

So the wait is not inside the batch. When the head record of a partition is
not ripe:

1. the offset is set back to that record, so nothing is skipped,
2. the partition is paused, so the fetcher stops asking for it,
3. the batch ends, commits what it earned, and calls `AllowRebalance`,
4. the poll is bounded by the soonest partition that ripens, so a consumer
   with every partition paused wakes up rather than blocking forever,
5. the partition resumes and the same record is fetched again.

`TestAHeldPartitionDoesNotHoldTheRebalance` is the claim: three partitions,
every one of them paused behind a one minute delay, a second member joins and
the topic splits between the two while nothing has been handled. The
assertion bounds that at thirty seconds and it takes about one.

## The clock is the record's own timestamp

A record's timestamp on a retry topic is the moment it was produced onto it,
which is when its wait started. `nine-docs/adr/0003` measured that a forwarded
record does not keep the timestamp of the record it came from, and that is a
loss for "how old is the trouble", which is why 0003 puts that in a header.
For "how long has this waited here" it is exactly right, and it is free.

A consequence worth stating because it is the behaviour after an outage: a
record that has already sat on the topic longer than the delay is ripe the
moment it is read. A consumer that starts after an hour down does not add a
fresh delay to a backlog that already waited.
`TestARecordOlderThanTheDelayIsRipeOnArrival` produces with a timestamp an
hour in the past and asserts it is handled at once behind a thirty second
delay.

## Measured end to end

Against the compose broker and database, `cmd/core` reading
`events.retry-1h` with `NINE_CONSUMER_DELAY=10s`, a real event produced by
`kafka-console-producer`:

```
at 5 s   rows in events for co42-run-1: 0
written after 11 s
```

Nothing at five seconds, the row at eleven. The same binary with no delay
writes it in under a second, which is `TestWithNoDelayTheSameRecordIsHandledAtOnce`.

## The criterion says five seconds, and no such topic exists

The board's criterion for this row reads "a message on the five second topic
is processed no earlier than five seconds later". It predates `adr/0001`,
which fixed the chain at two tiers of five minutes and one hour; the five
second ladder was the variant that measurement removed. The row's criterion
is `CO4`'s decision and is left as it stands rather than edited here.

What the tests do about it is the trade @MustafaKemalV named on
`nine-billing#32`: the mechanism is proven at three seconds and the shipped
numbers are not proven at all, because a test that waits five minutes is a
test nobody runs. The delay is configuration, one number, and the shipped one
comes from `0001`.

## One thing this does not do

**It does not forward anything.** A record that ripens goes to the handler and
the handler is the insert. Producing to the next tier, counting the round and
parking after the second, is `CO4.3` and `CO4.4`, and both wait on `adr/0003`
being signed. This row is the gate and nothing behind it.

An incidental confirmation from the first end to end run, which failed: the
topic still held two records from the `0003` timestamp probe, which are not
`agent_run.v1`. They decoded as `Poison`, there is no `Sink` yet, and the
consumer stopped with the offset uncommitted rather than dropping them. That
is `CO2.1`'s rule doing its job in a setting nobody arranged for it.
