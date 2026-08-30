# Error classification, the state before the session

CO1.1 is in place. This is what CO1.2 and CO1.3 still have to settle, what is
already settled and why, and one number that was wrong in the discussion and is
corrected here with the evidence.

## Settled, and not worth reopening

@MustafaKemalV raised three of these and they are right. They are written down so
the session does not spend time re-deriving them.

**Zero rows is Done.** An idempotent insert that conflicts writes nothing and
raises nothing. That is the event already being recorded, by whoever won the race,
so the log advances. A consumer that reads zero rows as failure redelivers the
same message forever against a database that will never answer differently. The
enum documents this on `Done` rather than leaving it to whoever writes the first
handler.

**Classify the cause, not the symptom.** Under concurrency Postgres reports a
`23505` and then, on every later statement in the same aborted transaction, a
`25P02 current transaction is aborted`. The second is a consequence of the first.
A consumer that classifies on the last error it saw reads `25P02` as transient and
retries, when the real answer was `23505`, which is `Done`. This is a rule about
the whole mapping rather than a row in it.

**The constraint name is a field, not a substring.** `pgconn.PgError` carries
`ConstraintName`. Matching on message text is what `nine-billing` did, it is what
BI4 existed to remove, and it is now removed there. Core starts with the field.

## The number in the discussion was wrong

The suggestion was that the retry round count derives from the chain rather than
being invented, which is right, and that the chain is three tiers at five seconds,
one minute and ten minutes. It is not. The topics that exist:

```
platform/compose/kafka/topics.sh
  events            6 partitions
  events.retry-5m   3
  events.retry-1h   3
  events.dlq        1
```

`core/CLAUDE.md` and the plan both say the same thing:
`events -> events.retry-5m -> events.retry-1h -> events.dlq`.

**Two retry tiers, at five minutes and one hour.** So if the count derives from the
chain, the answer is two rounds and not three, and the delays are two orders of
magnitude apart from the ones discussed. The reasoning was sound and the input was
not, which is exactly the kind of thing that must not be frozen into a contract.

## Still open, and the session should close it

### 1. Two rounds, or is the chain itself wrong

Deriving from the chain gives two. The question underneath is whether five minutes
then one hour is the right ladder for this failure class. A broker that blinks is
back in seconds and a tenant waits five minutes for nothing; a database failover
can outlast an hour. Either the chain is right and the answer is two, or the chain
is the thing to change first and the count follows.

Whichever way, it belongs in the ADR with the rejected alternative, which is what
CO1.3 asks for.

### 2. Does an exhausted Retry go where a Poison goes

Raised in the handover and worth deciding here, because the answer changes
`platform` and not just this repository.

The argument for separating them is the operator's next action. A poisoned payload
will never process and the useful action is a person reading it; replay is waste.
An exhausted retry is a good payload behind a dependency that was down, and the
useful action is replaying the lot once the dependency is back. Different action,
different destination.

Today there is one `events.dlq`. Separating them means a second topic declared in
`platform/compose/kafka/topics.sh`, which is a change in another repository and
another person's review. That is the cost, and it is small, but it is the reason
this cannot be decided silently inside `core`.

CO5.1 asks what a DLQ record contains. That question gets easier if the two are
apart: a parked record needs which dependency and when to replay, a poison record
needs which field and which schema version.

## The mapping table, as far as it can be written now

CO1.2 needs a row per error type. These are the ones the shape of the system
already fixes. The rest come from running the consumer against a real broker and a
real database and reading what comes back, the way BI4.1 was built rather than
recalled.

| What happened | Outcome | Why |
|---|---|---|
| Idempotent insert conflicted, zero rows | `Done` | Already recorded. See above |
| `23505` on the idempotency key | `Done` | The same event, arriving twice |
| `25P02` after another error in the same transaction | classify the first one | Symptom, never the cause |
| Broker unreachable, connection refused, timeout | `Retry` | Nothing about the message is wrong |
| Payload does not decode | `Poison` | No amount of waiting changes it |
| Schema version the consumer does not know | `Poison` | Needs a deploy, not a retry |
| Config missing or contradictory at startup | `Fatal` | Every message would be wrong, not this one |
| Schema mismatch against the database | `Fatal` | Continuing corrupts the log |

Two rows are deliberately absent. `23503` and `23514` both appear in
`nine-billing`'s mapping and both depend on which constraint fired, so they cannot
be classified until `core` has constraints of its own, which is CO3.

## What this file is not

Not the ADR. CO1.2 and CO1.3 produce that, and it belongs in `nine-docs` once
there is a place for ADRs. This is the state of the question on the morning of the
session, so the session can spend its time on the two open items rather than on
rebuilding the settled ones.
