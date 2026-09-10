# The chain is one list, and what writing the forwarder found

`CO4.4` sends a record where `nine-docs/adr/0001` says it goes. Two things
came out of writing it that are worth more than the row itself: the ladder is
now described in one place in Go, and three defects in `CO4.3`, merged the
same day, were only reachable once something actually forwarded a record.

## One list

@MustafaKemalV asked for this on the review of `CO4.3`, and the reason is the
history. `0001` exists because three places described the retry chain and no
two agreed. `Spent(r, tiers)` took the tier count as an argument, so the
number would have been written again at the call site: a fourth description,
in the shape of an integer literal.

```go
var Chain = []string{"events", "events.retry-5m", "events.retry-1h"}
func Tiers() int { return len(Chain) - 1 }
```

Routing reads the list rather than a number: a record is retried while the
ladder has another rung and parked when it does not. Changing the ladder is
one edit.

`topics.sh` in `nine-platform` is the other description and there is no way to
merge the two, so `TestTheChainMatchesTheTopicScript` reads the script and
fails if a topic named here is not created there. It skips when the sibling
repository is not checked out, which is the honest behaviour for a test that
reaches outside its own repository.

## Three defects, found by writing the next row

Each was invisible while nothing forwarded a record, and each was in code that
had passed review and CI the same day.

**A moving record carried an empty `nine-outcome`.** `0003`'s vocabulary for
that header is `retry-exhausted` or `poison`, and a record on its way to
another tier is neither. The comment in the same file says a header that is
present and meaningless is worse than one that is absent, and it was
describing its own output. A moving record now carries five marks and a
stopped one seven, which is a revision `0003` is owed.

**`Park` raised the round.** A record that spent both tiers would have parked
saying three, while `0003`'s table says the value is how many rounds were
spent. `Retry` spends a tier, `Park` and `Poison` do not.

**The rule that an unreadable round counts as spent could not be carried out.**
`Spent` answered true and sent the record to `events.parked`, and then `Park`
called `Read`, which refused the same header, so the forward failed and the
consumer stopped. The rule written to stop a loop stopped the consumer
instead. A record like that is now parked as fully spent and carries
`nine-failure-code: unreadable-round`, so it says what stopped it rather than
blaming whatever the handler last reported.

## Measured end to end

Against the compose broker and database, `cmd/core` reading `events` with the
forwarder wired, two payloads that will never decode:

```
"msg":"forwarded","to":"events.dlq","outcome":"Poison","round":0,
  "failure_code":"decode","from":"events","partition":0,"offset":5
```

and what is on the topic, headers included:

```
nine-event-id:run3-evt-12, nine-retry-round:0, nine-failure-code:decode,
nine-source-topic:events, nine-outcome:poison,
nine-first-failed-at:2026-09-10T14:54:08Z, nine-parked-at:2026-09-10T14:54:08Z
    {"event_id":"run3-evt-12","agent":"claude-code","occurred_at":"2026-09-08T10:00:012Z",...}

nine-retry-round:0, nine-failure-code:decode, nine-source-topic:events,
nine-outcome:poison, nine-first-failed-at:..., nine-parked-at:...
    {"not":"an agent_run"}
```

The first record has a valid `event_id` and an impossible timestamp, so the id
is read off the body and the record carries all seven marks. The second has no
id anywhere, so `nine-event-id` is absent rather than empty. That is the stated
limit of a dead letter record: the one case where the body cannot supply the id
is the case the header exists for, and `CO5.1` either closes it by having the
decoder surface an id from a partial parse or writes it down.

A fourth defect showed up in that same run and is fixed here: the log said
`"outcome":3`. `slog`'s JSON handler marshals an int type as an int, so the
`String` method added for exactly this printed nothing. It reads `"Poison"`
now.

## What is not here

**`CO5.1`, `CO5.2` and `CO5.3`.** Nothing reads `events.parked` or
`events.dlq` yet. This puts records there in the shape those rows will read.

**A delay consumer in the deployment.** `CO4.2` gave `cmd/core` a delay and
this gives it a forwarder, so a second instance on `events.retry-5m` with
`NINE_CONSUMER_DELAY=5m` is the whole of the chain running. Nothing in the
repository starts one; that is the deployment's job and `PL2` owns it.
