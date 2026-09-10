# nine-core

Queue consumer and aggregation. Kafka for the event log, and for retries and the dead letter queue.

Part of **Nine**, telemetry for AI coding agents.
An agent runs, leaves a trail; Nine collects it and makes it queryable.

## Status

Pre-alpha. Not yet usable. See [ROADMAP](#roadmap).

## Why this exists

Teams running Claude Code, Cursor or Codex across a codebase have no shared
view of what those agents actually did: which files they touched, how long
runs took, what they cost, where they failed. Nine ingests that stream and
answers those questions.

## Architecture

```
agent -> [ingest] -> Kafka ----> [core] -> Postgres
                       |                    ^
                       +-> events.retry-5m  |
                       +-> events.retry-1h  |
                       +-> events.parked    |
                       +-> events.dlq       |
                                    [api] <-+- Redis <- [web]
```

This repository is the **core** component. The others:

| Repo | Language | Role |
|---|---|---|
| [ingest](https://github.com/ennaton/nine-ingest) | Go | Event intake, auth, rate limiting |
| [core](https://github.com/ennaton/nine-core) | Go | Queue consumers, aggregation, storage |
| [api](https://github.com/ennaton/nine-api) | Go | Query API, caching |
| [web](https://github.com/ennaton/nine-web) | TypeScript | Dashboard |
| [platform](https://github.com/ennaton/nine-platform) | HCL | Deployment, scaling, observability |
| [sdk-js](https://github.com/ennaton/nine-sdk-js) | TS / Go / Python | Client libraries |
| [bench](https://github.com/ennaton/nine-bench) | Python | Load and chaos testing |
| [docs](https://github.com/ennaton/nine-docs) | Markdown | Decisions and measurements |

## Development

Go 1.26 and the compose stack from
[nine-platform](https://github.com/ennaton/nine-platform) for a broker.

```bash
go test -race ./...                       # the broker is franz-go's in process kfake, no Docker
NINE_TEST_MIGRATE_DSN=postgres://postgres:postgres@localhost:15432/postgres go test -race ./internal/store/   # nine:allow-secret, the compose owner
go run ./cmd/migrate                      # applies the events schema to nine_core, as the owner
NINE_KAFKA_BROKERS=localhost:19092 go run ./cmd/core   # joins group "core" on topic "events", writes as nine_app
```

`cmd/migrate` and `cmd/partition` read `NINE_CORE_MIGRATE_DSN` and `cmd/core`
reads `NINE_CORE_DSN`; all default to the compose stack. They are separate
binaries on purpose: the consumer connects as `nine_app`, which owns nothing
and can only insert and read, so it never holds the owner's password, and
creating a partition is DDL.

```bash
go run ./cmd/partition           # keeps four weeks ahead and two behind, idempotent
go run ./cmd/partition -list     # every partition and its bounds, creating nothing
```

The same binary reads a delay topic: `NINE_TOPIC=events.retry-5m` with
`NINE_CONSUMER_DELAY=5m` and its own group. A record is not handed to the
handler until it has sat on that topic for the delay, measured from the
record's own timestamp, and the wait is a pause and a seek rather than a
sleep, so a rebalance does not queue behind it. See
`docs/artifacts/2026-09-10-a-delay-topic-is-a-pause-not-a-sleep.md`.

`cmd/partition` runs on a schedule, weekly or nightly, and creates nothing
when the horizon is already there. It never creates a partition because an
event asked for one: the horizon follows the clock, not the data, and
`docs/artifacts/2026-09-09-the-horizon-runs-ahead-of-the-clock.md` measures
what the other way would cost.

The `events` table is the shape measured in
`docs/artifacts/2026-08-28-events-partition-interval.md`, weekly range
partitions on `occurred_at`, and the insert is `ON CONFLICT (tenant_id,
event_id, occurred_at) DO NOTHING`: the same event twice is one row, per
tenant, and a partitioned table forces the third column into the key
(`nine-docs/adr/0002`). The first migration creates twelve weekly partitions
from 31 August 2026 as a horizon; the mechanism that creates the next one is
CO3.2, and until it lands an event dated outside the horizon is a `Retry`.

Two instances of `cmd/core` split the topic between them and the rebalance is
visible in both logs; `docs/artifacts/2026-09-08-co2-1-two-instances-rebalance.md`
is one such run against the compose broker. Offsets are committed by hand and
only for records the consumer has accounted for, a `Done` from the handler or a
`Retry` or `Poison` the sink took; a record it cannot account for stops it with
that record and everything after it uncommitted. The order, database first and
offset second, is `nine-docs/adr/0002`.

`go build -tags faultinject ./cmd/core` produces the binary the crash tests
kill: `NINE_FAULT_AFTER_DB_COMMIT=exit` leaves with code 97 between the
handlers and the offset commit, `=pause` prints `nine-fault-reached` and waits
on stdin, or leaves with code 98 if stdin ends first, so a closed stdin is a
failure the test sees and not a pause that never happened. The plain build does not contain the variable's name, and CI checks
that on every push.

## Measurements

Every performance claim about this component links to a reproducible run in
[nine-bench](https://github.com/ennaton/nine-bench) and a written report in
[nine-docs](https://github.com/ennaton/nine-docs). No number without a method.

## Roadmap

See [nine-docs](https://github.com/ennaton/nine-docs) for the phase plan.

## License

MIT, see [LICENSE](./LICENSE).
