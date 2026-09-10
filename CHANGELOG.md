# Changelog

All notable changes to this component are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning follows [SemVer](https://semver.org/spec/v2.0.0.html).
Each repository versions independently.

## [Unreleased]

### Added
- `cmd/core/crash_test.go`, `CO2.4`: the process is killed between the database commit and the offset commit, restarted, and the events table holds one row per event rather than two. The window is forced through the faultinject exit point rather than waited for, and both sides of it are read from Postgres and from the broker rather than from the consumer. The workflow gains a Kafka service, because a test that cannot run in CI is the fail open the database already refuses there.
- Repository scaffold.
- `internal/consumer`: group membership with franz-go, automatic commit off, one synchronous offset commit per batch and only for the records the consumer accounted for: a `Done` from the handler, or a `Retry` or `Poison` the `Sink` acknowledged. A record it cannot account for, `Fatal`, `Unknown`, a `Retry` or `Poison` with no `Sink`, a `Sink` or hook that failed, stops it with that record and everything after it uncommitted. `BlockRebalanceOnPoll` keeps a partition until the batch is committed; the commit runs on its own bounded context so a shutdown inside a batch still commits what the handlers finished. A `CommitHook` sits between the handler and the offset commit, the crash window of `nine-docs/adr/0002`. Tests run against kfake, franz-go's in process broker.
- `cmd/core`: joins group `core` on topic `events` with a handler that logs the event id and answers `Done`, enough to show two instances splitting the topic. Built with `-tags faultinject` it carries the fault point CO2.4 and CO2.5 stand on, `NINE_FAULT_AFTER_DB_COMMIT=exit|pause`; the plain build does not contain the variable's name and CI checks that.
- `internal/store`: the `events` table as its first migration, weekly range partitions with a twelve week horizon, the three column unique key, and the idempotent insert `ON CONFLICT DO NOTHING RETURNING`. `Classify` is the mapping table of `nine-docs/adr/0001` applied to what Postgres said, closing rule `Fatal`; `23514`, no partition for the row, is `Retry` by revision 3. `Handler` is the consumer's handler, one insert and one classification per record. Tests measure a real PostgreSQL and skip without one.
- `internal/event`: `agent_run.v1` decoded strictly from the log into the stored shape, positional enums and 32 byte hashes, with the contract vendored and a test that pins field set and enum order against it.
- `store.EnsurePartitions` and `cmd/partition`: the weekly horizon of `events`, kept four weeks ahead of the clock and two behind, idempotent and serialised on an advisory lock because `CREATE TABLE IF NOT EXISTS` is not atomic. It refuses to run at all while `events` carries a default partition, which would end retention and hide the horizon's own failures. It never creates a partition in response to an event: an event outside the horizon is a `Retry` by `nine-docs/adr/0001` revision 3, and letting the data decide the partition count costs every query five times its planning time at 520 partitions.
- `cmd/migrate`: applies the schema as the owner. `cmd/core` now writes through the store as `nine_app`.
- `pipeline.Outcome` has a `String`, so logs and errors name the outcome instead of printing an integer.

### Changed
- The chain described in `CLAUDE.md` and the README ends in `events.parked`, not `events.dlq`: a `Retry` that used both rounds is parked for replay, a `Poison` goes to the dlq on first sight. Decided in `nine-docs/adr/0001`.
