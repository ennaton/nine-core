# Changelog

All notable changes to this component are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning follows [SemVer](https://semver.org/spec/v2.0.0.html).
Each repository versions independently.

## [Unreleased]

### Added
- Repository scaffold.
- `internal/consumer`: group membership with franz-go, automatic commit off, one synchronous offset commit per batch and only for the records the consumer accounted for: a `Done` from the handler, or a `Retry` or `Poison` the `Sink` acknowledged. A record it cannot account for, `Fatal`, `Unknown`, a `Retry` or `Poison` with no `Sink`, a `Sink` or hook that failed, stops it with that record and everything after it uncommitted. `BlockRebalanceOnPoll` keeps a partition until the batch is committed; the commit runs on its own bounded context so a shutdown inside a batch still commits what the handlers finished. A `CommitHook` sits between the handler and the offset commit, the crash window of `nine-docs/adr/0002`. Tests run against kfake, franz-go's in process broker.
- `cmd/core`: joins group `core` on topic `events` with a handler that logs the event id and answers `Done`, enough to show two instances splitting the topic. Built with `-tags faultinject` it carries the fault point CO2.4 and CO2.5 stand on, `NINE_FAULT_AFTER_DB_COMMIT=exit|pause`; the plain build does not contain the variable's name and CI checks that.
- `pipeline.Outcome` has a `String`, so logs and errors name the outcome instead of printing an integer.

### Changed
- The chain described in `CLAUDE.md` and the README ends in `events.parked`, not `events.dlq`: a `Retry` that used both rounds is parked for replay, a `Poison` goes to the dlq on first sight. Decided in `nine-docs/adr/0001`.
