# Changelog

All notable changes to this component are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning follows [SemVer](https://semver.org/spec/v2.0.0.html).
Each repository versions independently.

## [Unreleased]

### Added
- Repository scaffold.
- `internal/consumer`: group membership with franz-go, automatic commit off, offsets committed by hand and only for records the handler answered `Done`, one synchronous commit per batch. `BlockRebalanceOnPoll` keeps a partition until the batch is committed. A `CommitHook` sits between the handler and the offset commit, the crash window of `nine-docs/adr/0002`, so CO2.4 can stop the process there. `Retry` and `Poison` go to a `Sink`; without one they stop the consumer rather than losing the record. `Fatal` and `Unknown` stop it with nothing committed. Tests run against kfake, franz-go's in process broker.
- `cmd/core`: joins group `core` on topic `events` with a handler that logs the event id and answers `Done`, enough to show two instances splitting the topic.

### Changed
- The chain described in `CLAUDE.md` and the README ends in `events.parked`, not `events.dlq`: a `Retry` that used both rounds is parked for replay, a `Poison` goes to the dlq on first sight. Decided in `nine-docs/adr/0001`.
