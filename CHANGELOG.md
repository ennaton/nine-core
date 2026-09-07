# Changelog

All notable changes to this component are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning follows [SemVer](https://semver.org/spec/v2.0.0.html).
Each repository versions independently.

## [Unreleased]

### Added
- Repository scaffold.

### Changed
- The chain described in `CLAUDE.md` and the README ends in `events.parked`, not `events.dlq`: a `Retry` that used both rounds is parked for replay, a `Poison` goes to the dlq on first sight. Decided in `nine-docs/adr/0001`.
