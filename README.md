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

```bash
# requirements and setup land here once the component has code
```

## Measurements

Every performance claim about this component links to a reproducible run in
[nine-bench](https://github.com/ennaton/nine-bench) and a written report in
[nine-docs](https://github.com/ennaton/nine-docs). No number without a method.

## Roadmap

See [nine-docs](https://github.com/ennaton/nine-docs) for the phase plan.

## License

MIT, see [LICENSE](./LICENSE).
