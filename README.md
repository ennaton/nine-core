# wakelog/core

Queue consumer and aggregation. Kafka for the event log, RabbitMQ for retries and DLQ.

Part of **[wakelog](https://github.com/wakelog)** — telemetry for AI coding agents.
An agent runs, leaves a wake; wakelog collects it and makes it queryable.

## Status

Pre-alpha. Not yet usable. See [ROADMAP](#roadmap).

## Why this exists

Teams running Claude Code, Cursor or Codex across a codebase have no shared
view of what those agents actually did: which files they touched, how long
runs took, what they cost, where they failed. wakelog ingests that stream and
answers those questions.

## Architecture

```
agent -> [ingest] -> Kafka ----> [core] -> Postgres
             |                              ^
          RabbitMQ -> retry/DLQ             |
                                    [api] <-+- Redis <- [web]
```

This repository is the **core** component. The others:

| Repo | Language | Role |
|---|---|---|
| [ingest](https://github.com/wakelog/ingest) | Go | Event intake, auth, rate limiting |
| [core](https://github.com/wakelog/core) | Go | Queue consumers, aggregation, storage |
| [api](https://github.com/wakelog/api) | Go | Query API, caching |
| [web](https://github.com/wakelog/web) | TypeScript | Dashboard |
| [infra](https://github.com/wakelog/infra) | HCL | Deployment, scaling, observability |
| [sdk](https://github.com/wakelog/sdk) | TS / Go / Python | Client libraries |
| [bench](https://github.com/wakelog/bench) | Python | Load and chaos testing |
| [docs](https://github.com/wakelog/docs) | Markdown | Decisions and measurements |

## Development

```bash
# requirements and setup land here once the component has code
```

## Measurements

Every performance claim about this component links to a reproducible run in
[wakelog/bench](https://github.com/wakelog/bench) and a written report in
[wakelog/docs](https://github.com/wakelog/docs). No number without a method.

## Roadmap

See [wakelog/docs](https://github.com/wakelog/docs) for the phase plan.

## License

MIT — see [LICENSE](./LICENSE).
