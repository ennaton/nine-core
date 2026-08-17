# wesan-core

Queue consumer and aggregation. Kafka for the event log, RabbitMQ for retries and DLQ.

Part of **the wesan system**, telemetry for AI coding agents.
An agent runs, leaves a wake; wesan collects it and makes it queryable.

## Status

Pre-alpha. Not yet usable. See [ROADMAP](#roadmap).

## Why this exists

Teams running Claude Code, Cursor or Codex across a codebase have no shared
view of what those agents actually did: which files they touched, how long
runs took, what they cost, where they failed. wesan ingests that stream and
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
| [ingest](https://github.com/canakyuz/wesan-ingest) | Go | Event intake, auth, rate limiting |
| [core](https://github.com/canakyuz/wesan-core) | Go | Queue consumers, aggregation, storage |
| [api](https://github.com/canakyuz/wesan-api) | Go | Query API, caching |
| [web](https://github.com/canakyuz/wesan-web) | TypeScript | Dashboard |
| [infra](https://github.com/canakyuz/wesan-infra) | HCL | Deployment, scaling, observability |
| [sdk](https://github.com/canakyuz/wesan-sdk) | TS / Go / Python | Client libraries |
| [bench](https://github.com/canakyuz/wesan-bench) | Python | Load and chaos testing |
| [docs](https://github.com/canakyuz/wesan-docs) | Markdown | Decisions and measurements |

## Development

```bash
# requirements and setup land here once the component has code
```

## Measurements

Every performance claim about this component links to a reproducible run in
[wesan/bench](https://github.com/wesan/bench) and a written report in
[wesan/docs](https://github.com/wesan/docs). No number without a method.

## Roadmap

See [wesan/docs](https://github.com/wesan/docs) for the phase plan.

## License

MIT, see [LICENSE](./LICENSE).
