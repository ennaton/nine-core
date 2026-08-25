# nine-core

Queue consumer and aggregation. Go, Kafka, PostgreSQL.

## Status

Not started. This file states the rules before the first line of code, which is the only time they are free to set.

## What this repo is for

Turning the raw event log into queryable data. Everything that is not the edge and not a read query happens here.

## Rules specific to this repo

**Idempotent by construction.** The same event replayed from the log must not double count. The event id is the key; a unique constraint enforces it, not an application check that races.

**Retry and dead letter live on Kafka.** The topic chain is `events -> events.retry-5m -> events.retry-1h -> events.dlq`. A second broker is not added until a measurement shows the retry chain is insufficient, and the decision gets written up either way.

**Postgres holds the truth.** Time-partitioned events plus precomputed rollups. Every query that matters carries an `EXPLAIN` in the pull request that introduced it.

**Normalize before billing sees it.** `nine-billing` must never interpret raw agent telemetry. Core turns events into a stable usage contract; billing prices that.

## Rules every Nine repo shares

**Language.** Code, comments, commit messages, docs and UI strings are English. No exceptions, including in files nobody reads yet.

**No em dashes.** Commas and colons instead. The pre-commit hook blocks them.

**Commits.** `type(scope): message`, one line, no `Co-Authored-By`, no generator trailers. Enforced by `githooks/commit-msg`.

**Never `--no-verify`.** The hooks are the control, not a suggestion. If a hook is wrong, fix the hook in the same commit.

**Secrets.** Nothing that authenticates anything is committed, ever: keys, tokens, certificates, `.env`, connection strings with an inline password. `githooks/pre-commit` scans the staged diff and refuses. A documented example that trips the scanner ends its line with `nine:allow-secret`; a real value never does.

**After cloning:** `./githooks/install.sh` once, then `brew install gitleaks`.

**Claims carry numbers.** A README that says something is fast links to the run that measured it. No number, no claim.
