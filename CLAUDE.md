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

**Artifacts.** Generated output, a report, a dashboard, an analysis, a plan, a diagram, lands in `docs/artifacts/` inside a repo, and in `artifacts/` in `nine-docs`, where the repo is already docs. Never a repo root, and never the parent `nine/` folder, which is not a repository and therefore not version control. An artifact about one repo lives in that repo. An artifact about more than one lives in `nine-platform/docs/artifacts/`. Files are named `YYYY-MM-DD-subject.ext`, lowercase and hyphenated.

**An artifact is not a document.** `nine-docs` holds decisions and measurement reports: authored, reviewed, permanent, and bound by the rules above. An artifact is dated working output that nothing else is allowed to cite. When one earns permanence it is rewritten as an ADR or a report in `nine-docs` and the artifact is deleted, not copied. The same content living in two paths is the failure this rule exists to prevent.
