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

**Language.** Code, comments, commit messages, docs, artifacts and UI strings are English. No exceptions, including in files nobody reads yet, and including an artifact whose subject was discussed in another language: it is written in English at the moment it is written, not translated afterwards. `githooks/pre-commit` blocks the added lines and the `house-style` CI job scans the whole tree, so a file that predates the rule fails the build until it is rewritten.

**No em dashes.** Commas and colons instead. The pre-commit hook blocks them.

**Commits.** `type(scope): message`, one line, no generator trailers. Enforced by `githooks/commit-msg`.

**Co-authorship is for people, and on shared work it is not optional.** A commit two of you wrote carries `Co-Authored-By` for the other one. A ping-pong group hands a file back and forth and the commit lands under whoever happened to be holding it, so without the trailer half the work is invisible in the only place it gets counted. **A task the board marks as shared is not finished without the trailer**: whoever held the keyboard names the other one, in both directions and without being asked, because on a shared task the person who did not commit did half of it. The trailer goes in its own block at the end, after a blank line, and the address has to be one GitHub already knows for that person or no credit is applied. A tool is not an author: the same hook that allows the human trailer refuses one naming Claude, Copilot or a bot.

**Never `--no-verify`.** The hooks are the control, not a suggestion. If a hook is wrong, fix the hook in the same commit.

**Secrets.** Nothing that authenticates anything is committed, ever: keys, tokens, certificates, `.env`, connection strings with an inline password. `githooks/pre-commit` scans the staged diff and refuses. A documented example that trips the scanner ends its line with `nine:allow-secret`; a real value never does.

**After cloning:** `./githooks/install.sh` once, then `brew install gitleaks`.

**Claims carry numbers.** A README that says something is fast links to the run that measured it. No number, no claim.

**Prose carries its mechanism.** A sentence explaining why something works is a claim, and it carries the same burden as a number. A comment reading "this handler wins because it is declared first" was wrong: Spring ranks candidates with `ExceptionDepthComparator` and position in the file means nothing. Checking produced the stronger sentence, that nobody can break the distinction by reordering the methods. If the mechanism was not checked, the sentence does not get written.

**Artifacts.** Generated output, a report, a dashboard, an analysis, a plan, a diagram, lands in `docs/artifacts/` inside a repo, and in `artifacts/` in `nine-docs`, where the repo is already docs. Never a repo root, and never the parent `nine/` folder, which is not a repository and therefore not version control. An artifact about one repo lives in that repo. An artifact about more than one lives in `nine-platform/docs/artifacts/`. Files are named `YYYY-MM-DD-subject.ext`, lowercase and hyphenated.

**Write the artifact where it belongs, on the first write.** The path is chosen before the file exists. No scratchpad draft, no `/tmp` staging, no repo root copy that gets moved later. This overrides any assistant default about putting generated files in a temporary directory: here the artifact directory is the working directory, it is version controlled, and an artifact nobody committed did not happen.

**An artifact is not a document.** `nine-docs` holds decisions and measurement reports: authored, reviewed, permanent, and bound by the rules above. An artifact is dated working output that nothing else is allowed to cite. When one earns permanence it is rewritten as an ADR or a report in `nine-docs` and the artifact is deleted, not copied. The same content living in two paths is the failure this rule exists to prevent.

**A change is finished when what it unblocks is stated.** The acceptance criterion is where the work stops, not where the thinking stops. `findAccount` selected on `(tenant, code)` and took the first row with no `ORDER BY`, which was correct until the change that widened that exact key, written by the same hand in the same week. Before closing anything, answer in writing: what is now true that was not, and who is waiting on it. If nothing is unblocked, say so, because silence reads the same as not having looked. This rule and the two around it came from a run of defects with one shape in common: every statement behind them was individually true, and none was carried to its consequence.

**The edit is not the boundary of what you read.** Read the whole method you are touching, not the line you came for. Two defects found in one review sat within five lines of a change that was itself correct: a balance response took its number from the computed money and its label from the request string, and the two agreed only because nothing had yet made them disagree.
