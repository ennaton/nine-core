# The seam the crash tests need, and how it stays out of the shipped binary

`CO2.4` kills the consumer between the database commit and the offset commit.
`CO2.5` holds it at the same point while a rebalance takes the partition away.
Both need the consumer to stop there on purpose, and `CO2.1` is being written
now, so this says what the tests need before the seam is placed rather than
after.

A test that reaches the window by sleeping is a test that passes on a fast
machine and fails on a loaded one, and worse, passes for the wrong reason on
both. So the seam has to do two things: stop, and say that it stopped.

## The shape

One injection point, behind a build tag, with the mode chosen per point by an
environment variable:

```go
//go:build !faultinject
const faultInjection = false
func pauseOrCrash(string) {}
```

```go
//go:build faultinject
const faultInjection = true

func pauseOrCrash(point string) {
	switch os.Getenv("NINE_FAULT_" + point) {
	case "exit":
		os.Exit(97)
	case "pause":
		fmt.Println("nine-fault-reached: " + point)
		bufio.NewReader(os.Stdin).ReadString('\n')
	}
}
```

The call site is one line after the transaction commits and before the offset is
committed:

```go
if faultInjection {
	pauseOrCrash("AFTER_DB_COMMIT")
}
```

## It is absent from the shipped binary, not disabled in it

`faultInjection` is a constant, so the branch is eliminated at compile time.
Measured on go1.25.4, two binaries from the same source:

| Binary | Occurrences of `NINE_FAULT` in the binary | With the variable set |
|---|---|---|
| `go build` | 0 | runs to completion, exit 0 |
| `go build -tags faultinject` | 1 | stops at the point, exit 97 |

The production binary does not contain the environment variable's name, so the
question "can this fire in production" has an answer that does not depend on
anyone remembering to unset something.

## What each mode gives the test

**`exit`, for `CO2.4`.** The process is gone, which is the strongest signal there
is: the test waits on the process rather than on a clock, and the exit code says
which point fired. Then it verifies both sides of the window from outside, with
the consumer no longer running: the row is in Postgres, and the committed offset
for the group is unchanged. Neither assertion can pass by accident, and neither
needs the consumer's cooperation to be trusted.

**`pause`, for `CO2.5`.** The consumer announces the point on stdout and blocks
reading stdin. The test reads that line, which is a blocking read and not a
timeout, starts the second consumer to force the rebalance, and then releases the
first one by writing a line. Measured end to end, with no sleep anywhere:

```
1. line: database committed
2. line: nine-fault-reached: AFTER_DB_COMMIT   (blocking read returned in 0.008 s)
   process still alive: yes
3. line: offset committed   exit code: 0
```

**A third outcome, because the first two were not enough.** The design as first
written had a hole that `@canakyuz` found and that I then reproduced: run the
tagged binary with stdin closed, which is how docker and systemd start a service,
and `ReadString` returns EOF at once. Measured on the design exactly as written
here:

```
NINE_FAULT_AFTER_DB_COMMIT=pause ./test </dev/null
database committed
nine-fault-reached: AFTER_DB_COMMIT
offset committed
exit=0
```

The announcement was printed and the process walked straight through the window.
A test that had read that line would believe the consumer was standing before the
offset commit while the commit had already happened, which is this document's own
opening complaint arriving by a different road: it passes for the wrong reason.

So EOF before a release line is its own exit, 98, and never a return. What landed
carries it, and `CO2.5` is written against three outcomes rather than two.

## Why stdout and stdin rather than something cleverer

A file needs a path, a cleanup and a race on creation. A port needs a number that
is free. Both are state the test has to manage and something else can collide
with. The child process already has two pipes the parent owns, and a line on each
is a handshake with no shared resource, no polling and nothing to clean up.

## What landed, which is not quite the shape above

`CO2.1` merged as `nine-core#12` and the seam it carries is a combination of this
design and the one that was already being written, which is better than either.
The tag lives in `cmd/core`, not in the consumer: `fault_on.go` installs
`consumer.WithCommitHook(pauseOrCrash)` and `fault_off.go` adds nothing, so the
seam itself is an ordinary library option and only the crash binary ever reaches
for it. A `CO2.4` written from the shape section above would look for
`if faultInjection` inside the consumer and not find it.

The build tag is in CI, which was the point of writing the table above rather
than taking the measurement once. `.github/workflows/ci.yml` builds both binaries
on every push and fails if `NINE_FAULT` appears in the shipped one:

```
go build -o /tmp/core ./cmd/core
if grep -q NINE_FAULT /tmp/core; then echo "NINE_FAULT is in the shipped binary"; exit 1; fi
go build -tags faultinject -o /tmp/core-fault ./cmd/core
grep -q NINE_FAULT /tmp/core-fault
```

The numbers in the table came from a scratch module on go1.25.4, because this
repository asks for go1.26.4 and the machine that measured it had 1.25. They are
kept for the reasoning rather than as the current figures: the CI job above is
now the standing answer, on the repository's own toolchain, on every push.
