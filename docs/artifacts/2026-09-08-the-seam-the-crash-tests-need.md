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

## Why stdout and stdin rather than something cleverer

A file needs a path, a cleanup and a race on creation. A port needs a number that
is free. Both are state the test has to manage and something else can collide
with. The child process already has two pipes the parent owns, and a line on each
is a handshake with no shared resource, no polling and nothing to clean up.

## What this asks of `CO2.1`

Only that the point exists where the decision in `nine-docs/adr/0002` puts it:
after the database transaction returns, before the offset is committed, and
inside neither. One line, guarded by a constant, plus the two files above.

The build tag is worth a line in CI: a job that greps the shipped binary for
`NINE_FAULT` and fails if it finds it costs nothing and turns the table above
into a standing guarantee rather than a measurement someone took once.
