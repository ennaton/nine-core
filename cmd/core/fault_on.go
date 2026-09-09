//go:build faultinject

package main

import (
	"bufio"
	"context"
	"fmt"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ennaton/nine-core/internal/consumer"
)

// The crash test binary, built with -tags faultinject and nothing else. One
// point so far, AFTER_DB_COMMIT: the handlers have returned for the batch and
// the offsets are not yet committed, the window of nine-docs/adr/0002. The
// mode is chosen by NINE_FAULT_AFTER_DB_COMMIT: "exit" leaves with code 97,
// which is what CO2.4 waits on; "pause" prints a line and blocks on stdin
// until the test writes one, which is what CO2.5 needs to move a partition
// while the process stands in the window. Neither mode involves a clock.
//
// "pause" with no stdin is its own exit, 98, and not a pass. Measured with the
// design as first written: run with stdin closed, the way docker and systemd
// start a service, ReadString returned EOF at once, the process printed
// nine-fault-reached, committed 27 offsets and left with exit 0. A test that
// had read the line would believe the process stood in the window while it
// had already left, which is the failure the pause mode exists to prevent.
const faultInjection = true

const (
	exitAtFault   = 97
	exitNoRelease = 98
)

func faultOptions() []consumer.Option[envelope] {
	return []consumer.Option[envelope]{consumer.WithCommitHook[envelope](pauseOrCrash)}
}

func pauseOrCrash(_ context.Context, _ []*kgo.Record) error {
	const point = "AFTER_DB_COMMIT"
	switch os.Getenv("NINE_FAULT_" + point) {
	case "exit":
		os.Exit(exitAtFault)
	case "pause":
		fmt.Println("nine-fault-reached: " + point)
		if _, err := bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
			fmt.Fprintln(os.Stderr, "nine-fault: stdin ended before a release line: "+err.Error())
			os.Exit(exitNoRelease)
		}
	}
	return nil
}
