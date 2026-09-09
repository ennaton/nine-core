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
const faultInjection = true

const exitAtFault = 97

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
		_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
	}
	return nil
}
