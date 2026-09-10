package retry

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
)

// The broker is kfake, in process, so this file needs nothing running.
func brokerForTest(t *testing.T) []string {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "marks"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c.ListenAddrs()
}

func ctxFor(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}
