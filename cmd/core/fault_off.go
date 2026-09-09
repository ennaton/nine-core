//go:build !faultinject

package main

import "github.com/ennaton/nine-core/internal/consumer"

// The shipped binary. faultOptions adds nothing, the constant below folds the
// branch away, and the name of the environment variable that would fire a
// fault does not appear in the binary: the ci job builds without the tag and
// greps for it. See docs/artifacts/2026-09-08-the-seam-the-crash-tests-need.md.
const faultInjection = false

func faultOptions() []consumer.Option[envelope] { return nil }
