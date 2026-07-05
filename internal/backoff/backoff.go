// Package backoff computes retry delays: exponential growth with full jitter,
// the pattern recommended by the AWS architecture blog. Jitter prevents a
// thundering herd when many runs fail at once (e.g. a downstream outage).
package backoff

import (
	"math/rand/v2"
	"time"
)

const (
	base = 5 * time.Second
	max  = 15 * time.Minute
)

// Delay returns the wait before retry attempt n (1-based: the delay applied
// after the n-th failed attempt). Grows 5s, 10s, 20s, ... capped at 15m, then
// a uniformly random point in [delay/2, delay] is chosen.
func Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := base << (attempt - 1)
	if d > max || d <= 0 { // d <= 0 guards shift overflow
		d = max
	}
	half := d / 2
	return half + rand.N(half+1)
}
