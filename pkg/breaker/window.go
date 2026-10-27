package breaker

import (
	"errors"
	"time"
)

var (
	// ErrWindowSize is joined with ErrInvalidConfig when a window size is not positive.
	ErrWindowSize = errors.New("window size must be positive")
	// ErrWindowBuckets is joined with ErrInvalidConfig when a bucket count is negative.
	ErrWindowBuckets = errors.New("window buckets must not be negative")
)

// WindowSpec describes a sliding window. Config.Window holds one;
// SlidingCountWindow and SlidingTimeWindow are the two implementations.
type WindowSpec interface {
	validate() error
	newWindow() window
}

// window is the runtime instance behind a WindowSpec. Implementations are
// not safe for concurrent use; the breaker serializes access.
type window interface {
	record(now time.Time, outcome Outcome, slow bool)
	counts(now time.Time) Counts
	reset()
}

// consecutive tracks the run of identical outcomes shared by both windows.
type consecutive struct {
	failures  uint32
	successes uint32
}

func (c *consecutive) record(outcome Outcome) {
	if outcome == OutcomeFailure {
		c.failures++
		c.successes = 0
		return
	}
	c.successes++
	c.failures = 0
}

func (c *consecutive) reset() {
	*c = consecutive{}
}
