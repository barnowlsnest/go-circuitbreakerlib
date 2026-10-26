package breaker

import (
	"errors"
	"time"
)

const defaultTimeBuckets = 10

// SlidingTimeWindow slides over the last Size of elapsed time, split into
// Buckets fixed-width buckets (default 10). Elapsed time comes from the
// breaker's clock, so the window is unaffected by wall-clock adjustments.
// A bucket leaves the window once its start is Size old, so the window
// always covers exactly Buckets consecutive buckets ending at the current one.
type SlidingTimeWindow struct {
	Size    time.Duration
	Buckets int
}

func (w SlidingTimeWindow) validate() error {
	if w.Size <= 0 {
		return errors.Join(ErrInvalidConfig, ErrWindowSize)
	}
	if w.Buckets < 0 {
		return errors.Join(ErrInvalidConfig, ErrWindowBuckets)
	}
	return nil
}

func (w SlidingTimeWindow) newWindow() window {
	n := w.Buckets
	if n == 0 {
		n = defaultTimeBuckets
	}
	tw := &timeWindow{
		buckets: make([]bucket, n),
		width:   w.Size / time.Duration(n),
	}
	tw.reset()
	return tw
}

const unusedBucket = -1

type bucket struct {
	idx      int64
	total    uint32
	failures uint32
	slow     uint32
}

type timeWindow struct {
	origin  time.Time
	buckets []bucket
	run     consecutive
	width   time.Duration
	started bool
}

func (w *timeWindow) index(now time.Time) int64 {
	if !w.started {
		w.started = true
		w.origin = now
	}
	return int64(now.Sub(w.origin) / w.width)
}

func (w *timeWindow) record(now time.Time, outcome Outcome, slow bool) {
	idx := w.index(now)
	b := &w.buckets[idx%int64(len(w.buckets))]
	if b.idx != idx {
		*b = bucket{idx: idx}
	}
	b.total++
	if outcome == OutcomeFailure {
		b.failures++
	}
	if slow {
		b.slow++
	}
	w.run.record(outcome)
}

func (w *timeWindow) counts(now time.Time) Counts {
	c := Counts{ConsecutiveFailures: w.run.failures, ConsecutiveSuccesses: w.run.successes}
	if !w.started {
		return c
	}
	cur := int64(now.Sub(w.origin) / w.width)
	n := int64(len(w.buckets))
	for i := range w.buckets {
		b := &w.buckets[i]
		if b.idx == unusedBucket || b.idx > cur || cur-b.idx >= n {
			continue
		}
		c.Total += b.total
		c.Failures += b.failures
		c.Slow += b.slow
	}
	c.Successes = c.Total - c.Failures
	return c
}

func (w *timeWindow) reset() {
	for i := range w.buckets {
		w.buckets[i] = bucket{idx: unusedBucket}
	}
	w.started = false
	w.run.reset()
}
