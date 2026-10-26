package breaker

import (
	"errors"
	"time"
)

// SlidingCountWindow slides over the last Size calls. Intended for low-QPS
// keys; the default window is time-based.
type SlidingCountWindow struct {
	Size int
}

func (w SlidingCountWindow) validate() error {
	if w.Size <= 0 {
		return errors.Join(ErrInvalidConfig, ErrWindowSize)
	}
	return nil
}

func (w SlidingCountWindow) newWindow() window {
	return &countWindow{slots: make([]slot, w.Size)}
}

type slot struct {
	outcome Outcome
	slow    bool
	used    bool
}

type countWindow struct {
	slots []slot
	run   consecutive
	sum   Counts
	head  int
}

func (w *countWindow) record(_ time.Time, outcome Outcome, slow bool) {
	old := w.slots[w.head]
	if old.used {
		w.sum.Total--
		if old.outcome == OutcomeFailure {
			w.sum.Failures--
		} else {
			w.sum.Successes--
		}
		if old.slow {
			w.sum.Slow--
		}
	}
	w.slots[w.head] = slot{outcome: outcome, slow: slow, used: true}
	w.head = (w.head + 1) % len(w.slots)
	w.sum.Total++
	if outcome == OutcomeFailure {
		w.sum.Failures++
	} else {
		w.sum.Successes++
	}
	if slow {
		w.sum.Slow++
	}
	w.run.record(outcome)
}

func (w *countWindow) counts(_ time.Time) Counts {
	c := w.sum
	c.ConsecutiveFailures = w.run.failures
	c.ConsecutiveSuccesses = w.run.successes
	return c
}

func (w *countWindow) reset() {
	clear(w.slots)
	w.sum = Counts{}
	w.head = 0
	w.run.reset()
}
