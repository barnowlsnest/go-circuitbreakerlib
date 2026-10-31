package breaker

import (
	"errors"
	"math"
	"math/rand/v2"
	"time"
)

// Backoff decides how long a breaker stays Open for a given attempt.
type Backoff interface {
	Delay(attempt int) time.Duration
}

// ExponentialBackoff doubles Base each attempt, optionally capped by Max.
type ExponentialBackoff struct {
	Base   time.Duration
	Max    time.Duration
	Jitter bool
}

// maxJitterDelay is MaxInt64-1 so Int64N(d+1) cannot overflow.
const maxJitterDelay = time.Duration(math.MaxInt64 - 1)

func (e ExponentialBackoff) validate() error {
	if e.Base <= 0 {
		return errors.Join(ErrInvalidConfig, ErrInvalidBackoff)
	}
	if e.Max > 0 && e.Max < e.Base {
		return errors.Join(ErrInvalidConfig, ErrInvalidBackoff)
	}
	return nil
}

// Delay implements Backoff.
func (e ExponentialBackoff) Delay(attempt int) time.Duration {
	if attempt < 0 || e.Base <= 0 {
		return 0
	}
	d := time.Duration(float64(e.Base) * math.Pow(2, float64(attempt)))
	if e.Max > 0 && d > e.Max {
		d = e.Max
	}
	if d < 0 || d > maxJitterDelay {
		d = maxJitterDelay
	}
	if e.Jitter && d > 0 {
		d = time.Duration(rand.Int64N(int64(d) + 1))
	}
	return d
}
