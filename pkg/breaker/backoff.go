package breaker

import (
	"math"
	"math/rand/v2"
	"time"
)

// Backoff decides how long a breaker stays Open for a given attempt.
type Backoff interface {
	Delay(attempt int) time.Duration
}

// ExponentialBackoff doubles Base on every attempt, capped at Max. With
// Jitter the result is drawn uniformly from [0, delay].
type ExponentialBackoff struct {
	Base   time.Duration
	Max    time.Duration
	Jitter bool
}

// Delay implements Backoff.
func (e ExponentialBackoff) Delay(attempt int) time.Duration {
	if attempt < 0 || e.Base <= 0 {
		return 0
	}
	d := e.Max
	if scaled := float64(e.Base) * math.Pow(2, float64(attempt)); scaled < float64(e.Max) {
		d = time.Duration(scaled)
	}
	if e.Jitter && d > 0 {
		d = time.Duration(rand.Int64N(int64(d) + 1))
	}
	return d
}
