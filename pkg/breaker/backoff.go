package breaker

import (
	"math"
	"math/rand/v2"
	"time"
)

// Backoff returns how long a breaker stays Open on its attempt-th
// consecutive trip (attempt is 0 on the first). The signature matches
// go-asynctasklib's retry.Strategy, so those strategies satisfy it. A
// panicking Delay falls back to Config.OpenDuration and is reported through
// Events.OnHookPanic as "backoff".
type Backoff interface {
	Delay(attempt int) time.Duration
}

// ConstantBackoff returns the same delay for every attempt.
func ConstantBackoff(d time.Duration) Backoff { return constantBackoff(d) }

type constantBackoff time.Duration

func (c constantBackoff) Delay(int) time.Duration { return time.Duration(c) }

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
