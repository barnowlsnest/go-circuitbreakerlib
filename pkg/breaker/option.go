package breaker

import (
	"fmt"
	"time"

	"github.com/barnowlsnest/go-circuitbreakerlib/internal/breakercore"
)

// Option configures a Breaker. An option with an invalid value makes New
// return an error that matches ErrInvalidConfig or ErrInvalidOption.
type Option = breakercore.Option

// WithName sets the breaker name used in events and logs. name must be
// non-empty.
func WithName(name string) Option {
	return breakercore.WithName(name)
}

// WithPolicies replaces the trip policies.
func WithPolicies(policies ...Policy) Option {
	return breakercore.WithPolicies(policies...)
}

// WithWindow sets the sliding window. The default is
// SlidingTimeWindow{10s, 10}.
func WithWindow(w WindowSpec) Option {
	return breakercore.WithWindow(w)
}

// WithBackoff sets the open-state delay.
func WithBackoff(b Backoff) Option {
	switch eb := b.(type) {
	case nil:
		return breakercore.WithBackoff(b)
	case ExponentialBackoff:
		if err := eb.validate(); err != nil {
			return breakercore.Invalid(err)
		}
	case *ExponentialBackoff:
		if eb == nil {
			return breakercore.Invalid(fmt.Errorf("%w: nil backoff", ErrInvalidOption))
		}
		if err := eb.validate(); err != nil {
			return breakercore.Invalid(err)
		}
	}
	return breakercore.WithBackoff(b)
}

// WithEvents sets the event sink. The default is NoopEvents.
func WithEvents(ev Events) Option {
	return breakercore.WithEvents(ev)
}

// WithClassifier sets how call results become Outcomes. The default is
// DefaultClassifier.
func WithClassifier(fn Classifier) Option {
	return breakercore.WithClassifier(fn)
}

// WithClock sets the time source. The default is time.Now.
func WithClock(fn func() time.Time) Option {
	return breakercore.WithClock(fn)
}

// WithReopenEarly sets a predicate that may move Open to HalfOpen before
// the backoff elapses. nil clears it.
func WithReopenEarly(fn func() bool) Option {
	return breakercore.WithReopenEarly(fn)
}

// WithSlowCallDuration marks calls slower than d as slow. 0 disables.
func WithSlowCallDuration(d time.Duration) Option {
	return breakercore.WithSlowCallDuration(d)
}

// WithOpenDuration sets the default open delay when Backoff is unset. The
// default is 30s.
func WithOpenDuration(d time.Duration) Option {
	return breakercore.WithOpenDuration(d)
}

// WithProbeTimeout sets how long a HalfOpen probe may run. The default is
// 60s.
func WithProbeTimeout(d time.Duration) Option {
	return breakercore.WithProbeTimeout(d)
}

// WithHalfOpenMaxCalls sets how many probes HalfOpen admits. The default is
// 1.
func WithHalfOpenMaxCalls(n uint32) Option {
	return breakercore.WithHalfOpenMaxCalls(n)
}

// WithHalfOpenSuccesses sets how many probe successes close the breaker.
// The default equals HalfOpenMaxCalls.
func WithHalfOpenSuccesses(n uint32) Option {
	return breakercore.WithHalfOpenSuccesses(n)
}

func configFromCore(o *breakercore.Config) (config, error) {
	var window WindowSpec
	if o.Window != nil {
		var ok bool
		window, ok = o.Window.(WindowSpec)
		if !ok {
			return config{}, fmt.Errorf("%w: invalid window", ErrInvalidOption)
		}
	}
	cfg := config{
		Events:            o.Events,
		Window:            window,
		Backoff:           o.Backoff,
		Clock:             o.Clock,
		Classifier:        o.Classifier,
		ReopenEarly:       o.ReopenEarly,
		Name:              o.Name,
		Policies:          o.Policies,
		SlowCallDuration:  o.SlowCallDuration,
		OpenDuration:      o.OpenDuration,
		ProbeTimeout:      o.ProbeTimeout,
		HalfOpenMaxCalls:  o.HalfOpenMaxCalls,
		HalfOpenSuccesses: o.HalfOpenSuccesses,
	}
	return cfg, nil
}
