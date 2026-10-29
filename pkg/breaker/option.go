package breaker

import (
	"errors"
	"fmt"
	"time"
)

// Option configures a Breaker. An option with an invalid value makes New
// return an error that matches ErrInvalidConfig or ErrInvalidOption.
type Option func(*options) error

type options struct {
	events            Events
	window            WindowSpec
	backoff           Backoff
	clock             func() time.Time
	classifier        Classifier
	reopenEarly       func() bool
	name              string
	policies          []Policy
	slowCallDuration  time.Duration
	openDuration      time.Duration
	probeTimeout      time.Duration
	halfOpenMaxCalls  uint32
	halfOpenSuccesses uint32
	policiesSet       bool
}

// WithName sets the breaker name used in events and logs. name must be
// non-empty.
func WithName(name string) Option {
	return func(o *options) error {
		if name == "" {
			return errors.Join(ErrInvalidConfig, ErrEmptyName)
		}
		o.name = name
		return nil
	}
}

// WithPolicies replaces the trip policies. The default is FailureRate{0.5, 20}
// and ConsecutiveFailures{5}. A nil element is an error.
func WithPolicies(policies ...Policy) Option {
	return func(o *options) error {
		for i, p := range policies {
			if p == nil {
				return fmt.Errorf("%w: %w at %d", ErrInvalidConfig, ErrNilPolicy, i)
			}
		}
		o.policies = policies
		o.policiesSet = true
		return nil
	}
}

// WithWindow sets the sliding window. The default is
// SlidingTimeWindow{10s, 10}.
func WithWindow(w WindowSpec) Option {
	return func(o *options) error {
		if w == nil {
			return fmt.Errorf("%w: nil window", ErrInvalidOption)
		}
		o.window = w
		return nil
	}
}

// WithBackoff sets the open-state delay. When unset, ConstantBackoff of
// OpenDuration is used.
func WithBackoff(b Backoff) Option {
	return func(o *options) error {
		if b == nil {
			return fmt.Errorf("%w: nil backoff", ErrInvalidOption)
		}
		o.backoff = b
		return nil
	}
}

// WithEvents sets the event sink. The default is NoopEvents.
func WithEvents(ev Events) Option {
	return func(o *options) error {
		if ev == nil {
			return fmt.Errorf("%w: nil events", ErrInvalidOption)
		}
		o.events = ev
		return nil
	}
}

// FillEvents sets Events only when WithEvents was not used. Registry and
// composite use it to inject a shared Events value.
func FillEvents(ev Events) Option {
	return func(o *options) error {
		if ev == nil {
			return fmt.Errorf("%w: nil events", ErrInvalidOption)
		}
		if o.events == nil {
			o.events = ev
		}
		return nil
	}
}

// WithClassifier sets how call results become Outcomes. The default is
// DefaultClassifier.
func WithClassifier(fn Classifier) Option {
	return func(o *options) error {
		if fn == nil {
			return fmt.Errorf("%w: nil classifier", ErrInvalidOption)
		}
		o.classifier = fn
		return nil
	}
}

// WithClock sets the time source. The default is time.Now.
func WithClock(fn func() time.Time) Option {
	return func(o *options) error {
		if fn == nil {
			return fmt.Errorf("%w: nil clock", ErrInvalidOption)
		}
		o.clock = fn
		return nil
	}
}

// WithReopenEarly sets a predicate that may move Open to HalfOpen before
// the backoff elapses. nil clears it.
func WithReopenEarly(fn func() bool) Option {
	return func(o *options) error {
		o.reopenEarly = fn
		return nil
	}
}

// WithSlowCallDuration marks calls slower than d as slow. 0 disables.
func WithSlowCallDuration(d time.Duration) Option {
	return func(o *options) error {
		if d < 0 {
			return errors.Join(ErrInvalidConfig, ErrNegativeDuration)
		}
		o.slowCallDuration = d
		return nil
	}
}

// WithOpenDuration sets the default open delay when Backoff is unset. The
// default is 30s.
func WithOpenDuration(d time.Duration) Option {
	return func(o *options) error {
		if d < 0 {
			return errors.Join(ErrInvalidConfig, ErrNegativeDuration)
		}
		o.openDuration = d
		return nil
	}
}

// WithProbeTimeout sets how long a HalfOpen probe may run. The default is
// 60s.
func WithProbeTimeout(d time.Duration) Option {
	return func(o *options) error {
		if d < 0 {
			return errors.Join(ErrInvalidConfig, ErrNegativeDuration)
		}
		o.probeTimeout = d
		return nil
	}
}

// WithHalfOpenMaxCalls sets how many probes HalfOpen admits. The default is
// 1.
func WithHalfOpenMaxCalls(n uint32) Option {
	return func(o *options) error {
		o.halfOpenMaxCalls = n
		return nil
	}
}

// WithHalfOpenSuccesses sets how many probe successes close the breaker.
// The default equals HalfOpenMaxCalls.
func WithHalfOpenSuccesses(n uint32) Option {
	return func(o *options) error {
		o.halfOpenSuccesses = n
		return nil
	}
}

func applyOptions(opts ...Option) (options, error) {
	var o options
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return o, err
		}
	}
	return o, nil
}

// ValidateNoPolicies returns an error if opts include WithPolicies. Composite
// uses it so Trip stays the only parent policy.
func ValidateNoPolicies(opts ...Option) error {
	o, err := applyOptions(opts...)
	if err != nil {
		return err
	}
	if o.policiesSet {
		return fmt.Errorf("%w: policies must be empty, Trip is the parent policy", ErrInvalidConfig)
	}
	return nil
}

// Settings is the clock, classifier, events, and name New would use after
// applying opts and defaults. Composite reads them to mirror the parent.
type Settings struct {
	Clock      func() time.Time
	Classifier Classifier
	Events     Events
	Name       string
}

// ResolveSettings applies opts and defaults without constructing a Breaker.
func ResolveSettings(opts ...Option) (Settings, error) {
	o, err := applyOptions(opts...)
	if err != nil {
		return Settings{}, err
	}
	cfg := o.toConfig()
	resolved, err := cfg.withDefaults()
	if err != nil {
		return Settings{}, err
	}
	return Settings{
		Clock:      resolved.Clock,
		Classifier: resolved.Classifier,
		Events:     resolved.Events,
		Name:       resolved.Name,
	}, nil
}

func (o *options) toConfig() config {
	cfg := config{
		Events:            o.events,
		Window:            o.window,
		Backoff:           o.backoff,
		Clock:             o.clock,
		Classifier:        o.classifier,
		ReopenEarly:       o.reopenEarly,
		Name:              o.name,
		SlowCallDuration:  o.slowCallDuration,
		OpenDuration:      o.openDuration,
		ProbeTimeout:      o.probeTimeout,
		HalfOpenMaxCalls:  o.halfOpenMaxCalls,
		HalfOpenSuccesses: o.halfOpenSuccesses,
	}
	if o.policiesSet {
		cfg.Policies = o.policies
	}
	return cfg
}
