package breaker

import (
	"context"
	"errors"
	"time"

	"github.com/barnowlsnest/go-circuitbreakerlib/internal/breakercore"
)

const (
	defaultOpenDuration     = 30 * time.Second
	defaultProbeTimeout     = 60 * time.Second
	defaultHalfOpenMaxCalls = 1
	defaultWindowSize       = 10 * time.Second
	defaultFailureRate      = 0.5
	defaultMinCalls         = 20
	defaultConsecutive      = 5
)

// Classifier turns a call's result into an Outcome. It receives the caller's
// context so that the caller giving up (context.Canceled) can be told apart
// from the dependency failing.
type Classifier = breakercore.Classifier

// DefaultClassifier ignores calls the caller canceled, counts nil as
// success, and counts everything else, including deadline errors from
// either side, as failure. A hung dependency produces nothing but deadline
// errors, so they must trip the breaker.
func DefaultClassifier(ctx context.Context, err error) Outcome {
	return breakercore.DefaultClassifier(ctx, err)
}

// config is the resolved breaker settings after options and defaults.
type config struct {
	Events            Events
	Window            WindowSpec
	Backoff           Backoff
	Clock             func() time.Time
	Classifier        Classifier
	ReopenEarly       func() bool
	Name              string
	Policies          []Policy
	SlowCallDuration  time.Duration
	OpenDuration      time.Duration
	ProbeTimeout      time.Duration
	HalfOpenMaxCalls  uint32
	HalfOpenSuccesses uint32
}

func (c *config) withDefaults() (config, error) {
	if err := c.validate(); err != nil {
		return *c, err
	}
	out := *c
	out.applyDefaults()
	return out, nil
}

func (c *config) validate() error {
	if c.Name == "" {
		return errors.Join(ErrInvalidConfig, ErrEmptyName)
	}
	if c.SlowCallDuration < 0 || c.OpenDuration < 0 || c.ProbeTimeout < 0 {
		return errors.Join(ErrInvalidConfig, ErrNegativeDuration)
	}
	if c.Window != nil {
		if err := c.Window.validate(); err != nil {
			return err
		}
	}
	for _, p := range c.Policies {
		if err := validatePolicy(p); err != nil {
			return errors.Join(ErrInvalidConfig, err)
		}
	}
	return nil
}

func (c *config) applyDefaults() {
	if c.Window == nil {
		c.Window = SlidingTimeWindow{Size: defaultWindowSize, Buckets: defaultTimeBuckets}
	}
	if len(c.Policies) == 0 {
		c.Policies = []Policy{
			FailureRate{Threshold: defaultFailureRate, MinCalls: defaultMinCalls},
			ConsecutiveFailures{Count: defaultConsecutive},
		}
	}
	if c.OpenDuration == 0 {
		c.OpenDuration = defaultOpenDuration
	}
	if c.ProbeTimeout == 0 {
		c.ProbeTimeout = defaultProbeTimeout
	}
	if c.HalfOpenMaxCalls == 0 {
		c.HalfOpenMaxCalls = defaultHalfOpenMaxCalls
	}
	if c.HalfOpenSuccesses == 0 {
		c.HalfOpenSuccesses = c.HalfOpenMaxCalls
	}
	if c.Classifier == nil {
		c.Classifier = DefaultClassifier
	}
	if c.Events == nil {
		c.Events = NewNoopEvents()
	}
	if c.Clock == nil {
		c.Clock = time.Now
	}
}
