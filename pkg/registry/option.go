package registry

import (
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

const (
	// DefaultEvictSamples is the number of entries a full registry inspects
	// before evicting one.
	DefaultEvictSamples = 8
	// DefaultAccessGranularity is how stale an entry's access time must be
	// before a hit stores a new one.
	DefaultAccessGranularity = time.Second
)

// Option configures a Registry. An option with an invalid value makes New
// return ErrInvalidOption.
type Option func(*options) error

type options struct {
	clock        func() time.Time
	onEvict      func(key string, b *breaker.Breaker)
	events       breaker.Events
	rand         *rand.Rand
	breakerOpts  []breaker.Option
	maxEntries   int
	evictSamples int
	granularity  time.Duration
}

// WithBreaker sets the options applied to every breaker Get creates. The
// breaker's name is always the registry key; FillEvents from WithEvents
// still applies when these options leave Events unset.
func WithBreaker(opts ...breaker.Option) Option {
	return func(o *options) error {
		o.breakerOpts = opts
		return nil
	}
}

// WithEvents sets the Events given to every breaker whose Config.Events is
// nil.
func WithEvents(ev breaker.Events) Option {
	return func(o *options) error {
		if ev == nil {
			return fmt.Errorf("%w: nil events", ErrInvalidOption)
		}
		o.events = ev
		return nil
	}
}

// WithMaxEntries bounds the registry. 0 means unbounded.
func WithMaxEntries(n int) Option {
	return func(o *options) error {
		if n < 0 {
			return fmt.Errorf("%w: max entries %d", ErrInvalidOption, n)
		}
		o.maxEntries = n
		return nil
	}
}

// WithOnEvict sets a callback that runs after a breaker leaves the
// registry, by eviction or Remove. hooks.LogEvents.Forget is the intended
// callback.
func WithOnEvict(fn func(key string, b *breaker.Breaker)) Option {
	return func(o *options) error {
		if fn == nil {
			return fmt.Errorf("%w: nil on-evict", ErrInvalidOption)
		}
		o.onEvict = fn
		return nil
	}
}

// WithEvictSamples sets how many random entries a full registry inspects
// before evicting one. n must be at least 1; the default is 8. A value at or
// above the registry size inspects every entry.
func WithEvictSamples(n int) Option {
	return func(o *options) error {
		if n < 1 {
			return fmt.Errorf("%w: evict samples %d", ErrInvalidOption, n)
		}
		o.evictSamples = n
		return nil
	}
}

// WithAccessGranularity sets how stale an entry's access time must be
// before a hit stores a new one. 0 stores on every hit. The default is 1s.
func WithAccessGranularity(d time.Duration) Option {
	return func(o *options) error {
		if d < 0 {
			return fmt.Errorf("%w: access granularity %v", ErrInvalidOption, d)
		}
		o.granularity = d
		return nil
	}
}

// WithClock sets the time source for access times. The default is time.Now.
func WithClock(fn func() time.Time) Option {
	return func(o *options) error {
		if fn == nil {
			return fmt.Errorf("%w: nil clock", ErrInvalidOption)
		}
		o.clock = fn
		return nil
	}
}

// WithRand sets the generator that picks eviction samples. The registry
// only uses it under its own lock. The default is a generator of its own,
// seeded from the clock when the registry is created.
func WithRand(r *rand.Rand) Option {
	return func(o *options) error {
		if r == nil {
			return fmt.Errorf("%w: nil rand", ErrInvalidOption)
		}
		o.rand = r
		return nil
	}
}
