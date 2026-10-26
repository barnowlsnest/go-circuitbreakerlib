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
// panic with ErrInvalidOption.
type Option func(*options)

type options struct {
	clock        func() time.Time
	onEvict      func(key string, b *breaker.Breaker)
	events       breaker.Events
	rand         *rand.Rand
	maxEntries   int
	evictSamples int
	granularity  time.Duration
}

// WithEvents sets the Events given to every breaker whose Config.Events is
// nil.
func WithEvents(ev breaker.Events) Option {
	return func(o *options) {
		if ev == nil {
			panic(fmt.Errorf("%w: nil events", ErrInvalidOption))
		}
		o.events = ev
	}
}

// WithMaxEntries bounds the registry. 0 means unbounded.
func WithMaxEntries(n int) Option {
	return func(o *options) {
		if n < 0 {
			panic(fmt.Errorf("%w: max entries %d", ErrInvalidOption, n))
		}
		o.maxEntries = n
	}
}

// WithOnEvict sets a callback that runs after a breaker leaves the
// registry, by eviction or Remove. hooks.LogEvents.Forget is the intended
// callback.
func WithOnEvict(fn func(key string, b *breaker.Breaker)) Option {
	return func(o *options) {
		if fn == nil {
			panic(fmt.Errorf("%w: nil on-evict", ErrInvalidOption))
		}
		o.onEvict = fn
	}
}

// WithEvictSamples sets how many random entries a full registry inspects
// before evicting one. n must be at least 1; the default is 8. A value at or
// above the registry size inspects every entry.
func WithEvictSamples(n int) Option {
	return func(o *options) {
		if n < 1 {
			panic(fmt.Errorf("%w: evict samples %d", ErrInvalidOption, n))
		}
		o.evictSamples = n
	}
}

// WithAccessGranularity sets how stale an entry's access time must be
// before a hit stores a new one. 0 stores on every hit. The default is 1s.
func WithAccessGranularity(d time.Duration) Option {
	return func(o *options) {
		if d < 0 {
			panic(fmt.Errorf("%w: access granularity %v", ErrInvalidOption, d))
		}
		o.granularity = d
	}
}

// WithClock sets the time source for access times. The default is time.Now.
func WithClock(fn func() time.Time) Option {
	return func(o *options) {
		if fn == nil {
			panic(fmt.Errorf("%w: nil clock", ErrInvalidOption))
		}
		o.clock = fn
	}
}

// WithRand sets the generator that picks eviction samples. The registry
// only uses it under its own lock. The default is seeded from the clock at
// package init.
func WithRand(r *rand.Rand) Option {
	return func(o *options) {
		if r == nil {
			panic(fmt.Errorf("%w: nil rand", ErrInvalidOption))
		}
		o.rand = r
	}
}
