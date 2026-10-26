package registry

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

// Factory builds the Config for a key. A Config with an empty Name gets the
// key as its name; a nil Events gets the registry's WithEvents value.
type Factory func(key string) breaker.Config

// entry is one registered breaker. index is its position in Registry.entries
// and is only read or written under Registry.mu.
type entry struct {
	b          *breaker.Breaker
	key        string
	lastAccess atomic.Int64
	index      int
}

// Registry maps keys to breakers. It is safe for concurrent use.
type Registry struct {
	m           sync.Map
	factory     Factory
	clock       func() time.Time
	onEvict     func(key string, b *breaker.Breaker)
	events      breaker.Events
	rand        *rand.Rand
	entries     []*entry
	mu          sync.Mutex
	maxEntries  int
	samples     int
	granularity int64
}

// defaultRand seeds eviction sampling once per process. Sampling picks
// which entries to inspect; it has no security role.
//
//nolint:gosec
var defaultRand = rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano())))

// New returns a registry that builds breakers with factory. It panics with
// ErrNilFactory when factory is nil and with ErrInvalidOption on a bad
// option value.
func New(factory Factory, opts ...Option) *Registry {
	if factory == nil {
		panic(ErrNilFactory)
	}
	o := options{
		clock:        time.Now,
		rand:         defaultRand,
		evictSamples: DefaultEvictSamples,
		granularity:  DefaultAccessGranularity,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return &Registry{
		factory:     factory,
		clock:       o.clock,
		onEvict:     o.onEvict,
		events:      o.events,
		rand:        o.rand,
		maxEntries:  o.maxEntries,
		samples:     o.evictSamples,
		granularity: int64(o.granularity),
	}
}

// Get returns the breaker for key, creating it on first use. A factory
// Config that fails validation returns the error on every call and stores
// nothing.
func (r *Registry) Get(key string) (*breaker.Breaker, error) {
	if v, ok := r.m.Load(key); ok {
		e := v.(*entry)
		r.touch(e)
		return e.b, nil
	}
	return r.create(key)
}

// Lookup returns the breaker for key without creating one. It does not
// count as an access.
func (r *Registry) Lookup(key string) (*breaker.Breaker, bool) {
	v, ok := r.m.Load(key)
	if !ok {
		return nil, false
	}
	return v.(*entry).b, true
}

// Remove drops key and reports whether it was present. OnEvict runs after
// the removal.
func (r *Registry) Remove(key string) bool {
	r.mu.Lock()
	v, ok := r.m.LoadAndDelete(key)
	if !ok {
		r.mu.Unlock()
		return false
	}
	e := v.(*entry)
	r.unlink(e)
	r.mu.Unlock()
	r.evicted(key, e.b)
	return true
}

// Range calls fn for every breaker until fn returns false. The order is
// unspecified and the set may change while Range runs.
func (r *Registry) Range(fn func(key string, b *breaker.Breaker) bool) {
	r.m.Range(func(k, v any) bool {
		return fn(k.(string), v.(*entry).b)
	})
}

// Len returns the number of registered breakers.
func (r *Registry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// touch stores the access time only when the stored one is older than the
// granularity, so a hot key does not bounce a cache line between cores.
func (r *Registry) touch(e *entry) {
	now := r.clock().UnixNano()
	if now-e.lastAccess.Load() >= r.granularity {
		e.lastAccess.Store(now)
	}
}

func (r *Registry) create(key string) (*breaker.Breaker, error) {
	r.mu.Lock()
	if v, ok := r.m.Load(key); ok {
		r.mu.Unlock()
		e := v.(*entry)
		r.touch(e)
		return e.b, nil
	}
	cfg := r.factory(key)
	if cfg.Name == "" {
		cfg.Name = key
	}
	if cfg.Events == nil && r.events != nil {
		cfg.Events = r.events
	}
	b, err := breaker.New(cfg)
	if err != nil {
		r.mu.Unlock()
		return nil, err
	}
	var victim *entry
	if r.maxEntries > 0 && len(r.entries) >= r.maxEntries {
		victim = r.pickVictim()
		r.m.Delete(victim.key)
		r.unlink(victim)
	}
	e := &entry{b: b, key: key, index: len(r.entries)}
	e.lastAccess.Store(r.clock().UnixNano())
	r.entries = append(r.entries, e)
	r.m.Store(key, e)
	r.mu.Unlock()
	if victim != nil {
		r.evicted(victim.key, victim.b)
	}
	return b, nil
}

// pickVictim samples entries and returns the least recently used Closed one,
// or the least recently used of all samples when none is Closed. Called
// under mu with at least one entry.
func (r *Registry) pickVictim() *entry {
	n := len(r.entries)
	var oldest, oldestClosed *entry
	consider := func(e *entry) {
		if oldest == nil || e.lastAccess.Load() < oldest.lastAccess.Load() {
			oldest = e
		}
		if e.b.State() != breaker.StateClosed {
			return
		}
		if oldestClosed == nil || e.lastAccess.Load() < oldestClosed.lastAccess.Load() {
			oldestClosed = e
		}
	}
	if r.samples >= n {
		for _, e := range r.entries {
			consider(e)
		}
	} else {
		for range r.samples {
			consider(r.entries[r.rand.IntN(n)])
		}
	}
	if oldestClosed != nil {
		return oldestClosed
	}
	return oldest
}

// unlink swap-removes e from entries in O(1). Called under mu.
func (r *Registry) unlink(e *entry) {
	last := len(r.entries) - 1
	moved := r.entries[last]
	r.entries[e.index] = moved
	moved.index = e.index
	r.entries[last] = nil
	r.entries = r.entries[:last]
}

func (r *Registry) evicted(key string, b *breaker.Breaker) {
	if r.onEvict != nil {
		r.onEvict(key, b)
	}
}
