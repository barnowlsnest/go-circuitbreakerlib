package registry

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

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
	start   time.Time
	m       sync.Map
	entries []*entry
	options
	mu sync.Mutex
}

// newDefaultRand seeds eviction sampling for one registry. Sampling picks
// which entries to inspect; it has no security role.
func newDefaultRand(seed time.Time) *rand.Rand {
	return rand.New(rand.NewPCG(uint64(seed.UnixNano()), 0))
}

// New returns a registry that builds breakers with WithBreaker options. It
// returns ErrInvalidOption on a bad option value.
func New(opts ...Option) (*Registry, error) {
	o := options{
		clock:        time.Now,
		onEvict:      func(string, *breaker.Breaker) {},
		evictSamples: DefaultEvictSamples,
		granularity:  DefaultAccessGranularity,
	}
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	start := o.clock()
	if o.rand == nil {
		o.rand = newDefaultRand(start)
	}
	return &Registry{start: start, options: o}, nil
}

// Get returns the breaker for key, creating it on first use. Breaker options
// that fail validation return the error on every call and store nothing.
func (r *Registry) Get(key string) (*breaker.Breaker, error) {
	if e, ok := r.load(key); ok {
		r.touch(e)
		return e.b, nil
	}
	return r.create(key)
}

// Lookup returns the breaker for key without creating one. It does not
// count as an access.
func (r *Registry) Lookup(key string) (*breaker.Breaker, bool) {
	e, ok := r.load(key)
	if !ok {
		return nil, false
	}
	return e.b, true
}

// Remove drops key and reports whether it was present. OnEvict runs after
// the removal.
func (r *Registry) Remove(key string) bool {
	r.mu.Lock()
	v, ok := r.m.LoadAndDelete(key)
	if ok {
		r.unlink(v.(*entry))
	}
	r.mu.Unlock()
	if !ok {
		return false
	}
	r.onEvict(key, v.(*entry).b)
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

func (r *Registry) load(key string) (*entry, bool) {
	v, ok := r.m.Load(key)
	if !ok {
		return nil, false
	}
	return v.(*entry), true
}

// elapsed is the time since the registry was created, so access times
// compare by the clock's monotonic reading rather than the wall clock.
func (r *Registry) elapsed() int64 { return r.clock().Sub(r.start).Nanoseconds() }

// touch stores the access time only when the stored one is older than the
// granularity, so a hot key does not bounce a cache line between cores.
func (r *Registry) touch(e *entry) {
	now := r.elapsed()
	if time.Duration(now-e.lastAccess.Load()) >= r.granularity {
		e.lastAccess.Store(now)
	}
}

func (r *Registry) create(key string) (*breaker.Breaker, error) {
	e, victim, err := r.createLocked(key)
	if err != nil {
		return nil, err
	}
	if victim != nil {
		r.onEvict(victim.key, victim.b)
	}
	return e.b, nil
}

// createLocked returns the entry for key, building it if no other caller did
// first, and the entry it evicted to make room, if any.
func (r *Registry) createLocked(key string) (e, victim *entry, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.load(key); ok {
		r.touch(e)
		return e, nil, nil
	}
	opts := make([]breaker.Option, 0, len(r.breakerOpts)+2)
	opts = append(opts, r.breakerOpts...)
	opts = append(opts, breaker.WithName(key))
	if r.events != nil {
		opts = append(opts, breaker.FillEvents(r.events))
	}
	b, err := breaker.New(opts...)
	if err != nil {
		return nil, nil, err
	}
	if r.maxEntries > 0 && len(r.entries) >= r.maxEntries {
		victim = r.pickVictim()
		r.m.Delete(victim.key)
		r.unlink(victim)
	}
	e = &entry{b: b, key: key, index: len(r.entries)}
	e.lastAccess.Store(r.elapsed())
	r.entries = append(r.entries, e)
	r.m.Store(key, e)
	return e, victim, nil
}

// pickVictim samples entries and returns the least recently used Closed one,
// or the least recently used of all samples when none is Closed. Called
// under mu with at least one entry.
func (r *Registry) pickVictim() *entry {
	n := len(r.entries)
	var oldest, oldestClosed *entry
	var oldestAt, oldestClosedAt int64
	consider := func(e *entry) {
		at := e.lastAccess.Load()
		if oldest == nil || at < oldestAt {
			oldest, oldestAt = e, at
		}
		if (oldestClosed == nil || at < oldestClosedAt) && e.b.State() == breaker.StateClosed {
			oldestClosed, oldestClosedAt = e, at
		}
	}
	if r.evictSamples >= n {
		for _, e := range r.entries {
			consider(e)
		}
	} else {
		for range r.evictSamples {
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
