package composite

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

const (
	// MaxDepth is the longest path a Lease can hold, counted in hops from
	// the root composite to the leaf. A root with leaves uses 2.
	MaxDepth = 4
	// DefaultScanInterval is the minimum age of the cached open-children
	// count before a Record or Pick recounts.
	DefaultScanInterval = 10 * time.Millisecond

	defaultTripRatio    = 0.5
	defaultTripRate     = 0.5
	defaultTripMinCalls = 50
	tripPolicyName      = "composite-trip"
	hookClassifier      = "classifier"
	hookOnReject        = "on-reject"
)

// Config configures a Composite.
type Config struct {
	Events        breaker.Events
	ChildTemplate func(id string) breaker.Config
	Trip          TripFunc
	Name          string
	Parent        breaker.Config
	ScanInterval  time.Duration
}

// Composite is a tree of breakers behind one parent breaker. It is safe for
// concurrent use. It implements Child, so composites nest.
type Composite struct {
	parent        *breaker.Breaker
	parentEvents  breaker.Events
	template      func(id string) breaker.Config
	trip          TripFunc
	events        breaker.Events
	clock         func() time.Time
	classifier    breaker.Classifier
	children      atomic.Pointer[[]Child]
	tripAggregate atomic.Pointer[breaker.Counts]
	owner         atomic.Pointer[Composite]
	scan          atomic.Pointer[scanResult]
	noHealthy     [breaker.StateHalfOpen + 1]*breaker.RejectedError
	mu            sync.Mutex
	rr            atomic.Int64
	hookPanics    atomic.Uint64
	scanInterval  time.Duration
}

// New builds a composite with no children. It returns ErrInvalidConfig when
// Parent.Policies is set or ScanInterval is negative, and the parent
// breaker's own validation errors otherwise.
//
//nolint:gocritic // Config is passed by value like breaker.Config.
func New(cfg Config) (*Composite, error) {
	if len(cfg.Parent.Policies) > 0 {
		return nil, fmt.Errorf("%w: Parent.Policies must be empty, Trip is the parent policy", ErrInvalidConfig)
	}
	if cfg.ScanInterval < 0 {
		return nil, fmt.Errorf("%w: ScanInterval %v", ErrInvalidConfig, cfg.ScanInterval)
	}
	c := &Composite{
		template:     cfg.ChildTemplate,
		trip:         cfg.Trip,
		events:       cfg.Events,
		scanInterval: cfg.ScanInterval,
	}
	if c.trip == nil {
		c.trip = AllOf(OpenChildrenRatio(defaultTripRatio), AggregateFailureRate(defaultTripRate, defaultTripMinCalls))
	}
	if c.events == nil {
		c.events = breaker.NewNoopEvents()
	}
	if c.scanInterval == 0 {
		c.scanInterval = DefaultScanInterval
	}
	parentCfg := cfg.Parent
	if cfg.Name != "" {
		parentCfg.Name = cfg.Name
	}
	if parentCfg.Events == nil {
		parentCfg.Events = c.events
	}
	if parentCfg.Clock == nil {
		parentCfg.Clock = time.Now
	}
	if parentCfg.Classifier == nil {
		parentCfg.Classifier = breaker.DefaultClassifier
	}
	c.parentEvents = parentCfg.Events
	parentCfg.Events = &parentObserver{c: c, next: parentCfg.Events}
	parentCfg.Policies = []breaker.Policy{tripPolicy{c: c}}
	parentCfg.ReopenEarly = c.reopenEarly
	parent, err := breaker.New(parentCfg)
	if err != nil {
		return nil, err
	}
	c.parent = parent
	c.clock = parentCfg.Clock
	c.classifier = parentCfg.Classifier
	empty := make([]Child, 0)
	c.children.Store(&empty)
	for i := range c.noHealthy {
		c.noHealthy[i] = &breaker.RejectedError{Err: ErrNoHealthyChildren, Name: parentCfg.Name, State: breaker.State(i)}
	}
	return c, nil
}

// Name returns the parent's name.
func (c *Composite) Name() string { return c.parent.Name() }

// State returns the parent's lazy state.
func (c *Composite) State() breaker.State { return c.parent.State() }

// Counts returns the parent's live window counts.
func (c *Composite) Counts() breaker.Counts { return c.parent.Counts() }

// Snapshot returns the parent's snapshot.
func (c *Composite) Snapshot() breaker.Snapshot { return c.parent.Snapshot() }

// Allow asks the parent alone for a permit, refreshing the open-children
// count first so ReopenEarly sees current children. Use Pick to reach a
// leaf.
func (c *Composite) Allow() (breaker.Permit, error) {
	c.refreshScan()
	return c.parent.Allow()
}

// Record settles a parent permit taken by Allow, refreshing the
// open-children count first so the trip decision sees current children.
func (c *Composite) Record(p breaker.Permit, outcome breaker.Outcome, elapsed time.Duration) {
	c.refreshScan()
	c.parent.Record(p, outcome, elapsed)
}

// ForceOpen opens the parent until ForceClose or Reset.
func (c *Composite) ForceOpen() { c.parent.ForceOpen() }

// ForceClose closes the parent until ForceOpen or Reset.
func (c *Composite) ForceClose() { c.parent.ForceClose() }

// Reset returns the parent to Closed under normal control.
func (c *Composite) Reset() { c.parent.Reset() }

// HookPanics returns how many Events hooks have panicked: the parent
// breaker's own count plus hooks the composite fires itself.
func (c *Composite) HookPanics() uint64 { return c.parent.HookPanics() + c.hookPanics.Load() }

// reopenEarly runs lock-free on every parent Allow while Open. It reopens
// when the trip condition no longer holds against the frozen aggregate.
func (c *Composite) reopenEarly() bool {
	frozen := c.tripAggregate.Load()
	if frozen == nil {
		return false
	}
	open, total := c.scanned()
	return !c.trip(open, total, *frozen)
}

// tripPolicy adapts TripFunc to breaker.Policy. In Closed it sees the live
// window; on the closing HalfOpen record it sees the aggregate frozen at
// trip time, so rate terms stay meaningful after the window drained.
type tripPolicy struct {
	c *Composite
}

func (p tripPolicy) ShouldTrip(counts breaker.Counts) bool {
	agg := counts
	if p.c.parent.State() == breaker.StateHalfOpen {
		if frozen := p.c.tripAggregate.Load(); frozen != nil {
			agg = *frozen
		}
	}
	open, total := p.c.scanned()
	if !p.c.trip(open, total, agg) {
		return false
	}
	frozen := new(breaker.Counts)
	*frozen = agg
	p.c.tripAggregate.Store(frozen)
	return true
}

func (tripPolicy) Name() string { return tripPolicyName }

// parentObserver sits in front of the user's Events on the parent only and
// drops the frozen aggregate when the parent closes.
type parentObserver struct {
	c    *Composite
	next breaker.Events
}

func (o *parentObserver) OnStateChange(ev breaker.StateChangeEvent) {
	if ev.To == breaker.StateClosed {
		o.c.tripAggregate.Store(nil)
	}
	o.next.OnStateChange(ev)
}

func (o *parentObserver) OnCall(ev breaker.CallEvent)     { o.next.OnCall(ev) }
func (o *parentObserver) OnReject(ev breaker.RejectEvent) { o.next.OnReject(ev) }
func (o *parentObserver) OnHookPanic(name string, recovered any) {
	o.next.OnHookPanic(name, recovered)
}

// rejectNoHealthy fires OnReject on the parent's events for a Pick that
// found no admitting child, under recover like every breaker hook.
func (c *Composite) rejectNoHealthy(now time.Time, state breaker.State) {
	defer c.recoverHook(c.parentEvents, hookOnReject)
	c.parentEvents.OnReject(breaker.RejectEvent{At: now, Err: ErrNoHealthyChildren, Name: c.parent.Name(), State: state})
}

// recoverHook is deferred around a hook the composite fires itself. It
// counts the panic and reports it through OnHookPanic, which may panic too
// without escaping.
func (c *Composite) recoverHook(events breaker.Events, name string) {
	if r := recover(); r != nil {
		c.hookPanics.Add(1)
		c.fireHookPanic(events, name, r)
	}
}

func (c *Composite) fireHookPanic(events breaker.Events, name string, recovered any) {
	defer func() {
		if r := recover(); r != nil {
			c.hookPanics.Add(1)
		}
	}()
	events.OnHookPanic(name, recovered)
}
