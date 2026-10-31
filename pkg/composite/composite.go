package composite

import (
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

// Composite is a tree of breakers behind one parent breaker. It is safe for
// concurrent use. It implements Child, so composites nest.
type Composite struct {
	parent        *breaker.Breaker
	parentEvents  breaker.Events
	trip          TripFunc
	events        breaker.Events
	clock         func() time.Time
	classifier    breaker.Classifier
	children      atomic.Pointer[[]Child]
	tripAggregate atomic.Pointer[breaker.Counts]
	owner         atomic.Pointer[Composite]
	scan          atomic.Pointer[scanResult]
	noHealthy     [breaker.StateHalfOpen + 1]*breaker.RejectedError
	childOpts     []breaker.Option
	rr            atomic.Int64
	hookPanics    atomic.Uint64
	scanInterval  time.Duration
}

// New builds a composite with no children. It returns ErrInvalidConfig when
// parent options set Policies or ScanInterval is negative, and the parent
// breaker's own validation errors otherwise.
func New(opts ...Option) (*Composite, error) {
	var o options
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	c := &Composite{
		trip:         o.trip,
		events:       o.events,
		childOpts:    o.childOpts,
		scanInterval: o.scanInterval,
	}
	if c.trip == nil {
		c.trip = allOf(openChildrenRatio(defaultTripRatio), aggregateFailureRate(defaultTripRate, defaultTripMinCalls))
	}
	if c.events == nil {
		c.events = breaker.NewNoopEvents()
	}
	if !o.scanSet {
		c.scanInterval = DefaultScanInterval
	}
	base := make([]breaker.Option, 0, len(o.parentOpts)+2)
	base = append(base, o.parentOpts...)
	if o.nameSet {
		base = append(base, breaker.WithName(o.name))
	}
	base = append(base, breaker.FillEvents(c.events))
	settings, err := breaker.ResolveSettings(base...)
	if err != nil {
		return nil, err
	}
	c.clock = settings.Clock
	c.classifier = settings.Classifier
	c.parentEvents = settings.Events
	parentOpts := make([]breaker.Option, 0, len(base)+3)
	parentOpts = append(parentOpts, base...)
	parentOpts = append(parentOpts,
		breaker.WithEvents(&parentObserver{Events: settings.Events, c: c}),
		breaker.WithPolicies(tripPolicy{c: c}),
		breaker.WithReopenEarly(c.reopenEarly),
	)
	parent, err := breaker.New(parentOpts...)
	if err != nil {
		return nil, err
	}
	c.parent = parent
	empty := make([]Child, 0)
	c.children.Store(&empty)
	c.rescan(c.clock())
	for i := range c.noHealthy {
		c.noHealthy[i] = &breaker.RejectedError{Err: ErrNoHealthyChildren, Name: parent.Name(), State: breaker.State(i)}
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
func (c *Composite) Record(p breaker.Permit, outcome breaker.Outcome, elapsed time.Duration, err error) {
	c.refreshScan()
	c.parent.Record(p, outcome, elapsed, err)
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
	// Total child outage fail-fast: every child is Open.
	if total == 0 || (open < total && !p.c.trip(open, total, agg)) {
		return false
	}
	return true
}

func (tripPolicy) Name() string { return tripPolicyName }

// parentObserver sits in front of the user's Events on the parent only.
type parentObserver struct {
	breaker.Events
	c *Composite
}

func (o *parentObserver) OnStateChange(ev breaker.StateChangeEvent) {
	if ev.To == breaker.StateOpen && ev.Reason == tripPolicyName && o.c.tripAggregate.Load() == nil {
		frozen := new(breaker.Counts)
		*frozen = o.c.parent.Counts()
		o.c.tripAggregate.Store(frozen)
	}
	if ev.To == breaker.StateClosed {
		o.c.tripAggregate.Store(nil)
	}
	o.Events.OnStateChange(ev)
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
