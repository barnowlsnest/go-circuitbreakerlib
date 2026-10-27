package breaker

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	stateBits  = 2
	stateMask  = 1<<stateBits - 1
	firstEpoch = 1

	hookPolicy      = "policy"
	hookReopenEarly = "reopen-early"
	hookClassifier  = "classifier"
)

// Breaker is a circuit breaker. Construct with New. All methods are safe for
// concurrent use; Allow in Closed and the Open rejection path are lock-free,
// while HalfOpen probe admission takes the breaker's mutex.
type Breaker struct {
	openedAt         time.Time
	base             time.Time
	win              window
	halfOpenRejected *RejectedError
	openRejected     *RejectedError
	probeAdmitted    []int64
	cfg              Config
	packed           atomic.Uint64
	reopenAt         atomic.Int64
	hookPanics       atomic.Uint64
	attempt          int
	mu               sync.Mutex
	inFlight         atomic.Int32
	probeSuccesses   uint32
	forced           atomic.Bool
}

// New validates cfg, applies defaults, and returns a Closed breaker.
func New(cfg Config) (*Breaker, error) { //nolint:gocritic // by-value Config is the public constructor shape
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	b := &Breaker{
		cfg:              cfg,
		base:             cfg.Clock(),
		win:              cfg.Window.newWindow(),
		probeAdmitted:    make([]int64, cfg.HalfOpenMaxCalls),
		openRejected:     &RejectedError{Err: ErrOpen, Name: cfg.Name, State: StateOpen},
		halfOpenRejected: &RejectedError{Err: ErrTooManyProbes, Name: cfg.Name, State: StateHalfOpen},
	}
	b.packed.Store(pack(firstEpoch, StateClosed))
	return b, nil
}

func pack(epoch uint64, s State) uint64 { return epoch<<stateBits | uint64(s) }

func unpack(w uint64) (epoch uint64, s State) { return w >> stateBits, State(w & stateMask) }

// Name returns the configured name.
func (b *Breaker) Name() string { return b.cfg.Name }

// HookPanics returns how many Events hooks have panicked.
func (b *Breaker) HookPanics() uint64 { return b.hookPanics.Load() }

func (b *Breaker) rel(t time.Time) int64 { return t.Sub(b.base).Nanoseconds() }

func (b *Breaker) state() (uint64, State) { return unpack(b.packed.Load()) }

func (b *Breaker) backoffDue(now time.Time) bool { return b.rel(now) >= b.reopenAt.Load() }

// State returns a lazy, lock-free view: an Open breaker whose backoff has
// elapsed reports HalfOpen even though the transition itself happens on the
// next Allow.
func (b *Breaker) State() State {
	_, s := b.state()
	if s == StateOpen && !b.forced.Load() && b.backoffDue(b.cfg.Clock()) {
		return StateHalfOpen
	}
	return s
}

// Counts returns the window as of now, excluding expired data.
func (b *Breaker) Counts() Counts {
	now := b.cfg.Clock()
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.win.counts(now)
}

// Snapshot returns one consistent read of the breaker's state.
func (b *Breaker) Snapshot() Snapshot {
	now := b.cfg.Clock()
	b.mu.Lock()
	defer b.mu.Unlock()
	epoch, s := b.state()
	return Snapshot{
		OpenedAt: b.openedAt,
		ReopenAt: b.base.Add(time.Duration(b.reopenAt.Load())),
		Counts:   b.win.counts(now),
		Epoch:    epoch,
		Attempt:  b.attempt,
		InFlight: uint32(b.inFlight.Load()), //nolint:gosec // never negative
		State:    s,
		Forced:   b.forced.Load(),
	}
}

// Allow asks to make a call. It returns a Permit to pass to Record, or a
// *RejectedError wrapping ErrOpen or ErrTooManyProbes.
func (b *Breaker) Allow() (Permit, error) {
	now := b.cfg.Clock()
	epoch, s := b.state()
	switch s {
	case StateClosed:
		return Permit{epoch: epoch}, nil
	case StateOpen:
		if !b.forced.Load() {
			if b.cfg.ReopenEarly != nil && b.reopenEarly() {
				return b.toHalfOpen(epoch, now, ReasonEarlyHalfOpen)
			}
			if b.backoffDue(now) {
				return b.toHalfOpen(epoch, now, ReasonTimeout)
			}
		}
		b.reject(now, b.openRejected)
		return Permit{}, b.openRejected
	default:
		return b.admitProbe(now)
	}
}

func (b *Breaker) reopenEarly() (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			ok = false
			b.fireHookPanic(hookReopenEarly, r)
		}
	}()
	return b.cfg.ReopenEarly()
}

// toHalfOpen performs Open to HalfOpen under the lock. Exactly one caller
// wins; the rest observe the new state and compete for probe slots. A caller
// whose observed epoch is no longer current re-evaluates from scratch, so a
// waiter never skips a backoff that started after it looked.
func (b *Breaker) toHalfOpen(epoch uint64, now time.Time, reason Reason) (Permit, error) {
	b.mu.Lock()
	current, s := b.state()
	stale := current != epoch || s != StateOpen || b.forced.Load()
	if stale {
		b.mu.Unlock()
		return b.Allow()
	}
	ev := b.transition(StateHalfOpen, reason, now)
	b.win.reset()
	b.probeSuccesses = 0
	b.probeAdmitted[0] = b.rel(now) + 1
	b.inFlight.Store(1)
	b.mu.Unlock()
	b.fireStateChange(ev)
	return Permit{epoch: ev.Epoch, slot: 1}, nil
}

func (b *Breaker) admitProbe(now time.Time) (Permit, error) {
	b.mu.Lock()
	epoch, s := b.state()
	if s != StateHalfOpen {
		b.mu.Unlock()
		return b.Allow()
	}
	if b.probeExpired(now) {
		ev := b.open(now, ReasonProbeTimeout, b.attempt+1)
		b.mu.Unlock()
		b.fireStateChange(ev)
		b.reject(now, b.openRejected)
		return Permit{}, b.openRejected
	}
	for i, t := range b.probeAdmitted {
		if t == 0 {
			b.probeAdmitted[i] = b.rel(now) + 1
			b.inFlight.Add(1)
			b.mu.Unlock()
			return Permit{epoch: epoch, slot: uint32(i) + 1}, nil //nolint:gosec // bounded by HalfOpenMaxCalls
		}
	}
	b.mu.Unlock()
	b.reject(now, b.halfOpenRejected)
	return Permit{}, b.halfOpenRejected
}

func (b *Breaker) probeExpired(now time.Time) bool {
	limit := b.cfg.ProbeTimeout.Nanoseconds()
	rel := b.rel(now)
	for _, t := range b.probeAdmitted {
		if t != 0 && rel-(t-1) > limit {
			return true
		}
	}
	return false
}

// Record reports the outcome of a call admitted by Allow. Stale permits and
// non-probe ignored outcomes return before taking the lock.
func (b *Breaker) Record(p Permit, outcome Outcome, elapsed time.Duration) {
	if epoch, _ := b.state(); p.epoch != epoch {
		return
	}
	if outcome == OutcomeIgnored && !p.IsProbe() {
		return
	}
	now := b.cfg.Clock()
	b.mu.Lock()
	epoch, s := b.state()
	if p.epoch != epoch {
		b.mu.Unlock()
		return
	}
	if p.IsProbe() && b.probeAdmitted[p.slot-1] != 0 {
		b.probeAdmitted[p.slot-1] = 0
		b.inFlight.Add(-1)
	}
	if outcome == OutcomeIgnored {
		b.mu.Unlock()
		return
	}
	slow := b.cfg.SlowCallDuration > 0 && elapsed > b.cfg.SlowCallDuration
	b.win.record(now, outcome, slow)
	ev, changed, panics := b.evaluate(now, s, outcome)
	b.mu.Unlock()
	for _, hp := range panics {
		b.fireHookPanic(hookPolicy, hp)
	}
	b.fireCall(CallEvent{At: now, Name: b.cfg.Name, Elapsed: elapsed, State: s, Outcome: outcome, Slow: slow})
	if changed {
		b.fireStateChange(ev)
	}
}

// evaluate applies the state machine after a recorded outcome, under the lock.
func (b *Breaker) evaluate(now time.Time, s State, outcome Outcome) (ev StateChangeEvent, changed bool, panics []any) {
	switch s {
	case StateClosed:
		if b.forced.Load() {
			return ev, false, nil
		}
		p, panics := b.tripped(now)
		if p == nil {
			return ev, false, panics
		}
		return b.open(now, Reason(p.Name()), 0), true, panics
	case StateHalfOpen:
		if outcome == OutcomeFailure {
			return b.open(now, ReasonHalfOpenFailure, b.attempt+1), true, nil
		}
		b.probeSuccesses++
		if b.probeSuccesses < b.cfg.HalfOpenSuccesses {
			return ev, false, nil
		}
		p, panics := b.tripped(now)
		if p != nil {
			return b.open(now, Reason(p.Name()), b.attempt+1), true, panics
		}
		return b.close(now, ReasonHalfOpenSuccess), true, panics
	default:
		return ev, false, nil
	}
}

// tripped returns the first policy that trips, running each under recover.
func (b *Breaker) tripped(now time.Time) (tripped Policy, panics []any) {
	counts := b.win.counts(now)
	for _, p := range b.cfg.Policies {
		if trip, r := shouldTrip(p, counts); r != nil {
			panics = append(panics, r)
		} else if trip {
			return p, panics
		}
	}
	return nil, panics
}

func shouldTrip(p Policy, c Counts) (trip bool, recovered any) {
	defer func() {
		if r := recover(); r != nil {
			recovered = r
			trip = false
		}
	}()
	return p.ShouldTrip(c), nil
}

// open enters Open under the lock with the given attempt number.
func (b *Breaker) open(now time.Time, reason Reason, attempt int) StateChangeEvent {
	b.attempt = attempt
	b.openedAt = now
	b.reopenAt.Store(b.rel(now.Add(b.delay(attempt))))
	b.clearProbes()
	return b.transition(StateOpen, reason, now)
}

func (b *Breaker) delay(attempt int) (d time.Duration) {
	defer func() {
		if r := recover(); r != nil {
			d = b.cfg.OpenDuration
		}
	}()
	return b.cfg.Backoff.Delay(attempt)
}

// close enters Closed under the lock.
func (b *Breaker) close(now time.Time, reason Reason) StateChangeEvent {
	b.attempt = 0
	ev := b.transition(StateClosed, reason, now)
	b.win.reset()
	b.clearProbes()
	return ev
}

func (b *Breaker) clearProbes() {
	clear(b.probeAdmitted)
	b.inFlight.Store(0)
	b.probeSuccesses = 0
}

// transition bumps the epoch and stores the new state. Counts in the event
// are read before any clearing the caller does afterwards.
func (b *Breaker) transition(to State, reason Reason, now time.Time) StateChangeEvent {
	epoch, from := b.state()
	epoch++
	b.packed.Store(pack(epoch, to))
	return StateChangeEvent{
		At:      now,
		Name:    b.cfg.Name,
		Reason:  reason,
		Counts:  b.win.counts(now),
		Attempt: b.attempt,
		Epoch:   epoch,
		From:    from,
		To:      to,
	}
}

// ForceOpen pins the breaker Open until ForceClose or Reset.
func (b *Breaker) ForceOpen() {
	now := b.cfg.Clock()
	b.mu.Lock()
	b.forced.Store(true)
	if _, s := b.state(); s == StateOpen {
		b.mu.Unlock()
		return
	}
	b.openedAt = now
	b.reopenAt.Store(math.MaxInt64)
	b.clearProbes()
	ev := b.transition(StateOpen, ReasonForced, now)
	b.mu.Unlock()
	b.fireStateChange(ev)
}

// ForceClose pins the breaker Closed until ForceOpen or Reset. Outcomes are
// still recorded so Counts stays informative, but policies never trip.
func (b *Breaker) ForceClose() {
	now := b.cfg.Clock()
	b.mu.Lock()
	b.forced.Store(true)
	if _, s := b.state(); s == StateClosed {
		b.mu.Unlock()
		return
	}
	ev := b.close(now, ReasonForced)
	b.mu.Unlock()
	b.fireStateChange(ev)
}

// Reset unpins the breaker, clears the window, and returns to Closed with
// attempt 0. Permits issued before Reset become stale.
func (b *Breaker) Reset() {
	now := b.cfg.Clock()
	b.mu.Lock()
	b.forced.Store(false)
	ev := b.close(now, ReasonReset)
	b.mu.Unlock()
	b.fireStateChange(ev)
}

func (b *Breaker) reject(now time.Time, re *RejectedError) {
	b.fireReject(RejectEvent{At: now, Err: re.Err, Name: b.cfg.Name, State: re.State})
}

func (b *Breaker) recoverHook() {
	if r := recover(); r != nil {
		b.hookPanics.Add(1)
	}
}

func (b *Breaker) fireStateChange(ev StateChangeEvent) {
	defer b.recoverHook()
	b.cfg.Events.OnStateChange(ev)
}

func (b *Breaker) fireCall(ev CallEvent) {
	defer b.recoverHook()
	b.cfg.Events.OnCall(ev)
}

func (b *Breaker) fireReject(ev RejectEvent) {
	defer b.recoverHook()
	b.cfg.Events.OnReject(ev)
}

func (b *Breaker) fireHookPanic(name string, recovered any) {
	defer b.recoverHook()
	b.cfg.Events.OnHookPanic(name, recovered)
}
