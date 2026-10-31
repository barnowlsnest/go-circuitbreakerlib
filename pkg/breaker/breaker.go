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

	hookBackoff     = "backoff"
	hookPolicy      = "policy"
	hookReopenEarly = "reopen-early"
	hookClassifier  = "classifier"
	hookStateChange = "on-state-change"
	hookCall        = "on-call"
	hookReject      = "on-reject"
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
	cfg              config
	packed           atomic.Uint64
	reopenAt         atomic.Int64
	hookPanics       atomic.Uint64
	attempt          int
	mu               sync.Mutex
	inFlight         atomic.Uint32
	probeSuccesses   uint32
	forced           atomic.Bool
}

// New applies opts, validates, applies defaults, and returns a Closed
// breaker.
func New(opts ...Option) (*Breaker, error) {
	o, err := applyOptions(opts...)
	if err != nil {
		return nil, err
	}
	cfg := o.toConfig()
	c, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	b := &Breaker{
		cfg:              c,
		base:             c.Clock(),
		win:              c.Window.newWindow(),
		probeAdmitted:    make([]int64, c.HalfOpenMaxCalls),
		openRejected:     &RejectedError{Err: ErrOpen, Name: c.Name, State: StateOpen},
		halfOpenRejected: &RejectedError{Err: ErrTooManyProbes, Name: c.Name, State: StateHalfOpen},
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

// ReportHookPanic reports a panic recovered from a user hook that runs
// outside the breaker, such as an adapter classifier, through
// Events.OnHookPanic. A panic in OnHookPanic is counted in HookPanics.
func (b *Breaker) ReportHookPanic(name string, recovered any) { b.fireHookPanic(name, recovered) }

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

// Now returns the configured clock's current time.
func (b *Breaker) Now() time.Time { return b.cfg.Clock() }

// Snapshot returns one consistent read of the breaker's state.
func (b *Breaker) Snapshot() Snapshot {
	now := b.cfg.Clock()
	b.mu.Lock()
	defer b.mu.Unlock()
	epoch, s := b.state()
	if s == StateOpen && !b.forced.Load() && b.backoffDue(now) {
		s = StateHalfOpen
	}
	return Snapshot{
		OpenedAt: b.openedAt,
		ReopenAt: b.base.Add(time.Duration(b.reopenAt.Load())),
		Counts:   b.win.counts(now),
		Epoch:    epoch,
		Attempt:  b.attempt,
		InFlight: b.inFlight.Load(),
		State:    s,
		Forced:   b.forced.Load(),
	}
}

const maxAllowRetries = 64

// Allow asks to make a call. It returns a Permit to pass to Record, or a
// *RejectedError wrapping ErrOpen or ErrTooManyProbes.
func (b *Breaker) Allow() (Permit, error) {
	for range maxAllowRetries {
		now := b.cfg.Clock()
		epoch, s := b.state()
		switch s {
		case StateClosed:
			return Permit{epoch: epoch}, nil
		case StateOpen:
			if !b.forced.Load() {
				if b.cfg.ReopenEarly != nil && b.reopenEarly() {
					if p, again := b.toHalfOpen(epoch, now, ReasonEarlyHalfOpen); !again {
						return p, nil
					}
					continue
				}
				if b.backoffDue(now) {
					if p, again := b.toHalfOpen(epoch, now, ReasonTimeout); !again {
						return p, nil
					}
					continue
				}
			}
			b.reject(now, b.openRejected)
			return Permit{}, b.openRejected
		default:
			if p, err, again := b.admitProbe(now); !again {
				return p, err
			}
		}
	}
	return b.allowExhausted()
}

func (b *Breaker) allowExhausted() (Permit, error) {
	now := b.cfg.Clock()
	epoch, s := b.state()
	switch s {
	case StateClosed:
		return Permit{epoch: epoch}, nil
	case StateHalfOpen:
		b.reject(now, b.halfOpenRejected)
		return Permit{}, b.halfOpenRejected
	default:
		b.reject(now, b.openRejected)
		return Permit{}, b.openRejected
	}
}

func (b *Breaker) reopenEarly() (ok bool) {
	defer b.recoverReopenEarly(&ok)
	return b.cfg.ReopenEarly()
}

func (b *Breaker) recoverReopenEarly(ok *bool) {
	if r := recover(); r != nil {
		*ok = false
		b.fireHookPanic(hookReopenEarly, r)
	}
}

// toHalfOpen performs Open to HalfOpen under the lock. Exactly one caller
// wins; the rest observe the new state and compete for probe slots. A caller
// whose observed epoch is no longer current re-evaluates from scratch, so a
// waiter never skips a backoff that started after it looked. again is true
// when the caller must retry Allow.
func (b *Breaker) toHalfOpen(epoch uint64, now time.Time, reason Reason) (p Permit, again bool) {
	b.mu.Lock()
	current, s := b.state()
	stale := current != epoch || s != StateOpen || b.forced.Load()
	if stale {
		b.mu.Unlock()
		return Permit{}, true
	}
	ev := b.transition(StateHalfOpen, reason, now)
	b.win.reset()
	b.probeSuccesses = 0
	b.probeAdmitted[0] = b.rel(now) + 1
	b.inFlight.Store(1)
	b.mu.Unlock()
	b.fireStateChange(ev)
	return Permit{epoch: ev.Epoch, slot: 1}, false
}

func (b *Breaker) admitProbe(now time.Time) (p Permit, err error, again bool) {
	b.mu.Lock()
	epoch, s := b.state()
	if s != StateHalfOpen {
		b.mu.Unlock()
		return Permit{}, nil, true
	}
	if b.probeExpired(now) {
		ev, bp := b.open(now, ReasonProbeTimeout, b.attempt+1)
		b.mu.Unlock()
		if bp != nil {
			b.fireHookPanic(hookBackoff, bp)
		}
		b.fireStateChange(ev)
		b.reject(now, b.openRejected)
		return Permit{}, b.openRejected, false
	}
	var slot uint32
	for i, t := range b.probeAdmitted {
		slot++
		if t == 0 {
			b.probeAdmitted[i] = b.rel(now) + 1
			b.inFlight.Add(1)
			b.mu.Unlock()
			return Permit{epoch: epoch, slot: slot}, nil, false
		}
	}
	b.mu.Unlock()
	b.reject(now, b.halfOpenRejected)
	return Permit{}, b.halfOpenRejected, false
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

// Record reports the outcome of a call admitted by Allow.
func (b *Breaker) Record(p Permit, outcome Outcome, elapsed time.Duration, err error) {
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
		b.inFlight.Add(^uint32(0))
	}
	if outcome == OutcomeIgnored {
		b.mu.Unlock()
		return
	}
	slow := b.cfg.SlowCallDuration > 0 && elapsed > b.cfg.SlowCallDuration
	b.win.record(now, outcome, slow)
	ev, changed, panics := b.evaluate(now, epoch, s, outcome)
	b.mu.Unlock()
	for _, hp := range panics {
		b.fireHookPanic(hp.name, hp.recovered)
	}
	b.fireCall(CallEvent{At: now, Err: err, Name: b.cfg.Name, Elapsed: elapsed, State: s, Outcome: outcome, Slow: slow})
	if changed {
		b.fireStateChange(ev)
	}
}

// hookPanic is a panic recovered under the lock, fired after unlock.
type hookPanic struct {
	recovered any
	name      string
}

// evaluate applies the state machine after a recorded outcome.
func (b *Breaker) evaluate(now time.Time, epoch uint64, s State, outcome Outcome) (ev StateChangeEvent, changed bool, panics []hookPanic) {
	switch s {
	case StateClosed:
		if b.forced.Load() {
			return ev, false, nil
		}
		counts := b.win.counts(now)
		b.mu.Unlock()
		p, panics := b.tripped(counts)
		b.mu.Lock()
		current, state := b.state()
		if current != epoch || state != StateClosed || b.forced.Load() {
			return ev, false, panics
		}
		if p == nil {
			return ev, false, panics
		}
		return b.openWith(now, Reason(p.Name()), 0, panics)
	case StateHalfOpen:
		if outcome == OutcomeFailure {
			return b.openWith(now, ReasonHalfOpenFailure, b.attempt+1, nil)
		}
		b.probeSuccesses++
		if b.probeSuccesses < b.cfg.HalfOpenSuccesses {
			return ev, false, nil
		}
		counts := b.win.counts(now)
		b.mu.Unlock()
		p, panics := b.tripped(counts)
		b.mu.Lock()
		current, state := b.state()
		if current != epoch || state != StateHalfOpen {
			return ev, false, panics
		}
		if p != nil {
			return b.openWith(now, Reason(p.Name()), b.attempt+1, panics)
		}
		return b.close(now, ReasonHalfOpenSuccess), true, panics
	default:
		return ev, false, nil
	}
}

// openWith enters Open for evaluate, appending a Backoff panic to panics.
func (b *Breaker) openWith(now time.Time, reason Reason, attempt int, panics []hookPanic) (StateChangeEvent, bool, []hookPanic) {
	ev, bp := b.open(now, reason, attempt)
	if bp != nil {
		panics = append(panics, hookPanic{name: hookBackoff, recovered: bp})
	}
	return ev, true, panics
}

// tripped returns the first policy that trips, running each under recover.
func (b *Breaker) tripped(counts Counts) (tripped Policy, panics []hookPanic) {
	for _, p := range b.cfg.Policies {
		if trip, r := b.shouldTrip(p, counts); r != nil {
			panics = append(panics, hookPanic{name: hookPolicy, recovered: r})
		} else if trip {
			return p, panics
		}
	}
	return nil, panics
}

func (b *Breaker) shouldTrip(p Policy, c Counts) (trip bool, recovered any) {
	defer b.recoverShouldTrip(&trip, &recovered)
	return p.ShouldTrip(c), nil
}

func (*Breaker) recoverShouldTrip(trip *bool, recovered *any) {
	if r := recover(); r != nil {
		*trip = false
		*recovered = r
	}
}

// open enters Open under the lock with the given attempt number. It returns
// the value recovered from a panicking Backoff, for the caller to report
// after unlock.
func (b *Breaker) open(now time.Time, reason Reason, attempt int) (ev StateChangeEvent, backoffPanic any) {
	b.attempt = attempt
	b.openedAt = now
	d, backoffPanic := b.delay(attempt)
	b.reopenAt.Store(b.rel(now.Add(d)))
	b.clearProbes()
	return b.transition(StateOpen, reason, now), backoffPanic
}

func (b *Breaker) delay(attempt int) (d time.Duration, recovered any) {
	if b.cfg.Backoff == nil {
		return b.cfg.OpenDuration, nil
	}
	defer b.recoverDelay(&d, &recovered)
	return b.cfg.Backoff.Delay(attempt), nil
}

func (b *Breaker) recoverDelay(d *time.Duration, recovered *any) {
	if r := recover(); r != nil {
		*d, *recovered = b.cfg.OpenDuration, r
	}
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

func (b *Breaker) recoverHook(name string) {
	if r := recover(); r != nil {
		b.hookPanics.Add(1)
		b.fireHookPanic(name, r)
	}
}

func (b *Breaker) fireStateChange(ev StateChangeEvent) {
	defer b.recoverHook(hookStateChange)
	b.cfg.Events.OnStateChange(ev)
}

func (b *Breaker) fireCall(ev CallEvent) {
	defer b.recoverHook(hookCall)
	b.cfg.Events.OnCall(ev)
}

func (b *Breaker) fireReject(ev RejectEvent) {
	defer b.recoverHook(hookReject)
	b.cfg.Events.OnReject(ev)
}

func (b *Breaker) fireHookPanic(name string, recovered any) {
	defer func() {
		if r := recover(); r != nil {
			b.hookPanics.Add(1)
		}
	}()
	b.cfg.Events.OnHookPanic(name, recovered)
}
