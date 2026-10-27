package breaker

import "time"

// Events observes a breaker. Hooks fire synchronously on the calling
// goroutine after the breaker lock is released, so hook latency is request
// latency: implementations must not block, make network calls, or take
// locks shared with request handling. Every hook runs under recover; a
// panic is swallowed and counted in Breaker.HookPanics.
//
// Embed NoopEvents to implement only the hooks you need.
type Events interface {
	// OnStateChange fires exactly once per transition.
	OnStateChange(ev StateChangeEvent)
	// OnCall fires for every recorded, non-stale Success or Failure. It never
	// fires for OutcomeIgnored.
	OnCall(ev CallEvent)
	// OnReject fires for every rejected Allow.
	OnReject(ev RejectEvent)
	// OnHookPanic fires when a user hook panicked; name says which:
	// "classifier", "policy", "reopen-early", "backoff", or the Events hook
	// ("on-state-change", "on-call", "on-reject"). Breaker.ReportHookPanic
	// fires it for hooks run outside the breaker, such as adapter
	// classifiers. A panic in OnHookPanic itself is only counted.
	OnHookPanic(name string, recovered any)
}

// StateChangeEvent describes one transition. Counts is the window as it was
// before the transition cleared it.
type StateChangeEvent struct {
	At      time.Time
	Name    string
	Reason  Reason
	Counts  Counts
	Attempt int
	Epoch   uint64
	From    State
	To      State
}

// CallEvent describes one recorded outcome.
type CallEvent struct {
	At      time.Time
	Err     error
	Name    string
	Elapsed time.Duration
	State   State
	Outcome Outcome
	Slow    bool
}

// RejectEvent describes one rejected Allow. Err is ErrOpen or ErrTooManyProbes.
type RejectEvent struct {
	At    time.Time
	Err   error
	Name  string
	State State
}

// NoopEvents discards every event.
type NoopEvents struct{}

// NewNoopEvents returns a no-op observer.
func NewNoopEvents() *NoopEvents { return &NoopEvents{} }

// OnStateChange implements Events.
func (*NoopEvents) OnStateChange(StateChangeEvent) {}

// OnCall implements Events.
func (*NoopEvents) OnCall(CallEvent) {}

// OnReject implements Events.
func (*NoopEvents) OnReject(RejectEvent) {}

// OnHookPanic implements Events.
func (*NoopEvents) OnHookPanic(string, any) {}
