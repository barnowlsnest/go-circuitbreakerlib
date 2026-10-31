package breaker

import "github.com/barnowlsnest/go-circuitbreakerlib/internal/breakercore"

// Events observes a breaker. Hooks fire synchronously on the calling
// goroutine after the breaker lock is released, so hook latency is request
// latency: implementations must not block, make network calls, or take
// locks shared with request handling. Every hook runs under recover; a
// panic is counted in Breaker.HookPanics and reported through OnHookPanic,
// named after the hook. A panic in OnHookPanic itself is only counted.
//
// Embed NoopEvents to implement only the hooks you need.
type Events = breakercore.Events

// StateChangeEvent describes one transition. Counts is the window as it was
// before the transition cleared it.
type StateChangeEvent = breakercore.StateChangeEvent

// CallEvent describes one recorded outcome.
type CallEvent = breakercore.CallEvent

// RejectEvent describes one rejected Allow. Err is ErrOpen or ErrTooManyProbes.
type RejectEvent = breakercore.RejectEvent

// NoopEvents discards every event.
type NoopEvents = breakercore.NoopEvents

// NewNoopEvents returns a no-op observer.
func NewNoopEvents() *NoopEvents { return breakercore.NewNoopEvents() }
