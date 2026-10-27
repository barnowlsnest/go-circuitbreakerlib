package hooks

import "github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"

const (
	hookOnStateChange = "on-state-change"
	hookOnCall        = "on-call"
	hookOnReject      = "on-reject"
	hookOnHookPanic   = "on-hook-panic"
)

// multiEvents fans every hook out to each delegate in order.
type multiEvents struct {
	delegates []breaker.Events
}

// NewMultiEvents returns an Events that forwards every hook to each of evs
// in order. Nil delegates are skipped. Each delegate runs under its own
// recover, so one panicking observer never starves the next. A delegate's
// panic is reported to every other delegate through OnHookPanic, named
// after the hook that panicked.
func NewMultiEvents(evs ...breaker.Events) breaker.Events {
	delegates := make([]breaker.Events, 0, len(evs))
	for _, ev := range evs {
		if ev != nil {
			delegates = append(delegates, ev)
		}
	}
	return &multiEvents{delegates: delegates}
}

func (m *multiEvents) OnStateChange(ev breaker.StateChangeEvent) {
	for i, d := range m.delegates {
		m.safely(i, hookOnStateChange, func() { d.OnStateChange(ev) })
	}
}

func (m *multiEvents) OnCall(ev breaker.CallEvent) {
	for i, d := range m.delegates {
		m.safely(i, hookOnCall, func() { d.OnCall(ev) })
	}
}

func (m *multiEvents) OnReject(ev breaker.RejectEvent) {
	for i, d := range m.delegates {
		m.safely(i, hookOnReject, func() { d.OnReject(ev) })
	}
}

func (m *multiEvents) OnHookPanic(name string, recovered any) {
	for i, d := range m.delegates {
		m.safely(i, hookOnHookPanic, func() { d.OnHookPanic(name, recovered) })
	}
}

func (m *multiEvents) safely(i int, name string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			m.report(i, name, r)
		}
	}()
	fn()
}

func (m *multiEvents) report(from int, name string, recovered any) {
	for i, d := range m.delegates {
		if i != from {
			swallow(func() { d.OnHookPanic(name, recovered) })
		}
	}
}

func swallow(fn func()) {
	defer func() { _ = recover() }()
	fn()
}
