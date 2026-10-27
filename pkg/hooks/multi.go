package hooks

import "github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"

// multiEvents fans every hook out to each delegate in order.
type multiEvents struct {
	delegates []breaker.Events
}

// NewMultiEvents returns an Events that forwards every hook to each of evs
// in order. Nil delegates are skipped. Each delegate runs under its own
// recover, so one panicking observer never starves the next.
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
	for _, d := range m.delegates {
		safely(func() { d.OnStateChange(ev) })
	}
}

func (m *multiEvents) OnCall(ev breaker.CallEvent) {
	for _, d := range m.delegates {
		safely(func() { d.OnCall(ev) })
	}
}

func (m *multiEvents) OnReject(ev breaker.RejectEvent) {
	for _, d := range m.delegates {
		safely(func() { d.OnReject(ev) })
	}
}

func (m *multiEvents) OnHookPanic(name string, recovered any) {
	for _, d := range m.delegates {
		safely(func() { d.OnHookPanic(name, recovered) })
	}
}

func safely(fn func()) {
	defer func() { _ = recover() }()
	fn()
}
