package breaker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	fuzzMaxCalls     = 3
	fuzzOpenDuration = 10 * time.Second
	fuzzProbeTimeout = 20 * time.Second
	fuzzAdvanceStep  = time.Second
	fuzzOps          = 8
)

// countingEvents counts transitions without testify's reflection cost.
type countingEvents struct {
	NoopEvents
	transitions atomic.Int64
}

func (c *countingEvents) OnStateChange(StateChangeEvent) { c.transitions.Add(1) }

// FuzzStateMachine drives random Allow/Record/clock/force sequences and
// checks the breaker's invariants after every operation.
func FuzzStateMachine(f *testing.F) {
	f.Add([]byte{0, 1, 0, 1, 3, 0, 1, 2, 4, 0, 5, 6, 0})
	f.Add([]byte{0, 0, 0, 1, 1, 1, 3, 3, 0, 1, 0, 1})
	f.Fuzz(func(t *testing.T, ops []byte) {
		clock := newTestClock()
		events := &countingEvents{}
		b, err := New(Config{
			Name:             "fuzz",
			Policies:         []Policy{ConsecutiveFailures{Count: 2}},
			OpenDuration:     fuzzOpenDuration,
			ProbeTimeout:     fuzzProbeTimeout,
			HalfOpenMaxCalls: fuzzMaxCalls,
			Events:           events,
			Clock:            clock.Now,
		})
		if err != nil {
			t.Fatal(err)
		}
		var (
			live        []Permit
			stale       []Permit
			transitions int64
			lastState   = StateClosed
			lastAttempt int
		)
		for _, op := range ops {
			switch op % fuzzOps {
			case 0:
				if p, err := b.Allow(); err == nil {
					live = append(live, p)
				}
			case 1, 2:
				if len(live) > 0 {
					p := live[0]
					live = live[1:]
					b.Record(p, Outcome(op%fuzzOps-1), 0)
				}
			case 3:
				if len(stale) > 0 {
					b.Record(stale[0], OutcomeFailure, 0)
				}
			case 4:
				clock.advance(fuzzAdvanceStep)
			case 5:
				b.ForceOpen()
			case 6:
				b.ForceClose()
			case 7:
				b.Reset()
			}
			snap := b.Snapshot()
			if snap.State != lastState {
				stale = append(stale, live...)
				live = nil
			}
			if got := events.transitions.Load(); got-transitions > 1 {
				t.Fatalf("one op produced %d transitions", got-transitions)
			}
			transitions = events.transitions.Load()
			if snap.InFlight > fuzzMaxCalls {
				t.Fatalf("in-flight %d exceeds max", snap.InFlight)
			}
			if snap.State == StateClosed && snap.InFlight != 0 {
				t.Fatalf("closed with %d in flight", snap.InFlight)
			}
			if snap.State > StateHalfOpen {
				t.Fatalf("bad state %d", snap.State)
			}
			if snap.State != StateClosed && lastState != StateClosed && snap.Attempt < lastAttempt {
				t.Fatalf("attempt went backwards %d -> %d", lastAttempt, snap.Attempt)
			}
			lastState, lastAttempt = snap.State, snap.Attempt
		}
	})
}

// TestStalePermitRaceAcrossGoroutines interleaves Allow with a trip so a
// permit issued under the old packed word is never accepted afterwards.
func TestStalePermitRaceAcrossGoroutines(t *testing.T) {
	const (
		workers = 8
		rounds  = 200
	)
	clock := newTestClock()
	b, err := New(Config{
		Name:         "race",
		Policies:     []Policy{ConsecutiveFailures{Count: 1}},
		OpenDuration: fuzzOpenDuration,
		Clock:        clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	for range rounds {
		var wg sync.WaitGroup
		var permits [workers]Permit
		var ok [workers]bool
		for i := range workers {
			wg.Go(func() {
				p, err := b.Allow()
				permits[i], ok[i] = p, err == nil
			})
		}
		wg.Go(func() {
			if p, err := b.Allow(); err == nil {
				b.Record(p, OutcomeFailure, 0)
			}
		})
		wg.Wait()
		before := b.Counts()
		for i := range workers {
			if ok[i] {
				b.Record(permits[i], OutcomeFailure, 0)
			}
		}
		if b.State() == StateOpen && b.Counts() != before {
			t.Fatal("stale permit accepted after trip")
		}
		b.Reset()
	}
}

func TestLeakedSlotDetectedUnderSteadyProbes(t *testing.T) {
	const (
		maxCalls  = 3
		successes = 1000
		period    = time.Second
	)
	clock := newTestClock()
	b, err := New(Config{
		Name:              "leak",
		Policies:          []Policy{ConsecutiveFailures{Count: 1}},
		OpenDuration:      fuzzOpenDuration,
		ProbeTimeout:      testProbeTimeout,
		HalfOpenMaxCalls:  maxCalls,
		HalfOpenSuccesses: successes,
		Clock:             clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	p, _ := b.Allow()
	b.Record(p, OutcomeFailure, 0)
	clock.advance(fuzzOpenDuration)
	if leaked, err := b.Allow(); err != nil || !leaked.IsProbe() {
		t.Fatalf("expected probe, got %v", err)
	}
	for range int(testProbeTimeout / period) {
		clock.advance(period)
		for range maxCalls - 1 {
			q, err := b.Allow()
			if err != nil {
				if b.State() == StateOpen {
					t.Fatalf("reopened early at %v", clock.now)
				}
				t.Fatal(err)
			}
			b.Record(q, OutcomeSuccess, 0)
		}
	}
	clock.advance(time.Millisecond)
	if _, err := b.Allow(); err == nil || b.Snapshot().State != StateOpen {
		t.Fatalf("expected reopen on leaked slot, state %v err %v", b.State(), err)
	}
}
