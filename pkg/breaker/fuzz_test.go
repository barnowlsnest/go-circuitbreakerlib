package breaker

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	fuzzMaxCalls     = 3
	fuzzOpenDuration = 10 * time.Second
	fuzzProbeTimeout = 20 * time.Second
	fuzzAdvanceStep  = time.Second
	fuzzOps          = 8
)

// FuzzStateMachine drives random Allow/Record/clock/force sequences and
// checks the breaker's invariants after every operation. Go requires fuzz
// targets to be top-level FuzzXxx functions, so this one cannot live on a
// suite; it uses the shared MockEvents and testify's require package.
func FuzzStateMachine(f *testing.F) {
	f.Add([]byte{0, 1, 0, 1, 3, 0, 1, 2, 4, 0, 5, 6, 0})
	f.Add([]byte{0, 0, 0, 1, 1, 1, 3, 3, 0, 1, 0, 1})
	f.Fuzz(func(t *testing.T, ops []byte) {
		clock := newTestClock()
		events := newNoopMockEvents()
		transitions := 0
		events.On("OnStateChange", mock.Anything).Run(func(mock.Arguments) { transitions++ })
		b, err := New(&Config{
			Name:             "fuzz",
			Policies:         []Policy{ConsecutiveFailures{Count: 2}},
			OpenDuration:     fuzzOpenDuration,
			ProbeTimeout:     fuzzProbeTimeout,
			HalfOpenMaxCalls: fuzzMaxCalls,
			Events:           events,
			Clock:            clock.Now,
		})
		require.NoError(t, err)
		var (
			live        []Permit
			stale       []Permit
			seen        int
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
			require.LessOrEqual(t, transitions-seen, 1, "one op produced several transitions")
			seen = transitions
			require.LessOrEqual(t, snap.InFlight, uint32(fuzzMaxCalls), "in-flight exceeds max")
			if snap.State == StateClosed {
				require.Zero(t, snap.InFlight, "closed with calls in flight")
			}
			require.LessOrEqual(t, snap.State, StateHalfOpen, "bad state")
			if snap.State != StateClosed && lastState != StateClosed {
				require.GreaterOrEqual(t, snap.Attempt, lastAttempt, "attempt went backwards")
			}
			lastState, lastAttempt = snap.State, snap.Attempt
		}
	})
}

// InvariantSuite holds the concurrency and timing invariants that are too
// long-running for the fuzzer's per-input budget.
type InvariantSuite struct {
	suite.Suite
	clock  *testClock
	events *MockEvents
}

func TestInvariantSuite(t *testing.T) {
	suite.Run(t, new(InvariantSuite))
}

func (s *InvariantSuite) SetupTest() {
	s.clock = newTestClock()
	s.events = newNoopMockEvents()
}

func (s *InvariantSuite) TearDownTest() {
	s.events.AssertExpectations(s.T())
}

// TestStalePermitRaceAcrossGoroutines interleaves Allow with a trip so a
// permit issued under the old packed word is never accepted afterwards.
func (s *InvariantSuite) TestStalePermitRaceAcrossGoroutines() {
	const (
		workers = 8
		rounds  = 200
	)
	b, err := New(&Config{
		Name:         "race",
		Policies:     []Policy{ConsecutiveFailures{Count: 1}},
		OpenDuration: fuzzOpenDuration,
		Events:       s.events,
		Clock:        s.clock.Now,
	})
	s.Require().NoError(err)
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
		if b.State() == StateOpen {
			s.Require().Equal(before, b.Counts(), "stale permit accepted after trip")
		}
		b.Reset()
	}
}

// TestLeakedSlotDetectedUnderSteadyProbes keeps the other probe slots busy
// and checks that the one slot nobody records still reopens the breaker
// after ProbeTimeout.
func (s *InvariantSuite) TestLeakedSlotDetectedUnderSteadyProbes() {
	const (
		maxCalls  = 3
		successes = 1000
		period    = time.Second
	)
	b, err := New(&Config{
		Name:              "leak",
		Policies:          []Policy{ConsecutiveFailures{Count: 1}},
		OpenDuration:      fuzzOpenDuration,
		ProbeTimeout:      testProbeTimeout,
		HalfOpenMaxCalls:  maxCalls,
		HalfOpenSuccesses: successes,
		Events:            s.events,
		Clock:             s.clock.Now,
	})
	s.Require().NoError(err)
	p, err := b.Allow()
	s.Require().NoError(err)
	b.Record(p, OutcomeFailure, 0)
	s.clock.advance(fuzzOpenDuration)
	leaked, err := b.Allow()
	s.Require().NoError(err)
	s.Require().True(leaked.IsProbe())
	for range int(testProbeTimeout / period) {
		s.clock.advance(period)
		for range maxCalls - 1 {
			q, err := b.Allow()
			s.Require().NoError(err, "reopened early at %v", s.clock.now)
			b.Record(q, OutcomeSuccess, 0)
		}
	}
	s.clock.advance(time.Millisecond)
	_, err = b.Allow()
	s.Require().Error(err)
	s.Require().Equal(StateOpen, b.Snapshot().State, "expected reopen on leaked slot")
}
