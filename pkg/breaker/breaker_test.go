package breaker

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

const (
	testName         = "dep"
	testOpenDuration = 30 * time.Second
	testProbeTimeout = 60 * time.Second
	testTripCount    = 3
	testRejections   = 100
	testGoroutines   = 64
)

type BreakerSuite struct {
	suite.Suite
	clock  *testClock
	events *MockEvents
}

func TestBreakerSuite(t *testing.T) {
	suite.Run(t, new(BreakerSuite))
}

func (s *BreakerSuite) SetupTest() {
	s.clock = newTestClock()
	s.events = newNoopMockEvents()
}

func (s *BreakerSuite) TearDownTest() {
	s.events.AssertExpectations(s.T())
}

func (s *BreakerSuite) config() *Config {
	return &Config{
		Name:         testName,
		Policies:     []Policy{ConsecutiveFailures{Count: testTripCount}},
		OpenDuration: testOpenDuration,
		ProbeTimeout: testProbeTimeout,
		Events:       s.events,
		Clock:        s.clock.Now,
	}
}

func (s *BreakerSuite) mustNew(cfg *Config) *Breaker {
	s.T().Helper()
	b, err := New(*cfg)
	s.Require().NoError(err)
	return b
}

func (s *BreakerSuite) trip(b *Breaker) {
	s.T().Helper()
	for range testTripCount {
		p, err := b.Allow()
		s.Require().NoError(err)
		b.Record(p, OutcomeFailure, 0)
	}
	s.Require().Equal(StateOpen, b.State())
}

func (s *BreakerSuite) onCallCount() int {
	s.T().Helper()
	n := 0
	for i := range s.events.Calls {
		if s.events.Calls[i].Method == "OnCall" {
			n++
		}
	}
	return n
}

func (s *BreakerSuite) halfOpen(b *Breaker) Permit {
	s.T().Helper()
	s.trip(b)
	s.clock.advance(testOpenDuration)
	p, err := b.Allow()
	s.Require().NoError(err)
	s.Require().True(p.IsProbe())
	return p
}

func (s *BreakerSuite) TestNewDefaults() {
	b := s.mustNew(s.config())
	s.Equal(StateClosed, b.State())
	s.Equal(uint64(1), b.Snapshot().Epoch)
	s.Equal(testName, b.Name())
}

func (s *BreakerSuite) TestNewInvalid() {
	_, err := New(Config{})
	s.ErrorIs(err, ErrInvalidConfig)
}

func (s *BreakerSuite) TestClosedAllowReturnsPermit() {
	b := s.mustNew(s.config())
	p, err := b.Allow()
	s.NoError(err)
	s.False(p.IsProbe())
}

func (s *BreakerSuite) TestTripsOnConsecutiveFailures() {
	b := s.mustNew(s.config())
	s.trip(b)
	s.events.AssertCalled(s.T(), "OnStateChange", mock.MatchedBy(func(ev StateChangeEvent) bool {
		return ev.From == StateClosed && ev.To == StateOpen && ev.Reason == "consecutive-failures" && ev.Attempt == 0
	}))
	_, err1 := b.Allow()
	_, err2 := b.Allow()
	s.ErrorIs(err1, ErrOpen)
	var r1, r2 *RejectedError
	s.Require().ErrorAs(err1, &r1)
	s.Require().ErrorAs(err2, &r2)
	s.Same(r1, r2)
	s.Equal(StateOpen, r1.State)
	s.events.AssertCalled(s.T(), "OnReject", mock.MatchedBy(func(ev RejectEvent) bool {
		return errors.Is(ev.Err, ErrOpen) && ev.State == StateOpen
	}))
}

func (s *BreakerSuite) TestOpenToHalfOpenAfterBackoff() {
	b := s.mustNew(s.config())
	s.trip(b)
	s.clock.advance(testOpenDuration - time.Second)
	_, err := b.Allow()
	s.ErrorIs(err, ErrOpen)
	s.clock.advance(time.Second)
	p, err := b.Allow()
	s.NoError(err)
	s.True(p.IsProbe())
	s.Equal(StateHalfOpen, b.State())
	s.events.AssertCalled(s.T(), "OnStateChange", mock.MatchedBy(func(ev StateChangeEvent) bool {
		return ev.From == StateOpen && ev.To == StateHalfOpen && ev.Reason == ReasonTimeout
	}))
}

func (s *BreakerSuite) TestBackoffCalledOncePerOpen() {
	backoff := &MockBackoff{}
	backoff.On("Delay", 0).Return(testOpenDuration).Once()
	cfg := s.config()
	cfg.Backoff = backoff
	b := s.mustNew(cfg)
	s.trip(b)
	for range testRejections {
		_, err := b.Allow()
		s.ErrorIs(err, ErrOpen)
	}
	backoff.AssertNumberOfCalls(s.T(), "Delay", 1)
}

func (s *BreakerSuite) TestHalfOpenProbeLimit() {
	b := s.mustNew(s.config())
	s.halfOpen(b)
	_, err1 := b.Allow()
	_, err2 := b.Allow()
	s.ErrorIs(err1, ErrTooManyProbes)
	var r1, r2 *RejectedError
	s.Require().ErrorAs(err1, &r1)
	s.Require().ErrorAs(err2, &r2)
	s.Same(r1, r2)
	s.Equal(StateHalfOpen, r1.State)
}

func (s *BreakerSuite) TestHalfOpenSuccessCloses() {
	cfg := s.config()
	cfg.HalfOpenMaxCalls = 2
	cfg.HalfOpenSuccesses = 2
	b := s.mustNew(cfg)
	p1 := s.halfOpen(b)
	p2, err := b.Allow()
	s.Require().NoError(err)
	b.Record(p1, OutcomeSuccess, 0)
	s.Equal(StateHalfOpen, b.State())
	b.Record(p2, OutcomeSuccess, 0)
	s.Equal(StateClosed, b.State())
	snap := b.Snapshot()
	s.Equal(0, snap.Attempt)
	s.Equal(uint32(0), snap.InFlight)
	s.Equal(uint32(0), snap.Counts.Total)
	s.events.AssertCalled(s.T(), "OnStateChange", mock.MatchedBy(func(ev StateChangeEvent) bool {
		return ev.To == StateClosed && ev.Reason == ReasonHalfOpenSuccess && ev.Counts.Successes == 2
	}))
}

func (s *BreakerSuite) TestHalfOpenFailureReopens() {
	backoff := &MockBackoff{}
	backoff.On("Delay", 0).Return(testOpenDuration).Once()
	backoff.On("Delay", 1).Return(2 * testOpenDuration).Once()
	cfg := s.config()
	cfg.Backoff = backoff
	b := s.mustNew(cfg)
	p := s.halfOpen(b)
	b.Record(p, OutcomeFailure, 0)
	s.Equal(StateOpen, b.State())
	s.Equal(1, b.Snapshot().Attempt)
	s.events.AssertCalled(s.T(), "OnStateChange", mock.MatchedBy(func(ev StateChangeEvent) bool {
		return ev.From == StateHalfOpen && ev.To == StateOpen && ev.Reason == ReasonHalfOpenFailure && ev.Attempt == 1
	}))
	backoff.AssertExpectations(s.T())
}

func (s *BreakerSuite) TestStalePermitDiscarded() {
	b := s.mustNew(s.config())
	stale, err := b.Allow()
	s.Require().NoError(err)
	s.halfOpen(b)
	before := b.Snapshot()
	calls := s.onCallCount()
	b.Record(stale, OutcomeFailure, 0)
	after := b.Snapshot()
	s.Equal(StateHalfOpen, after.State)
	s.Equal(before.InFlight, after.InFlight)
	s.Equal(before.Epoch, after.Epoch)
	s.Equal(calls, s.onCallCount())
}

func (s *BreakerSuite) TestRecordAfterResetDiscarded() {
	b := s.mustNew(s.config())
	p, err := b.Allow()
	s.Require().NoError(err)
	b.Reset()
	b.Record(p, OutcomeFailure, 0)
	s.Equal(uint32(0), b.Counts().Failures)
}

func (s *BreakerSuite) TestIgnoredReleasesProbeSlot() {
	b := s.mustNew(s.config())
	p := s.halfOpen(b)
	calls := s.onCallCount()
	b.Record(p, OutcomeIgnored, 0)
	s.Equal(uint32(0), b.Snapshot().InFlight)
	s.Equal(StateHalfOpen, b.State())
	s.Equal(calls, s.onCallCount())
}

func (s *BreakerSuite) TestIgnoredInClosedIsLockFreeAndUncounted() {
	b := s.mustNew(s.config())
	p, err := b.Allow()
	s.Require().NoError(err)
	b.Record(p, OutcomeIgnored, 0)
	s.Equal(uint32(0), b.Counts().Total)
	s.events.AssertNotCalled(s.T(), "OnCall", mock.Anything)
}

func (s *BreakerSuite) TestTwoSlotRelease() {
	cfg := s.config()
	cfg.HalfOpenMaxCalls = 2
	cfg.HalfOpenSuccesses = 1
	b := s.mustNew(cfg)
	p1 := s.halfOpen(b)
	p2, err := b.Allow()
	s.Require().NoError(err)
	b.Record(p1, OutcomeIgnored, 0)
	b.Record(p2, OutcomeSuccess, 0)
	s.Equal(StateClosed, b.State())
	s.Equal(uint32(0), b.Snapshot().InFlight)
}

func (s *BreakerSuite) TestForceOpenPins() {
	b := s.mustNew(s.config())
	b.ForceOpen()
	s.Equal(StateOpen, b.State())
	s.True(b.Snapshot().Forced)
	s.clock.advance(time.Hour)
	_, err := b.Allow()
	s.ErrorIs(err, ErrOpen)
	s.Equal(StateOpen, b.State())
}

func (s *BreakerSuite) TestForceClosePinsAndRecords() {
	b := s.mustNew(s.config())
	b.ForceClose()
	for range testRejections {
		p, err := b.Allow()
		s.Require().NoError(err)
		b.Record(p, OutcomeFailure, 0)
	}
	s.Equal(StateClosed, b.State())
	s.Equal(uint32(testRejections), b.Counts().Failures)
	b.Reset()
	s.False(b.Snapshot().Forced)
	s.Equal(uint32(0), b.Counts().Failures)
	s.trip(b)
}

func (s *BreakerSuite) TestReopenEarly() {
	calls := 0
	cfg := s.config()
	cfg.ReopenEarly = func() bool { calls++; return true }
	b := s.mustNew(cfg)
	s.trip(b)
	p, err := b.Allow()
	s.NoError(err)
	s.True(p.IsProbe())
	s.Equal(1, calls)
	s.events.AssertCalled(s.T(), "OnStateChange", mock.MatchedBy(func(ev StateChangeEvent) bool {
		return ev.To == StateHalfOpen && ev.Reason == ReasonEarlyHalfOpen
	}))
	b.ForceOpen()
	_, err = b.Allow()
	s.ErrorIs(err, ErrOpen)
	s.Equal(1, calls)
}

func (s *BreakerSuite) TestReopenEarlyPanicIsFalse() {
	cfg := s.config()
	cfg.ReopenEarly = func() bool { panic("boom") }
	b := s.mustNew(cfg)
	s.trip(b)
	_, err := b.Allow()
	s.ErrorIs(err, ErrOpen)
	s.Equal(StateOpen, b.State())
	s.events.AssertCalled(s.T(), "OnHookPanic", "reopen-early", "boom")
}

func (s *BreakerSuite) TestClosingRecordPolicyTrips() {
	const slow = 10 * time.Millisecond
	cfg := s.config()
	cfg.Policies = []Policy{ConsecutiveFailures{Count: testTripCount}, SlowCallRate{Threshold: 0.5, MinCalls: 1}}
	cfg.SlowCallDuration = slow
	b := s.mustNew(cfg)
	p := s.halfOpen(b)
	b.Record(p, OutcomeSuccess, 2*slow)
	s.Equal(StateOpen, b.State())
	s.Equal(1, b.Snapshot().Attempt)
	s.events.AssertCalled(s.T(), "OnStateChange", mock.MatchedBy(func(ev StateChangeEvent) bool {
		return ev.From == StateHalfOpen && ev.To == StateOpen && ev.Reason == "slow-call-rate"
	}))
	s.events.AssertNotCalled(s.T(), "OnStateChange", mock.MatchedBy(func(ev StateChangeEvent) bool {
		return ev.To == StateClosed
	}))
}

func (s *BreakerSuite) TestPolicyPanicCountsFailureAndFiresHook() {
	policy := &MockPolicy{}
	policy.On("ShouldTrip", mock.Anything).Panic("policy boom")
	policy.On("Name").Return("mock").Maybe()
	cfg := s.config()
	cfg.Policies = []Policy{policy}
	b := s.mustNew(cfg)
	p, err := b.Allow()
	s.Require().NoError(err)
	s.NotPanics(func() { b.Record(p, OutcomeSuccess, 0) })
	s.Equal(StateClosed, b.State())
	s.events.AssertCalled(s.T(), "OnHookPanic", "policy", "policy boom")
}

func (s *BreakerSuite) TestHookPanicSwallowedAndReported() {
	tests := []struct {
		name          string
		reportPanics  bool
		expectedCount uint64
	}{
		{name: "reported through OnHookPanic", expectedCount: 1},
		{name: "panicking OnHookPanic is only counted", reportPanics: true, expectedCount: 2},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			events := &MockEvents{}
			events.On("OnCall", mock.Anything).Panic("hook boom").Once()
			report := events.On("OnHookPanic", "on-call", "hook boom").Once()
			if tt.reportPanics {
				report.Panic("report boom")
			}
			cfg := s.config()
			cfg.Events = events
			b := s.mustNew(cfg)
			p, err := b.Allow()
			s.Require().NoError(err)
			s.NotPanics(func() { b.Record(p, OutcomeSuccess, 0) })
			s.Equal(tt.expectedCount, b.HookPanics())
			events.AssertExpectations(s.T())
		})
	}
}

func (s *BreakerSuite) TestEveryHookPanicReportedByName() {
	tests := []struct {
		trigger      func(*Breaker)
		name         string
		method       string
		expectedHook string
	}{
		{name: "OnStateChange", method: "OnStateChange", expectedHook: "on-state-change", trigger: func(b *Breaker) { b.ForceOpen() }},
		{name: "OnReject", method: "OnReject", expectedHook: "on-reject", trigger: func(b *Breaker) {
			b.ForceOpen()
			p, err := b.Allow()
			s.ErrorIs(err, ErrOpen)
			s.False(p.IsProbe())
		}},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			events := &MockEvents{}
			events.On(tt.method, mock.Anything).Panic("hook boom").Once()
			events.On("OnStateChange", mock.Anything).Maybe()
			events.On("OnHookPanic", tt.expectedHook, "hook boom").Once()
			cfg := s.config()
			cfg.Events = events
			b := s.mustNew(cfg)
			s.NotPanics(func() { tt.trigger(b) })
			s.Equal(uint64(1), b.HookPanics())
			events.AssertExpectations(s.T())
		})
	}
}

func (s *BreakerSuite) TestLazyStateView() {
	b := s.mustNew(s.config())
	s.trip(b)
	epoch := b.Snapshot().Epoch
	s.clock.advance(testOpenDuration)
	s.Equal(StateHalfOpen, b.State())
	s.Equal(epoch, b.Snapshot().Epoch)
	s.events.AssertNotCalled(s.T(), "OnStateChange", mock.MatchedBy(func(ev StateChangeEvent) bool {
		return ev.To == StateHalfOpen
	}))
	p, err := b.Allow()
	s.NoError(err)
	s.True(p.IsProbe())
	s.Equal(epoch+1, b.Snapshot().Epoch)
}

func (s *BreakerSuite) TestProbeTimeoutReopens() {
	b := s.mustNew(s.config())
	s.halfOpen(b)
	s.clock.advance(testProbeTimeout + time.Millisecond)
	_, err := b.Allow()
	s.ErrorIs(err, ErrOpen)
	snap := b.Snapshot()
	s.Equal(StateOpen, snap.State)
	s.Equal(1, snap.Attempt)
	s.Equal(uint32(0), snap.InFlight)
	s.events.AssertCalled(s.T(), "OnStateChange", mock.MatchedBy(func(ev StateChangeEvent) bool {
		return ev.Reason == ReasonProbeTimeout
	}))
}

func (s *BreakerSuite) TestProbeAdmittedAtBaseInstantIsOccupied() {
	b := s.mustNew(s.config())
	s.halfOpen(b)
	_, err := b.Allow()
	s.ErrorIs(err, ErrTooManyProbes)
}

func (s *BreakerSuite) TestOpenToHalfOpenCAS() {
	b := s.mustNew(s.config())
	s.trip(b)
	s.clock.advance(testOpenDuration)
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		probes int
	)
	for range testGoroutines {
		wg.Go(func() {
			p, err := b.Allow()
			if err == nil && p.IsProbe() {
				mu.Lock()
				probes++
				mu.Unlock()
				return
			}
			if !errors.Is(err, ErrTooManyProbes) {
				s.T().Errorf("unexpected error: %v", err)
			}
		})
	}
	wg.Wait()
	s.Equal(1, probes)
	s.events.AssertNumberOfCalls(s.T(), "OnStateChange", 2)
}

func (s *BreakerSuite) TestStaleWaiterDoesNotSkipBackoff() {
	b := s.mustNew(s.config())
	s.trip(b)
	s.clock.advance(testOpenDuration)
	now := s.clock.Now()
	staleEpoch, _ := b.state()
	p, err := b.Allow()
	s.Require().NoError(err)
	b.Record(p, OutcomeFailure, 0)
	s.Require().Equal(StateOpen, b.State())
	expected := b.Snapshot()

	actual, err := b.toHalfOpen(staleEpoch, now, ReasonTimeout)

	s.Require().ErrorIs(err, ErrOpen)
	s.False(actual.IsProbe())
	s.Equal(StateOpen, b.State())
	s.Equal(expected.Epoch, b.Snapshot().Epoch)
	s.Equal(expected.Attempt, b.Snapshot().Attempt)
}
