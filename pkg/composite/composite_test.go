package composite

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

const (
	testName          = "dep"
	testChildOpen     = 5 * time.Second
	testParentOpen    = 30 * time.Second
	testLongChildOpen = testParentOpen + testChildOpen
	testScanInterval  = 10 * time.Millisecond
	testWindowDrain   = 11 * time.Second
	testRateCalls     = 50
	testRateFailures  = 30
	testLowFailures   = 10
	testNeverTrip     = 1 << 30
	testHalfOpenCalls = 1
	testAttachRounds  = 500
	testReentryWait   = time.Second
)

// baseSuite holds the fixture shared by the composite suites. It has no test
// methods of its own.
type baseSuite struct {
	suite.Suite
	clock     *testClock
	events    *MockEvents
	childTrip uint32
	childOpen time.Duration
}

type CompositeSuite struct {
	baseSuite
}

func TestCompositeSuite(t *testing.T) {
	suite.Run(t, new(CompositeSuite))
}

func (s *baseSuite) SetupTest() {
	s.clock = newTestClock()
	s.events = newNoopMockEvents()
	s.childTrip = 1
	s.childOpen = testChildOpen
}

func (s *baseSuite) TearDownTest() {
	s.events.AssertExpectations(s.T())
}

func (s *baseSuite) childOpts() []breaker.Option {
	return []breaker.Option{
		breaker.WithPolicies(breaker.ConsecutiveFailures{Count: s.childTrip}),
		breaker.WithOpenDuration(s.childOpen),
		breaker.WithClock(s.clock.Now),
	}
}

func (s *baseSuite) opts(name string, trip TripFunc) []Option {
	out := []Option{
		WithChild(s.childOpts()...),
		WithScanInterval(testScanInterval),
		WithEvents(s.events),
		WithParent(
			breaker.WithOpenDuration(testParentOpen),
			breaker.WithHalfOpenMaxCalls(testHalfOpenCalls),
			breaker.WithClock(s.clock.Now),
		),
	}
	if name != "" {
		out = append(out, WithName(name))
	}
	if trip != nil {
		out = append(out, WithTrip(trip))
	}
	return out
}

// openChildren builds an OpenChildren trip that must be valid.
func (s *baseSuite) openChildren(minOpen int) TripFunc {
	s.T().Helper()
	trip, err := OpenChildren(minOpen)
	s.Require().NoError(err)
	return trip
}

// mustNew builds a composite with n leaves named a, b, c, ...
func (s *baseSuite) mustNew(trip TripFunc, leaves ...string) (c *Composite, byID map[string]*breaker.Breaker) {
	s.T().Helper()
	return s.mustNewNamed(testName, trip, leaves...)
}

// mustNewNamed builds a composite named name with the given leaves attached to it.
func (s *baseSuite) mustNewNamed(name string, trip TripFunc, leaves ...string) (c *Composite, byID map[string]*breaker.Breaker) {
	s.T().Helper()
	c, err := New(s.opts(name, trip)...)
	s.Require().NoError(err)
	byID = make(map[string]*breaker.Breaker, len(leaves))
	for _, id := range leaves {
		b, err := c.AddChild(id)
		s.Require().NoError(err)
		byID[id] = b
	}
	return c, byID
}

// trip opens b through its own policy, then lets the scan cache expire.
func (s *baseSuite) trip(b *breaker.Breaker) {
	s.T().Helper()
	p, err := b.Allow()
	s.Require().NoError(err)
	b.Record(p, breaker.OutcomeFailure, 0, nil)
	s.Require().Equal(breaker.StateOpen, b.State())
	s.clock.advance(testScanInterval)
}

// forceOpen forces b Open, then lets the scan cache expire.
func (s *baseSuite) forceOpen(b *breaker.Breaker) {
	s.T().Helper()
	b.ForceOpen()
	s.clock.advance(testScanInterval)
}

// record runs n calls through c with the given number of failures.
func (s *baseSuite) record(c *Composite, n, failures int) {
	s.T().Helper()
	for i := range n {
		l, err := c.Pick()
		s.Require().NoError(err)
		outcome := breaker.OutcomeSuccess
		if i < failures {
			outcome = breaker.OutcomeFailure
		}
		c.RecordLease(l, outcome, 0, nil)
	}
}

func (s *CompositeSuite) TestInvalidConfig() {
	tests := []struct {
		expectedErr error
		name        string
		opts        []Option
	}{
		{
			name: "parent policies",
			opts: append(s.opts(testName, nil), WithParent(
				breaker.WithOpenDuration(testParentOpen),
				breaker.WithHalfOpenMaxCalls(testHalfOpenCalls),
				breaker.WithClock(s.clock.Now),
				breaker.WithPolicies(breaker.ConsecutiveFailures{Count: 1}),
			)),
			expectedErr: ErrInvalidConfig,
		},
		{
			name: "parent reopen early",
			opts: append(s.opts(testName, nil), WithParent(
				breaker.WithOpenDuration(testParentOpen),
				breaker.WithHalfOpenMaxCalls(testHalfOpenCalls),
				breaker.WithClock(s.clock.Now),
				breaker.WithReopenEarly(func() bool { return true }),
			)),
			expectedErr: ErrInvalidConfig,
		},
		{name: "empty name", opts: s.opts("", nil), expectedErr: breaker.ErrEmptyName},
		{name: "negative scan interval", opts: append(s.opts(testName, nil), WithScanInterval(-time.Second)), expectedErr: ErrInvalidConfig},
		{
			name: "parent negative duration",
			opts: append(s.opts(testName, nil), WithParent(
				breaker.WithOpenDuration(-time.Second),
				breaker.WithHalfOpenMaxCalls(testHalfOpenCalls),
				breaker.WithClock(s.clock.Now),
			)),
			expectedErr: breaker.ErrNegativeDuration,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			c, err := New(tt.opts...)
			s.ErrorIs(err, tt.expectedErr)
			s.Nil(c)
		})
	}
}

func (s *CompositeSuite) TestParentNameFallsBackToParentConfig() {
	opts := s.opts("", nil)
	opts = append(opts, WithParent(
		breaker.WithName("from-parent"),
		breaker.WithOpenDuration(testParentOpen),
		breaker.WithHalfOpenMaxCalls(testHalfOpenCalls),
		breaker.WithClock(s.clock.Now),
	))
	c, err := New(opts...)
	s.Require().NoError(err)
	s.Equal("from-parent", c.Name())
}

func (s *CompositeSuite) TestAddChild() {
	c, _ := s.mustNew(nil)
	b, err := c.AddChild("a")
	s.Require().NoError(err)
	s.Equal("a", b.Name())
	actual, ok := c.Child("a")
	s.True(ok)
	s.Same(b, actual)
	s.Equal([]string{"a"}, c.Children())

	_, err = c.AddChild("a")
	s.ErrorIs(err, ErrDuplicateChild)
	s.Equal([]string{"a"}, c.Children())
}

func (s *CompositeSuite) TestAddChildDefaults() {
	c, err := New(WithName(testName), WithEvents(s.events), WithParent(breaker.WithClock(s.clock.Now)))
	s.Require().NoError(err)
	b, err := c.AddChild("a")
	s.Require().NoError(err)
	s.Equal("a", b.Name())
}

func (s *CompositeSuite) TestAttachChild() {
	var nilComposite *Composite
	var nilBreaker *breaker.Breaker
	leaf, err := breaker.New(append(s.childOpts(), breaker.WithName("leaf"))...)
	s.Require().NoError(err)
	tests := []struct {
		child       Child
		expectedErr error
		name        string
	}{
		{name: "nil interface", child: nil, expectedErr: ErrNilChild},
		{name: "nil composite", child: nilComposite, expectedErr: ErrNilChild},
		{name: "nil breaker", child: nilBreaker, expectedErr: ErrNilChild},
		{name: "unnamed leaf", child: leaf, expectedErr: nil},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			c, _ := s.mustNew(nil)
			err := c.AttachChild(tt.child)
			s.ErrorIs(err, tt.expectedErr)
		})
	}
}

func (s *CompositeSuite) TestAttachLeafToManyParents() {
	leaf, err := breaker.New(append(s.childOpts(), breaker.WithName("shared"))...)
	s.Require().NoError(err)
	first, _ := s.mustNew(nil)
	second, err := New(s.opts("second", nil)...)
	s.Require().NoError(err)
	s.NoError(first.AttachChild(leaf))
	s.NoError(second.AttachChild(leaf))
}

func (s *CompositeSuite) TestAttachTwiceRejected() {
	sub, err := New(s.opts("sub", nil)...)
	s.Require().NoError(err)
	first, _ := s.mustNew(nil)
	second, err := New(s.opts("second", nil)...)
	s.Require().NoError(err)
	s.NoError(first.AttachChild(sub))
	s.ErrorIs(second.AttachChild(sub), ErrAlreadyAttached)
	s.ErrorIs(first.AttachChild(sub), ErrDuplicateChild)
	s.True(first.RemoveChild("sub"))
	s.NoError(second.AttachChild(sub))
}

func (s *CompositeSuite) TestAttachAncestorRejected() {
	root, _ := s.mustNew(nil)
	sub, err := New(s.opts("sub", nil)...)
	s.Require().NoError(err)
	s.Require().NoError(root.AttachChild(sub))
	s.ErrorIs(sub.AttachChild(root), ErrCycle)
	s.ErrorIs(root.AttachChild(root), ErrCycle)
}

func (s *CompositeSuite) TestRemoveChild() {
	c, leaves := s.mustNew(nil, "a", "b")
	s.True(c.RemoveChild("a"))
	s.False(c.RemoveChild("a"))
	s.Equal([]string{"b"}, c.Children())
	_, ok := c.Child("a")
	s.False(ok)
	s.NoError(c.AttachChild(leaves["a"]))
	s.Equal([]string{"b", "a"}, c.Children())
}

func (s *CompositeSuite) TestRemoveInvalidatesScan() {
	c, leaves := s.mustNew(nil, "a", "b")
	s.trip(leaves["a"])
	c.refreshScan()
	s.Equal(1, c.cachedOpen())
	s.True(c.RemoveChild("a"))
	s.Equal(0, c.cachedOpen())
}

func (s *CompositeSuite) TestScanThrottled() {
	c, leaves := s.mustNew(nil, "a", "b")
	p, err := leaves["a"].Allow()
	s.Require().NoError(err)
	leaves["a"].Record(p, breaker.OutcomeFailure, 0, nil)
	s.Require().Equal(breaker.StateOpen, leaves["a"].State())
	c.refreshScan()
	s.Equal(0, c.cachedOpen(), "inside the interval the cache is kept")
	s.clock.advance(testScanInterval)
	c.refreshScan()
	s.Equal(1, c.cachedOpen())
}

func (s *CompositeSuite) TestEventsDefaults() {
	parentEvents := newNoopMockEvents()
	childEvents := newNoopMockEvents()
	tests := []struct {
		expectedParent *MockEvents
		expectedChild  *MockEvents
		name           string
		parentOpts     []breaker.Option
		childOpts      []breaker.Option
	}{
		{name: "both nil use composite events", expectedParent: s.events, expectedChild: s.events},
		{
			name:           "own events kept",
			parentOpts:     []breaker.Option{breaker.WithEvents(parentEvents)},
			childOpts:      []breaker.Option{breaker.WithEvents(childEvents)},
			expectedParent: parentEvents,
			expectedChild:  childEvents,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			parent := []breaker.Option{
				breaker.WithOpenDuration(testParentOpen),
				breaker.WithHalfOpenMaxCalls(testHalfOpenCalls),
				breaker.WithClock(s.clock.Now),
			}
			parent = append(parent, tt.parentOpts...)
			child := append(s.childOpts(), tt.childOpts...)
			c, err := New(
				WithName(testName),
				WithTrip(s.openChildren(1)),
				WithScanInterval(testScanInterval),
				WithEvents(s.events),
				WithParent(parent...),
				WithChild(child...),
			)
			s.Require().NoError(err)
			leaf, err := c.AddChild("a")
			s.Require().NoError(err)
			s.trip(leaf)
			tt.expectedChild.AssertCalled(s.T(), "OnStateChange", mock.MatchedBy(func(ev breaker.StateChangeEvent) bool {
				return ev.Name == "a" && ev.To == breaker.StateOpen
			}))
			c.refreshScan()
			_, err = c.Pick()
			s.Require().ErrorIs(err, ErrNoHealthyChildren)
			tt.expectedParent.AssertCalled(s.T(), "OnReject", mock.MatchedBy(func(ev breaker.RejectEvent) bool {
				return ev.Name == testName && errors.Is(ev.Err, ErrNoHealthyChildren) && ev.State == breaker.StateOpen
			}))
		})
	}
}

func (s *CompositeSuite) TestDefaultTrip() {
	s.childTrip = testNeverTrip
	tests := []struct {
		name          string
		openLeaves    []string
		failures      int
		expectedState breaker.State
	}{
		{name: "one of three open high rate", openLeaves: []string{"a"}, failures: testRateFailures, expectedState: breaker.StateClosed},
		{name: "two of three open low rate", openLeaves: []string{"a", "b"}, failures: testLowFailures, expectedState: breaker.StateClosed},
		{name: "two of three open high rate", openLeaves: []string{"a", "b"}, failures: testRateFailures, expectedState: breaker.StateOpen},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.SetupTest()
			s.childTrip = testNeverTrip
			c, leaves := s.mustNew(nil, "a", "b", "c")
			for _, id := range tt.openLeaves {
				leaves[id].ForceOpen()
			}
			s.clock.advance(testScanInterval)
			s.record(c, testRateCalls, tt.failures)
			s.Equal(tt.expectedState, c.State())
		})
	}
}

func (s *CompositeSuite) TestTripFuncMayReenterPick() {
	var c *Composite
	reentered := false
	trip := func(int, int, breaker.Counts) bool {
		if !reentered {
			reentered = true
			lease, err := c.Pick()
			if err == nil {
				c.RecordLease(lease, breaker.OutcomeSuccess, 0, nil)
			}
		}
		return true
	}
	c, _ = s.mustNew(trip, "a")
	permit, err := c.Allow()
	s.Require().NoError(err)

	done := make(chan struct{})
	go func() {
		c.Record(permit, breaker.OutcomeFailure, 0, nil)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(testReentryWait):
		s.FailNow("Record did not complete after TripFunc re-entered Pick")
	}
	s.Equal(breaker.StateOpen, c.State())
}

func (s *CompositeSuite) TestTripFreezesAggregate() {
	s.childTrip = testNeverTrip
	c, leaves := s.mustNew(nil, "a", "b", "c")
	leaves["a"].ForceOpen()
	leaves["b"].ForceOpen()
	s.clock.advance(testScanInterval)
	s.False(c.frozen())
	s.record(c, testRateCalls, testRateFailures)
	s.Require().Equal(breaker.StateOpen, c.State())
	s.True(c.frozen())
}

func (s *CompositeSuite) TestReopenEarlyWhenChildrenRecover() {
	c, leaves := s.mustNew(s.openChildren(2), "a", "b", "c")
	s.trip(leaves["a"])
	s.trip(leaves["b"])
	s.record(c, 1, 1)
	s.Require().Equal(breaker.StateOpen, c.State())

	s.clock.advance(testChildOpen)
	s.Equal(breaker.StateHalfOpen, leaves["a"].State())
	l, err := c.Pick()
	s.Require().NoError(err)
	s.Equal(breaker.StateHalfOpen, c.State())
	s.events.AssertCalled(s.T(), "OnStateChange", mock.MatchedBy(func(ev breaker.StateChangeEvent) bool {
		return ev.Name == testName && ev.To == breaker.StateHalfOpen && ev.Reason == breaker.ReasonEarlyHalfOpen
	}))
	c.RecordLease(l, breaker.OutcomeSuccess, 0, nil)
	s.Equal(breaker.StateClosed, c.State())
	s.False(c.frozen())
}

func (s *CompositeSuite) TestFrozenAggregateKeepsParentOpen() {
	s.childTrip = testNeverTrip
	c, leaves := s.mustNew(nil, "a", "b", "c")
	leaves["a"].ForceOpen()
	leaves["b"].ForceOpen()
	s.clock.advance(testScanInterval)
	s.record(c, testRateCalls, testRateFailures)
	s.Require().Equal(breaker.StateOpen, c.State())

	s.clock.advance(testWindowDrain)
	s.Equal(uint32(0), c.Counts().Total, "the live window has drained")
	_, err := c.Pick()
	s.ErrorIs(err, breaker.ErrOpen)
	s.Equal(breaker.StateOpen, c.State())
}

func (s *CompositeSuite) TestClosingRecheckReopens() {
	s.childTrip = testNeverTrip
	c, leaves := s.mustNew(s.openChildren(2), "a", "b", "c")
	s.forceOpen(leaves["a"])
	s.forceOpen(leaves["b"])
	s.record(c, 1, 1)
	s.Require().Equal(breaker.StateOpen, c.State())

	s.clock.advance(testParentOpen)
	l, err := c.Pick()
	s.Require().NoError(err)
	s.Equal("c", l.ID())
	s.Equal(breaker.StateHalfOpen, c.State())
	c.RecordLease(l, breaker.OutcomeSuccess, 0, nil)
	snap := c.Snapshot()
	s.Equal(breaker.StateOpen, snap.State)
	s.Equal(1, snap.Attempt)
	s.events.AssertNotCalled(s.T(), "OnStateChange", mock.MatchedBy(func(ev breaker.StateChangeEvent) bool {
		return ev.Name == testName && ev.To == breaker.StateClosed
	}))
	s.True(c.frozen())
}

func (s *CompositeSuite) TestFrozenAggregateUsesTripTimeCounts() {
	s.childTrip = testNeverTrip
	const windowSize = 10 * time.Second
	var drained bool
	trip := func(_ int, _ int, agg breaker.Counts) bool {
		if !drained && agg.Failures > 0 {
			drained = true
			s.clock.advance(windowSize)
			return true
		}
		return agg.Failures > 0
	}
	opts := append(s.opts(testName, trip), WithParent(
		breaker.WithOpenDuration(testParentOpen),
		breaker.WithHalfOpenMaxCalls(testHalfOpenCalls),
		breaker.WithClock(s.clock.Now),
		breaker.WithWindow(breaker.SlidingTimeWindow{Size: windowSize, Buckets: 10}),
	))
	c, err := New(opts...)
	s.Require().NoError(err)
	_, err = c.AddChild("a")
	s.Require().NoError(err)
	s.record(c, 1, 1)
	s.Require().Equal(breaker.StateOpen, c.State())
	s.Equal(uint32(0), c.Counts().Total, "the live window drained during ShouldTrip")
	frozen := c.tripAggregate.Load()
	s.Require().NotNil(frozen)
	s.Equal(uint32(1), frozen.Total, "frozen aggregate must be the trip-time snapshot")
	s.Equal(uint32(1), frozen.Failures)
}

func (s *CompositeSuite) TestHalfOpenFailureKeepsTripAggregate() {
	s.childTrip = testNeverTrip
	s.childOpen = testLongChildOpen
	c, leaves := s.mustNew(nil, "a", "b", "c")
	s.forceOpen(leaves["a"])
	s.forceOpen(leaves["b"])
	s.record(c, testRateCalls, testRateFailures)
	s.Require().Equal(breaker.StateOpen, c.State())
	frozen := c.tripAggregate.Load()
	s.Require().NotNil(frozen)
	s.Equal(uint32(testRateCalls), frozen.Total)

	s.clock.advance(testParentOpen)
	l, err := c.Pick()
	s.Require().NoError(err)
	s.Equal("c", l.ID())
	c.RecordLease(l, breaker.OutcomeFailure, 0, nil)
	s.Require().Equal(breaker.StateOpen, c.State())

	frozen = c.tripAggregate.Load()
	s.Require().NotNil(frozen)
	s.Equal(uint32(testRateCalls), frozen.Total)
	_, err = c.Pick()
	s.ErrorIs(err, breaker.ErrOpen)
	s.Equal(breaker.StateOpen, c.State())
}

func (s *CompositeSuite) TestTripAggregateClearedOnClose() {
	c, leaves := s.mustNew(s.openChildren(1), "a", "b")
	s.trip(leaves["a"])
	s.record(c, 1, 1)
	s.Require().Equal(breaker.StateOpen, c.State())
	s.True(c.frozen())

	s.clock.advance(testParentOpen)
	l, err := c.Pick()
	s.Require().NoError(err)
	c.RecordLease(l, breaker.OutcomeSuccess, 0, nil)
	s.Equal(breaker.StateClosed, c.State())
	s.False(c.frozen())
	s.events.AssertCalled(s.T(), "OnStateChange", mock.MatchedBy(func(ev breaker.StateChangeEvent) bool {
		return ev.Name == testName && ev.To == breaker.StateClosed
	}))
}

func (s *CompositeSuite) TestForceAndReset() {
	c, _ := s.mustNew(nil, "a")
	c.ForceOpen()
	s.Equal(breaker.StateOpen, c.State())
	_, err := c.Pick()
	s.ErrorIs(err, breaker.ErrOpen)
	c.ForceClose()
	s.Equal(breaker.StateClosed, c.State())
	c.Reset()
	s.Equal(breaker.StateClosed, c.State())
}

func (s *CompositeSuite) TestConcurrentCrossAttachNeverCycles() {
	for range testAttachRounds {
		a, err := New(s.opts("a", nil)...)
		s.Require().NoError(err)
		b, err := New(s.opts("b", nil)...)
		s.Require().NoError(err)
		var (
			wg    sync.WaitGroup
			errAB error
			errBA error
		)
		wg.Go(func() { errAB = a.AttachChild(b) })
		wg.Go(func() { errBA = b.AttachChild(a) })
		wg.Wait()
		s.Require().False(errAB == nil && errBA == nil, "both attaches succeeded: cycle")
	}
}
