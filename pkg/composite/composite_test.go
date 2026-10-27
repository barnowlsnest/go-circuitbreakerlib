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
	testScanInterval  = 10 * time.Millisecond
	testWindowDrain   = 11 * time.Second
	testRateCalls     = 50
	testRateFailures  = 30
	testLowFailures   = 10
	testNeverTrip     = 1 << 30
	testHalfOpenCalls = 1
	testAttachRounds  = 500
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

func (s *baseSuite) template(id string) breaker.Config {
	return breaker.Config{
		Name:         id,
		Policies:     []breaker.Policy{breaker.ConsecutiveFailures{Count: s.childTrip}},
		OpenDuration: s.childOpen,
		Clock:        s.clock.Now,
	}
}

func (s *baseSuite) config(name string, trip TripFunc) Config {
	return Config{
		ChildTemplate: s.template,
		Trip:          trip,
		ScanInterval:  testScanInterval,
		Events:        s.events,
		Parent: breaker.Config{
			OpenDuration:     testParentOpen,
			HalfOpenMaxCalls: testHalfOpenCalls,
			Clock:            s.clock.Now,
		},
		Name: name,
	}
}

// mustNew builds a composite with n leaves named a, b, c, ...
func (s *baseSuite) mustNew(trip TripFunc, leaves ...string) (c *Composite, byID map[string]*breaker.Breaker) {
	s.T().Helper()
	c, err := New(s.config(testName, trip))
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
	b.Record(p, breaker.OutcomeFailure, 0)
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
		c.RecordLease(l, outcome, 0)
	}
}

func (s *CompositeSuite) TestInvalidConfig() {
	tests := []struct {
		expectedErr error
		mutate      func(*Config)
		name        string
	}{
		{
			name:        "parent policies",
			mutate:      func(c *Config) { c.Parent.Policies = []breaker.Policy{breaker.ConsecutiveFailures{Count: 1}} },
			expectedErr: ErrInvalidConfig,
		},
		{name: "empty name", mutate: func(c *Config) { c.Name = "" }, expectedErr: breaker.ErrEmptyName},
		{name: "negative scan interval", mutate: func(c *Config) { c.ScanInterval = -time.Second }, expectedErr: ErrInvalidConfig},
		{
			name:        "parent negative duration",
			mutate:      func(c *Config) { c.Parent.OpenDuration = -time.Second },
			expectedErr: breaker.ErrNegativeDuration,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			cfg := s.config(testName, nil)
			tt.mutate(&cfg)
			c, err := New(cfg)
			s.ErrorIs(err, tt.expectedErr)
			s.Nil(c)
		})
	}
}

func (s *CompositeSuite) TestParentNameFallsBackToParentConfig() {
	cfg := s.config("", nil)
	cfg.Parent.Name = "from-parent"
	c, err := New(cfg)
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

func (s *CompositeSuite) TestAddChildWithoutTemplate() {
	cfg := s.config(testName, nil)
	cfg.ChildTemplate = nil
	c, err := New(cfg)
	s.Require().NoError(err)
	_, err = c.AddChild("a")
	s.ErrorIs(err, ErrNoChildTemplate)
}

func (s *CompositeSuite) TestAttachChild() {
	var nilComposite *Composite
	var nilBreaker *breaker.Breaker
	leaf, err := breaker.New(s.template("leaf"))
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
	leaf, err := breaker.New(s.template("shared"))
	s.Require().NoError(err)
	first, _ := s.mustNew(nil)
	second, err := New(s.config("second", nil))
	s.Require().NoError(err)
	s.NoError(first.AttachChild(leaf))
	s.NoError(second.AttachChild(leaf))
}

func (s *CompositeSuite) TestAttachTwiceRejected() {
	sub, err := New(s.config("sub", nil))
	s.Require().NoError(err)
	first, _ := s.mustNew(nil)
	second, err := New(s.config("second", nil))
	s.Require().NoError(err)
	s.NoError(first.AttachChild(sub))
	s.ErrorIs(second.AttachChild(sub), ErrAlreadyAttached)
	s.ErrorIs(first.AttachChild(sub), ErrDuplicateChild)
	s.True(first.RemoveChild("sub"))
	s.NoError(second.AttachChild(sub))
}

func (s *CompositeSuite) TestAttachAncestorRejected() {
	root, _ := s.mustNew(nil)
	sub, err := New(s.config("sub", nil))
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
	leaves["a"].Record(p, breaker.OutcomeFailure, 0)
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
		parentEvents   breaker.Events
		childEvents    breaker.Events
		expectedParent *MockEvents
		expectedChild  *MockEvents
		name           string
	}{
		{name: "both nil use composite events", expectedParent: s.events, expectedChild: s.events},
		{name: "own events kept", parentEvents: parentEvents, childEvents: childEvents, expectedParent: parentEvents, expectedChild: childEvents},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			cfg := s.config(testName, OpenChildren(1))
			cfg.Parent.Events = tt.parentEvents
			cfg.ChildTemplate = func(id string) breaker.Config {
				t := s.template(id)
				t.Events = tt.childEvents
				return t
			}
			c, err := New(cfg)
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
				return ev.Name == testName && errors.Is(ev.Err, ErrNoHealthyChildren) && ev.State == breaker.StateClosed
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
	c, leaves := s.mustNew(OpenChildren(2), "a", "b", "c")
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
	c.RecordLease(l, breaker.OutcomeSuccess, 0)
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
	c, leaves := s.mustNew(OpenChildren(2), "a", "b", "c")
	s.forceOpen(leaves["a"])
	s.forceOpen(leaves["b"])
	s.record(c, 1, 1)
	s.Require().Equal(breaker.StateOpen, c.State())

	s.clock.advance(testParentOpen)
	l, err := c.Pick()
	s.Require().NoError(err)
	s.Equal("c", l.ID())
	s.Equal(breaker.StateHalfOpen, c.State())
	c.RecordLease(l, breaker.OutcomeSuccess, 0)
	snap := c.Snapshot()
	s.Equal(breaker.StateOpen, snap.State)
	s.Equal(1, snap.Attempt)
	s.events.AssertNotCalled(s.T(), "OnStateChange", mock.MatchedBy(func(ev breaker.StateChangeEvent) bool {
		return ev.Name == testName && ev.To == breaker.StateClosed
	}))
	s.True(c.frozen())
}

func (s *CompositeSuite) TestTripAggregateClearedOnClose() {
	c, leaves := s.mustNew(OpenChildren(1), "a", "b")
	s.trip(leaves["a"])
	s.record(c, 1, 1)
	s.Require().Equal(breaker.StateOpen, c.State())
	s.True(c.frozen())

	s.clock.advance(testParentOpen)
	l, err := c.Pick()
	s.Require().NoError(err)
	c.RecordLease(l, breaker.OutcomeSuccess, 0)
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
		a, err := New(s.config("a", nil))
		s.Require().NoError(err)
		b, err := New(s.config("b", nil))
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
