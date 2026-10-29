package composite

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

const (
	testPicks      = 6
	testRaceRounds = 200
	testRaceWorker = 4
)

var errBoom = errors.New("boom")

type LeaseSuite struct {
	fn *MockFunc
	baseSuite
}

func TestLeaseSuite(t *testing.T) {
	suite.Run(t, new(LeaseSuite))
}

func (s *LeaseSuite) SetupTest() {
	s.baseSuite.SetupTest()
	s.childTrip = testNeverTrip
	s.fn = &MockFunc{}
}

func (s *LeaseSuite) TearDownTest() {
	s.baseSuite.TearDownTest()
	s.fn.AssertExpectations(s.T())
}

func (s *LeaseSuite) TestPickRoundRobinSkipsOpen() {
	c, leaves := s.mustNew(nil, "a", "b", "c")
	s.forceOpen(leaves["b"])
	var picked []string
	for range testPicks {
		l, err := c.Pick()
		s.Require().NoError(err)
		picked = append(picked, l.ID())
		c.RecordLease(l, breaker.OutcomeSuccess, 0)
	}
	for i, id := range picked {
		s.Contains([]string{"a", "c"}, id)
		if i > 0 {
			s.NotEqual(picked[i-1], id, "round robin alternates between the healthy leaves")
		}
	}
}

func (s *LeaseSuite) TestPickRejected() {
	tests := []struct {
		expectedErr   error
		name          string
		leaves        []string
		expectedState breaker.State
		forceParent   bool
		forceLeaves   bool
	}{
		{name: "parent open", leaves: []string{"a"}, forceParent: true, expectedErr: breaker.ErrOpen, expectedState: breaker.StateOpen},
		{
			name:          "all children open",
			leaves:        []string{"a", "b"},
			forceLeaves:   true,
			expectedErr:   ErrNoHealthyChildren,
			expectedState: breaker.StateClosed,
		},
		{name: "no children", leaves: nil, expectedErr: ErrNoHealthyChildren, expectedState: breaker.StateClosed},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			c, leaves := s.mustNew(nil, tt.leaves...)
			if tt.forceParent {
				c.ForceOpen()
			}
			if tt.forceLeaves {
				for _, b := range leaves {
					s.forceOpen(b)
				}
			}
			l, err := c.Pick()
			s.Require().ErrorIs(err, tt.expectedErr)
			s.ErrorIs(err, breaker.ErrOpen)
			var re *breaker.RejectedError
			s.Require().ErrorAs(err, &re)
			s.Equal(testName, re.Name)
			s.Equal(tt.expectedState, re.State)
			s.Nil(l.Leaf)
			s.Empty(l.ID())
			s.Nil(l.Path())
			s.Equal(uint32(0), c.Snapshot().InFlight)
		})
	}
}

func (s *LeaseSuite) TestRecordHitsLeafAndParent() {
	c, leaves := s.mustNew(nil, "a")
	l, err := c.Pick()
	s.Require().NoError(err)
	s.Equal([]string{testName, "a"}, l.Path())
	c.RecordLease(l, breaker.OutcomeFailure, 0)
	s.Equal(breaker.Counts{Total: 1, Failures: 1, ConsecutiveFailures: 1}, leaves["a"].Counts())
	s.Equal(breaker.Counts{Total: 1, Failures: 1, ConsecutiveFailures: 1}, c.Counts())
}

func (s *LeaseSuite) TestZeroLeaseIsInert() {
	c, _ := s.mustNew(nil, "a")
	var l Lease
	s.NotPanics(func() { c.RecordLease(l, breaker.OutcomeFailure, 0) })
	s.Equal(uint32(0), c.Counts().Total)
}

func (s *LeaseSuite) TestNestedRecordWalksPath() {
	root, _ := s.mustNew(nil)
	mid, _ := s.mustNewNamed("mid", OpenChildren(1))
	leafOwner, leaves := s.mustNewNamed("inner", OpenChildren(1), "leaf")
	s.Require().NoError(mid.AttachChild(leafOwner))
	s.Require().NoError(root.AttachChild(mid))

	l, err := root.Pick()
	s.Require().NoError(err)
	s.Equal([]string{testName, "mid", "inner", "leaf"}, l.Path())
	s.Equal("leaf", l.ID())
	root.RecordLease(l, breaker.OutcomeFailure, 0)
	for name, counted := range map[string]interface{ Counts() breaker.Counts }{
		"root": root, "mid": mid, "inner": leafOwner, "leaf": leaves["leaf"],
	} {
		s.Equal(uint32(1), counted.Counts().Failures, name)
	}
}

func (s *LeaseSuite) TestNestedPickReleasesIntermediatePermit() {
	root, _ := s.mustNew(nil)
	first, firstLeaves := s.mustNewNamed("first", OpenChildren(1), "a", "b")
	second, secondLeaves := s.mustNewNamed("second", OpenChildren(1), "c", "d")
	s.Require().NoError(root.AttachChild(first))
	s.Require().NoError(root.AttachChild(second))
	for _, leaves := range []map[string]*breaker.Breaker{firstLeaves, secondLeaves} {
		for _, b := range leaves {
			s.forceOpen(b)
		}
	}
	// Both sub-composites trip on their open leaves, then reach HalfOpen.
	for _, sub := range []*Composite{first, second} {
		_, err := sub.Pick()
		s.Require().ErrorIs(err, ErrNoHealthyChildren)
		p, err := sub.Allow()
		s.Require().NoError(err)
		sub.Record(p, breaker.OutcomeFailure, 0)
		s.Require().Equal(breaker.StateOpen, sub.State())
	}
	s.clock.advance(testParentOpen)
	s.Require().Equal(breaker.StateHalfOpen, first.State())

	_, err := root.Pick()
	s.ErrorIs(err, ErrNoHealthyChildren)
	for _, c := range []*Composite{root, first, second} {
		s.Equal(uint32(0), c.Snapshot().InFlight, c.Name())
	}
	_, err = root.Pick()
	s.ErrorIs(err, ErrNoHealthyChildren, "the probe slots were released, not leaked")
}

func (s *LeaseSuite) TestNestedPickSkipsOpenSubComposite() {
	root, _ := s.mustNew(nil)
	first, _ := s.mustNewNamed("first", OpenChildren(1), "a")
	second, _ := s.mustNewNamed("second", OpenChildren(1), "b")
	s.Require().NoError(root.AttachChild(first))
	s.Require().NoError(root.AttachChild(second))
	first.ForceOpen()
	for range testPicks {
		l, err := root.Pick()
		s.Require().NoError(err)
		s.Equal([]string{testName, "second", "b"}, l.Path())
		root.RecordLease(l, breaker.OutcomeSuccess, 0)
	}
}

func (s *LeaseSuite) TestTooDeep() {
	root, _ := s.mustNew(nil)
	level2, _ := s.mustNewNamed("level2", OpenChildren(1))
	level3, _ := s.mustNewNamed("level3", OpenChildren(1))
	level4, _ := s.mustNewNamed("level4", OpenChildren(1))
	withLeaves, _ := s.mustNewNamed("with-leaves", OpenChildren(1), "x")
	s.Require().NoError(root.AttachChild(level2))
	s.Require().NoError(level2.AttachChild(level3))
	s.Require().NoError(level3.AttachChild(level4))
	tests := []struct {
		expectedErr error
		attach      func() error
		name        string
	}{
		{name: "leaf at depth four", attach: func() error { _, err := level3.AddChild("leaf"); return err }, expectedErr: nil},
		{name: "leaf at depth five", attach: func() error { _, err := level4.AddChild("leaf"); return err }, expectedErr: ErrTooDeep},
		{name: "composite with leaves at depth four", attach: func() error { return level3.AttachChild(withLeaves) }, expectedErr: ErrTooDeep},
		{name: "composite with leaves at depth three", attach: func() error { return level2.AttachChild(withLeaves) }, expectedErr: nil},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.ErrorIs(tt.attach(), tt.expectedErr)
		})
	}
	s.Equal(0, level4.height()-1, "level4 stayed empty")
}

func (s *LeaseSuite) TestExecute() {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		ctx              context.Context
		fnErr            error
		expectedErr      error
		name             string
		expectedFailures uint32
		expectedTotal    uint32
		fnPanics         bool
		forceOpen        bool
		expectedCalled   bool
	}{
		{name: "success", ctx: context.Background(), expectedCalled: true, expectedTotal: 1},
		{
			name:             "error",
			ctx:              context.Background(),
			fnErr:            errBoom,
			expectedErr:      errBoom,
			expectedCalled:   true,
			expectedTotal:    1,
			expectedFailures: 1,
		},
		{name: "panic records failure", ctx: context.Background(), fnPanics: true, expectedCalled: true, expectedTotal: 1, expectedFailures: 1},
		{name: "canceled context", ctx: canceled, expectedErr: context.Canceled},
		{name: "rejected", ctx: context.Background(), forceOpen: true, expectedErr: breaker.ErrOpen},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.fn = &MockFunc{}
			c, leaves := s.mustNew(nil, "a")
			if tt.forceOpen {
				c.ForceOpen()
			}
			if tt.expectedCalled {
				call := s.fn.On("Call", tt.ctx, "a").Return(tt.fnErr).Once()
				if tt.fnPanics {
					call.Run(func(mock.Arguments) { panic("fn boom") })
				}
			}
			if tt.fnPanics {
				s.Panics(func() { _ = c.Execute(tt.ctx, s.fn.Call) })
			} else {
				s.ErrorIs(c.Execute(tt.ctx, s.fn.Call), tt.expectedErr)
			}
			for name, counted := range map[string]interface{ Counts() breaker.Counts }{"parent": c, "leaf": leaves["a"]} {
				s.Equal(tt.expectedTotal, counted.Counts().Total, name)
				s.Equal(tt.expectedFailures, counted.Counts().Failures, name)
			}
			s.fn.AssertExpectations(s.T())
		})
	}
}

func (s *LeaseSuite) TestExecuteNilFunc() {
	c, _ := s.mustNew(nil, "a")
	s.ErrorIs(c.Execute(context.Background(), nil), breaker.ErrNilFunc)
}

func (s *LeaseSuite) TestExecuteClassifierPanicIsFailure() {
	cfg := s.config(testName, nil)
	cfg.Parent.Classifier = func(context.Context, error) breaker.Outcome { panic("classify boom") }
	c, err := New(cfg)
	s.Require().NoError(err)
	_, err = c.AddChild("a")
	s.Require().NoError(err)
	s.fn.On("Call", mock.Anything, "a").Return(nil).Once()
	s.events.ExpectedCalls = nil
	s.events.On("OnHookPanic", "classifier", "classify boom").Once()
	s.events.On("OnCall", mock.Anything).Maybe()
	s.NoError(c.Execute(context.Background(), s.fn.Call))
	s.Equal(uint32(1), c.Counts().Failures)
}

func (s *LeaseSuite) TestExecuteMeasuresElapsed() {
	c, _ := s.mustNew(nil, "a")
	s.fn.On("Call", mock.Anything, "a").Run(func(mock.Arguments) { s.clock.advance(time.Second) }).Return(nil).Once()
	s.NoError(c.Execute(context.Background(), s.fn.Call))
	s.events.AssertCalled(s.T(), "OnCall", mock.MatchedBy(func(ev breaker.CallEvent) bool {
		return ev.Name == testName && ev.Elapsed == time.Second
	}))
}

func (s *LeaseSuite) TestPerHopEpochIndependent() {
	s.childTrip = 1
	c, leaves := s.mustNew(nil, "a")
	l, err := c.Pick()
	s.Require().NoError(err)
	s.trip(leaves["a"])
	c.RecordLease(l, breaker.OutcomeSuccess, 0)
	s.Equal(breaker.Counts{Total: 1, Failures: 1, ConsecutiveFailures: 1}, leaves["a"].Counts(), "the leaf discarded the stale permit")
	s.Equal(breaker.Counts{Total: 1, Successes: 1, ConsecutiveSuccesses: 1}, c.Counts(), "the parent counted its own permit")
}

func (s *LeaseSuite) TestConcurrentPickAndChildChanges() {
	c, _ := s.mustNew(nil, "a", "b")
	var wg sync.WaitGroup
	for range testRaceWorker {
		wg.Go(func() {
			for range testRaceRounds {
				l, err := c.Pick()
				if err == nil {
					c.RecordLease(l, breaker.OutcomeSuccess, 0)
				}
			}
		})
	}
	wg.Go(func() {
		for i := range testRaceRounds {
			id := "x"
			if i%2 == 1 {
				s.True(c.RemoveChild(id))
				continue
			}
			_, err := c.AddChild(id)
			s.NoError(err)
		}
	})
	wg.Wait()
	s.Equal([]string{"a", "b"}, c.Children())
}

func (s *LeaseSuite) TestPickNoHealthyHookPanicSwallowed() {
	c, leaves := s.mustNew(nil, "a")
	s.forceOpen(leaves["a"])
	s.events.ExpectedCalls = nil
	s.events.On("OnStateChange", mock.Anything).Maybe()
	noHealthy := mock.MatchedBy(func(ev breaker.RejectEvent) bool { return errors.Is(ev.Err, ErrNoHealthyChildren) })
	s.events.On("OnReject", noHealthy).Run(func(mock.Arguments) { panic("reject boom") }).Once()
	s.events.On("OnReject", mock.Anything).Maybe()
	s.events.On("OnHookPanic", hookOnReject, "reject boom").Run(func(mock.Arguments) { panic("hook boom") }).Once()
	expected := c.HookPanics()
	var err error
	s.NotPanics(func() { _, err = c.Pick() })
	s.ErrorIs(err, ErrNoHealthyChildren)
	s.Equal(expected+2, c.HookPanics(), "OnReject and OnHookPanic both panicked")
}

func (s *LeaseSuite) TestExecuteClassifierHookPanicSwallowed() {
	cfg := s.config(testName, nil)
	cfg.Parent.Classifier = func(context.Context, error) breaker.Outcome { panic("classify boom") }
	c, err := New(cfg)
	s.Require().NoError(err)
	_, err = c.AddChild("a")
	s.Require().NoError(err)
	s.fn.On("Call", mock.Anything, "a").Return(nil).Once()
	s.events.ExpectedCalls = nil
	s.events.On("OnHookPanic", hookClassifier, "classify boom").Run(func(mock.Arguments) { panic("hook boom") }).Once()
	s.events.On("OnCall", mock.Anything).Maybe()
	s.events.On("OnStateChange", mock.Anything).Maybe()
	s.NotPanics(func() { err = c.Execute(context.Background(), s.fn.Call) })
	s.NoError(err)
	s.Equal(uint32(1), c.Counts().Failures)
	s.Equal(uint64(1), c.HookPanics())
}
