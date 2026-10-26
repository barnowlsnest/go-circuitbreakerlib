package breaker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

const testElapsed = 25 * time.Millisecond

var errBoom = errors.New("boom")

type ExecuteSuite struct {
	suite.Suite
	clock  *testClock
	events *MockEvents
}

func TestExecuteSuite(t *testing.T) {
	suite.Run(t, new(ExecuteSuite))
}

func (s *ExecuteSuite) SetupTest() {
	s.clock = newTestClock()
	s.events = newNoopMockEvents()
}

func (s *ExecuteSuite) TearDownTest() {
	s.events.AssertExpectations(s.T())
}

func (s *ExecuteSuite) newBreaker() *Breaker {
	s.T().Helper()
	b, err := New(Config{
		Name:         testName,
		Policies:     []Policy{ConsecutiveFailures{Count: testTripCount}},
		OpenDuration: testOpenDuration,
		Events:       s.events,
		Clock:        s.clock.Now,
	})
	s.Require().NoError(err)
	return b
}

func (s *ExecuteSuite) TestCancelledContextNotRecorded() {
	b := s.newBreaker()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	err := b.Execute(ctx, func(context.Context) error { called = true; return nil })
	s.ErrorIs(err, context.Canceled)
	s.False(called)
	s.Equal(uint32(0), b.Counts().Total)
}

func (s *ExecuteSuite) TestNilFunc() {
	b := s.newBreaker()
	s.ErrorIs(b.Execute(context.Background(), nil), ErrNilFunc)
	_, err := Do[int](context.Background(), b, nil)
	s.ErrorIs(err, ErrNilFunc)
}

func (s *ExecuteSuite) TestRejectedWhenOpen() {
	b := s.newBreaker()
	b.ForceOpen()
	called := false
	err := b.Execute(context.Background(), func(context.Context) error { called = true; return nil })
	s.ErrorIs(err, ErrOpen)
	var re *RejectedError
	s.ErrorAs(err, &re)
	s.False(called)
}

func (s *ExecuteSuite) TestErrorPassesThroughAndCounts() {
	b := s.newBreaker()
	err := b.Execute(context.Background(), func(context.Context) error { return errBoom })
	s.ErrorIs(err, errBoom)
	s.Equal(uint32(1), b.Counts().Failures)
}

func (s *ExecuteSuite) TestPanicRecordsFailureAndPropagates() {
	b := s.newBreaker()
	s.Panics(func() {
		_ = b.Execute(context.Background(), func(context.Context) error { panic("fn boom") })
	})
	s.Equal(uint32(1), b.Counts().Failures)
}

func (s *ExecuteSuite) TestElapsedMeasured() {
	b := s.newBreaker()
	err := b.Execute(context.Background(), func(context.Context) error {
		s.clock.advance(testElapsed)
		return nil
	})
	s.NoError(err)
	s.events.AssertCalled(s.T(), "OnCall", mock.MatchedBy(func(ev CallEvent) bool {
		return ev.Elapsed == testElapsed && ev.Outcome == OutcomeSuccess
	}))
}

func (s *ExecuteSuite) TestClassifierPanicIsFailure() {
	b, err := New(Config{
		Name:       testName,
		Classifier: func(context.Context, error) Outcome { panic("classify boom") },
		Events:     s.events,
		Clock:      s.clock.Now,
	})
	s.Require().NoError(err)
	s.NoError(b.Execute(context.Background(), func(context.Context) error { return nil }))
	s.Equal(uint32(1), b.Counts().Failures)
	s.events.AssertCalled(s.T(), "OnHookPanic", "classifier", "classify boom")
}

func (s *ExecuteSuite) TestDoReturnsValue() {
	b := s.newBreaker()
	const expected = 42
	v, err := Do(context.Background(), b, func(context.Context) (int, error) { return expected, nil })
	s.NoError(err)
	s.Equal(expected, v)
	s.Equal(uint32(1), b.Counts().Successes)
}
