package breaker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

const (
	testElapsed = 25 * time.Millisecond
	testValue   = 42
)

var errBoom = errors.New("boom")

type ExecuteSuite struct {
	suite.Suite
	clock      *MockClock
	events     *MockEvents
	policy     *MockPolicy
	classifier *MockClassifier
	fn         *MockFunc
	valueFn    *MockValueFunc
	ctx        context.Context
}

func TestExecuteSuite(t *testing.T) {
	suite.Run(t, new(ExecuteSuite))
}

func (s *ExecuteSuite) SetupTest() {
	s.clock = &MockClock{}
	s.clock.On("Now").Return(newTestClock().now).Maybe()
	s.events = newNoopMockEvents()
	s.policy = &MockPolicy{}
	s.policy.On("Name").Return("mock").Maybe()
	s.policy.On("ShouldTrip", mock.Anything).Return(false).Maybe()
	s.classifier = &MockClassifier{}
	s.fn = &MockFunc{}
	s.valueFn = &MockValueFunc{}
	s.ctx = context.Background()
}

func (s *ExecuteSuite) TearDownTest() {
	for _, m := range []interface{ AssertExpectations(mock.TestingT) bool }{
		s.clock, s.events, s.policy, s.classifier, s.fn, s.valueFn,
	} {
		m.AssertExpectations(s.T())
	}
}

func (s *ExecuteSuite) newBreaker() *Breaker {
	s.T().Helper()
	b, err := New(
		WithName(testName),
		WithPolicies(s.policy),
		WithOpenDuration(testOpenDuration),
		WithEvents(s.events),
		WithClassifier(s.classifier.Classify),
		WithClock(s.clock.Now),
	)
	s.Require().NoError(err)
	return b
}

func (s *ExecuteSuite) TestExecute() {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		ctx               context.Context
		fnErr             error
		expectedErr       error
		name              string
		expectedSuccesses uint32
		expectedFailures  uint32
		fnPanics          bool
		classified        Outcome
		forceOpen         bool
		expectedCalled    bool
		expectedRejected  bool
	}{
		{
			name:              "success recorded",
			ctx:               context.Background(),
			classified:        OutcomeSuccess,
			expectedCalled:    true,
			expectedSuccesses: 1,
		},
		{
			name:             "error passes through and counts",
			ctx:              context.Background(),
			fnErr:            errBoom,
			classified:       OutcomeFailure,
			expectedErr:      errBoom,
			expectedCalled:   true,
			expectedFailures: 1,
		},
		{
			name:           "ignored outcome not counted",
			ctx:            context.Background(),
			fnErr:          errBoom,
			classified:     OutcomeIgnored,
			expectedErr:    errBoom,
			expectedCalled: true,
		},
		{
			name:             "panic records failure and propagates",
			ctx:              context.Background(),
			fnPanics:         true,
			expectedCalled:   true,
			expectedFailures: 1,
		},
		{
			name:        "canceled context not recorded",
			ctx:         canceled,
			expectedErr: context.Canceled,
		},
		{
			name:             "rejected when open",
			ctx:              context.Background(),
			forceOpen:        true,
			expectedErr:      ErrOpen,
			expectedRejected: true,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.SetupTest()
			defer s.TearDownTest()
			b := s.newBreaker()
			if tt.forceOpen {
				b.ForceOpen()
			}
			if tt.expectedCalled {
				call := s.fn.On("Call", tt.ctx).Return(tt.fnErr).Once()
				if tt.fnPanics {
					call.Run(func(mock.Arguments) { panic("fn boom") })
				} else {
					s.classifier.On("Classify", tt.ctx, tt.fnErr).Return(tt.classified).Once()
				}
			}
			if tt.fnPanics {
				s.Panics(func() { _ = b.Execute(tt.ctx, s.fn.Call) })
			} else {
				err := b.Execute(tt.ctx, s.fn.Call)
				s.ErrorIs(err, tt.expectedErr)
				var re *RejectedError
				s.Equal(tt.expectedRejected, errors.As(err, &re))
			}
			counts := b.Counts()
			s.Equal(tt.expectedSuccesses, counts.Successes)
			s.Equal(tt.expectedFailures, counts.Failures)
		})
	}
}

func (s *ExecuteSuite) TestNilFunc() {
	b := s.newBreaker()
	s.ErrorIs(b.Execute(s.ctx, nil), ErrNilFunc)
	_, err := Do[int](s.ctx, b, nil)
	s.ErrorIs(err, ErrNilFunc)
}

func (s *ExecuteSuite) TestElapsedMeasured() {
	s.clock.ExpectedCalls = nil
	start := newTestClock().now
	// New, Allow, and Execute's start each read the clock once; every read
	// after that is Execute's end.
	s.clock.On("Now").Return(start).Times(3)
	s.clock.On("Now").Return(start.Add(testElapsed))
	s.events = &MockEvents{}
	s.events.On("OnCall", mock.MatchedBy(func(ev CallEvent) bool {
		return ev.Elapsed == testElapsed && ev.Outcome == OutcomeSuccess
	})).Once()
	s.fn.On("Call", s.ctx).Return(nil).Once()
	s.classifier.On("Classify", s.ctx, nil).Return(OutcomeSuccess).Once()
	b := s.newBreaker()
	s.NoError(b.Execute(s.ctx, s.fn.Call))
}

func (s *ExecuteSuite) TestClassifierPanicIsFailure() {
	s.fn.On("Call", s.ctx).Return(nil).Once()
	s.classifier.On("Classify", s.ctx, nil).Run(func(mock.Arguments) { panic("classify boom") }).Once()
	s.events = &MockEvents{}
	s.events.On("OnHookPanic", "classifier", "classify boom").Once()
	s.events.On("OnCall", mock.Anything).Maybe()
	b := s.newBreaker()
	s.NoError(b.Execute(s.ctx, s.fn.Call))
	s.Equal(uint32(1), b.Counts().Failures)
}

func (s *ExecuteSuite) TestPolicyTripsAfterExecute() {
	s.policy.ExpectedCalls = nil
	s.policy.On("Name").Return("mock").Maybe()
	s.policy.On("ShouldTrip", Counts{Total: 1, Failures: 1, ConsecutiveFailures: 1}).Return(true).Once()
	s.fn.On("Call", s.ctx).Return(errBoom).Once()
	s.classifier.On("Classify", s.ctx, errBoom).Return(OutcomeFailure).Once()
	b := s.newBreaker()
	s.ErrorIs(b.Execute(s.ctx, s.fn.Call), errBoom)
	s.Equal(StateOpen, b.State())
}

func (s *ExecuteSuite) TestDo() {
	tests := []struct {
		fnErr         error
		expectedErr   error
		name          string
		value         int
		expectedValue int
		classified    Outcome
	}{
		{name: "returns value", value: testValue, classified: OutcomeSuccess, expectedValue: testValue},
		{name: "returns error and zero value", fnErr: errBoom, classified: OutcomeFailure, expectedErr: errBoom},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.SetupTest()
			defer s.TearDownTest()
			s.valueFn.On("Call", s.ctx).Return(tt.value, tt.fnErr).Once()
			s.classifier.On("Classify", s.ctx, tt.fnErr).Return(tt.classified).Once()
			b := s.newBreaker()
			v, err := Do(s.ctx, b, s.valueFn.Call)
			s.ErrorIs(err, tt.expectedErr)
			s.Equal(tt.expectedValue, v)
		})
	}
}
