package hooks

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

type MultiEventsSuite struct {
	suite.Suite
	first  *MockEvents
	second *MockEvents
	multi  breaker.Events
}

func TestMultiEventsSuite(t *testing.T) {
	suite.Run(t, new(MultiEventsSuite))
}

func (s *MultiEventsSuite) SetupTest() {
	s.first = &MockEvents{}
	s.second = &MockEvents{}
	s.multi = NewMultiEvents(s.first, s.second)
}

func (s *MultiEventsSuite) TearDownTest() {
	s.first.AssertExpectations(s.T())
	s.second.AssertExpectations(s.T())
}

func (s *MultiEventsSuite) TestFanOutInOrder() {
	var order []string
	s.first.On("OnReject", mock.Anything).Run(func(mock.Arguments) { order = append(order, "first") }).Once()
	s.second.On("OnReject", mock.Anything).Run(func(mock.Arguments) { order = append(order, "second") }).Once()
	s.multi.OnReject(breaker.RejectEvent{Name: testName})
	s.Equal([]string{"first", "second"}, order)
}

func (s *MultiEventsSuite) TestEveryHookReachesEveryDelegate() {
	for _, m := range []*MockEvents{s.first, s.second} {
		m.On("OnStateChange", mock.Anything).Once()
		m.On("OnCall", mock.Anything).Once()
		m.On("OnReject", mock.Anything).Once()
		m.On("OnHookPanic", "policy", errBoom).Once()
	}
	s.multi.OnStateChange(breaker.StateChangeEvent{Name: testName})
	s.multi.OnCall(breaker.CallEvent{Name: testName})
	s.multi.OnReject(breaker.RejectEvent{Name: testName})
	s.multi.OnHookPanic("policy", errBoom)
}

func (s *MultiEventsSuite) TestFanOutAndRecover() {
	s.first.On("OnCall", mock.Anything).Run(func(mock.Arguments) { panic(errBoom) }).Once()
	s.second.On("OnCall", mock.Anything).Once()
	s.second.On("OnHookPanic", hookOnCall, errBoom).Once()
	s.NotPanics(func() { s.multi.OnCall(breaker.CallEvent{Name: testName}) })
}

func (s *MultiEventsSuite) TestPanicReportedToOtherDelegates() {
	tests := []struct {
		fire         func(breaker.Events)
		name         string
		method       string
		expectedHook string
		args         []any
	}{
		{
			name:         "OnStateChange",
			method:       "OnStateChange",
			args:         []any{mock.Anything},
			fire:         func(ev breaker.Events) { ev.OnStateChange(breaker.StateChangeEvent{Name: testName}) },
			expectedHook: hookOnStateChange,
		},
		{
			name:         "OnCall",
			method:       "OnCall",
			args:         []any{mock.Anything},
			fire:         func(ev breaker.Events) { ev.OnCall(breaker.CallEvent{Name: testName}) },
			expectedHook: hookOnCall,
		},
		{
			name:         "OnReject",
			method:       "OnReject",
			args:         []any{mock.Anything},
			fire:         func(ev breaker.Events) { ev.OnReject(breaker.RejectEvent{Name: testName}) },
			expectedHook: hookOnReject,
		},
		{
			name:         "OnHookPanic",
			method:       "OnHookPanic",
			args:         []any{"policy", "policy boom"},
			fire:         func(ev breaker.Events) { ev.OnHookPanic("policy", "policy boom") },
			expectedHook: hookOnHookPanic,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			first, second, third := &MockEvents{}, &MockEvents{}, &MockEvents{}
			first.On(tt.method, tt.args...).Panic("first boom").Once()
			second.On(tt.method, tt.args...).Once()
			second.On("OnHookPanic", tt.expectedHook, "first boom").Panic("second boom").Once()
			third.On(tt.method, tt.args...).Once()
			third.On("OnHookPanic", tt.expectedHook, "first boom").Once()
			s.NotPanics(func() { tt.fire(NewMultiEvents(first, second, third)) })
			first.AssertExpectations(s.T())
			second.AssertExpectations(s.T())
			third.AssertExpectations(s.T())
		})
	}
}

func (s *MultiEventsSuite) TestEmptyIsNoop() {
	s.NotPanics(func() { NewMultiEvents().OnReject(breaker.RejectEvent{}) })
}

func (s *MultiEventsSuite) TestNilDelegatesAreSkipped() {
	s.second.On("OnReject", mock.Anything).Once()
	multi := NewMultiEvents(nil, s.second)
	s.NotPanics(func() { multi.OnReject(breaker.RejectEvent{}) })
}
