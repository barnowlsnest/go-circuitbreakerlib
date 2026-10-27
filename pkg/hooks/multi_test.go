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
	s.NotPanics(func() { s.multi.OnCall(breaker.CallEvent{Name: testName}) })
}

func (s *MultiEventsSuite) TestEmptyIsNoop() {
	s.NotPanics(func() { NewMultiEvents().OnReject(breaker.RejectEvent{}) })
}

func (s *MultiEventsSuite) TestNilDelegatesAreSkipped() {
	s.second.On("OnReject", mock.Anything).Once()
	multi := NewMultiEvents(nil, s.second)
	s.NotPanics(func() { multi.OnReject(breaker.RejectEvent{}) })
}
