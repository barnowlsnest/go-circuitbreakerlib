package adapter

import (
	"net/http"

	"github.com/stretchr/testify/mock"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

// MockRoundTripper is a testify mock of http.RoundTripper.
type MockRoundTripper struct {
	mock.Mock
}

func (m *MockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	args := m.Called(req)
	resp, _ := args.Get(0).(*http.Response)
	return resp, args.Error(1)
}

// MockBody is a testify mock of io.ReadCloser.
type MockBody struct {
	mock.Mock
}

func (m *MockBody) Read(p []byte) (int, error) {
	args := m.Called(p)
	return args.Int(0), args.Error(1)
}

func (m *MockBody) Close() error { return m.Called().Error(0) }

// MockEvents is a testify mock of breaker.Events.
type MockEvents struct {
	mock.Mock
}

func (m *MockEvents) OnStateChange(ev breaker.StateChangeEvent) { m.Called(ev) }
func (m *MockEvents) OnCall(ev breaker.CallEvent)               { m.Called(ev) }
func (m *MockEvents) OnReject(ev breaker.RejectEvent)           { m.Called(ev) }
func (m *MockEvents) OnHookPanic(name string, recovered any) {
	m.Called(name, recovered)
}

// newNoopMockEvents returns a MockEvents that accepts every call.
func newNoopMockEvents() *MockEvents {
	m := &MockEvents{}
	m.On("OnStateChange", mock.Anything).Maybe()
	m.On("OnCall", mock.Anything).Maybe()
	m.On("OnReject", mock.Anything).Maybe()
	m.On("OnHookPanic", mock.Anything, mock.Anything).Maybe()
	return m
}
