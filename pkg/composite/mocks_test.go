package composite

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

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

// MockChild is a testify mock of Child.
type MockChild struct {
	mock.Mock
}

func (m *MockChild) Name() string         { return m.Called().String(0) }
func (m *MockChild) State() breaker.State { return m.Called().Get(0).(breaker.State) }
func (m *MockChild) Allow() (breaker.Permit, error) {
	args := m.Called()
	return args.Get(0).(breaker.Permit), args.Error(1)
}
func (m *MockChild) Record(p breaker.Permit, o breaker.Outcome, d time.Duration) { m.Called(p, o, d) }

// MockFunc is a testify mock of the function Execute runs.
type MockFunc struct {
	mock.Mock
}

func (m *MockFunc) Call(ctx context.Context, leaf string) error { return m.Called(ctx, leaf).Error(0) }

// testClock is an injectable clock shared by the parent and every child.
type testClock struct {
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *testClock) Now() time.Time          { return c.now }
func (c *testClock) advance(d time.Duration) { c.now = c.now.Add(d) }
