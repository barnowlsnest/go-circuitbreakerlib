package breaker

import (
	"time"

	"github.com/stretchr/testify/mock"
)

// MockEvents is a testify mock of Events.
type MockEvents struct {
	mock.Mock
}

func (m *MockEvents) OnStateChange(ev StateChangeEvent) { m.Called(ev) }
func (m *MockEvents) OnCall(ev CallEvent)               { m.Called(ev) }
func (m *MockEvents) OnReject(ev RejectEvent)           { m.Called(ev) }
func (m *MockEvents) OnHookPanic(name string, recovered any) {
	m.Called(name, recovered)
}

// MockPolicy is a testify mock of Policy.
type MockPolicy struct {
	mock.Mock
}

func (m *MockPolicy) ShouldTrip(c Counts) bool { return m.Called(c).Bool(0) }
func (m *MockPolicy) Name() string             { return m.Called().String(0) }

// MockBackoff is a testify mock of Backoff.
type MockBackoff struct {
	mock.Mock
}

func (m *MockBackoff) Delay(attempt int) time.Duration {
	return m.Called(attempt).Get(0).(time.Duration)
}

// testClock is an injectable clock.
type testClock struct {
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *testClock) Now() time.Time          { return c.now }
func (c *testClock) advance(d time.Duration) { c.now = c.now.Add(d) }

// newNoopMockEvents returns a MockEvents that accepts every call. Tests that
// assert on specific events use their own expectations instead.
func newNoopMockEvents() *MockEvents {
	m := &MockEvents{}
	m.On("OnStateChange", mock.Anything).Maybe()
	m.On("OnCall", mock.Anything).Maybe()
	m.On("OnReject", mock.Anything).Maybe()
	m.On("OnHookPanic", mock.Anything, mock.Anything).Maybe()
	return m
}
