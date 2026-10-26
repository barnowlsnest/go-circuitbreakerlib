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
