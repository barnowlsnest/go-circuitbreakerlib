package breaker

import (
	"context"
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

// MockFunc is a testify mock of the function Execute runs.
type MockFunc struct {
	mock.Mock
}

func (m *MockFunc) Call(ctx context.Context) error { return m.Called(ctx).Error(0) }

// MockValueFunc is a testify mock of the function Do runs.
type MockValueFunc struct {
	mock.Mock
}

func (m *MockValueFunc) Call(ctx context.Context) (int, error) {
	args := m.Called(ctx)
	return args.Int(0), args.Error(1)
}

// MockClassifier is a testify mock of Config.Classifier.
type MockClassifier struct {
	mock.Mock
}

func (m *MockClassifier) Classify(ctx context.Context, err error) Outcome {
	return m.Called(ctx, err).Get(0).(Outcome)
}

// MockClock is a testify mock of Config.Clock for tests that script every
// read. Suites that step time use testClock instead.
type MockClock struct {
	mock.Mock
}

func (m *MockClock) Now() time.Time { return m.Called().Get(0).(time.Time) }
