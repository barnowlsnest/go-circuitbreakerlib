package registry

import (
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

// MockOnEvict is a testify mock of the WithOnEvict callback.
type MockOnEvict struct {
	mock.Mock
}

func (m *MockOnEvict) Evict(key string, b *breaker.Breaker) { m.Called(key, b) }

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

// testClock is an injectable clock.
type testClock struct {
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *testClock) Now() time.Time          { return c.now }
func (c *testClock) advance(d time.Duration) { c.now = c.now.Add(d) }
