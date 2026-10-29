package adapter

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/interop/grpc_testing"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/registry"
)

const (
	testName          = "dep"
	testClassifyPanic = "classify boom"
)

var errTransport = errors.New("transport down")

// testBreaker builds a breaker named testName that trips on the first
// failure; cfg supplies everything else.
func testBreaker(t *testing.T, cfg *breaker.Config) *breaker.Breaker {
	t.Helper()
	cfg.Name = testName
	cfg.Policies = []breaker.Policy{breaker.ConsecutiveFailures{Count: 1}}
	b, err := breaker.New(cfg)
	require.NoError(t, err)
	return b
}

func testCanceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func testRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	r, err := registry.New(func(string) breaker.Config { return breaker.Config{} })
	require.NoError(t, err)
	return r
}

func testFailingRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	r, err := registry.New(func(string) breaker.Config { return breaker.Config{OpenDuration: -1} })
	require.NoError(t, err)
	return r
}

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

// MockTestService is a testify mock of the grpc_testing TestService.
type MockTestService struct {
	grpc_testing.UnimplementedTestServiceServer
	mock.Mock
}

func (m *MockTestService) UnaryCall(_ context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
	args := m.Called(req)
	resp, _ := args.Get(0).(*grpc_testing.SimpleResponse)
	return resp, args.Error(1)
}

func (m *MockTestService) StreamingOutputCall(
	req *grpc_testing.StreamingOutputCallRequest,
	stream grpc.ServerStreamingServer[grpc_testing.StreamingOutputCallResponse],
) error {
	return m.Called(req, stream).Error(0)
}

// MockContextDialer is a testify mock of ContextDialer.
type MockContextDialer struct {
	mock.Mock
}

func (m *MockContextDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	args := m.Called(ctx, network, address)
	conn, _ := args.Get(0).(net.Conn)
	return conn, args.Error(1)
}

// MockConn is a testify mock of net.Conn.
type MockConn struct {
	mock.Mock
}

func (m *MockConn) Read(p []byte) (int, error) {
	args := m.Called(p)
	return args.Int(0), args.Error(1)
}

func (m *MockConn) Write(p []byte) (int, error) {
	args := m.Called(p)
	return args.Int(0), args.Error(1)
}

func (m *MockConn) Close() error                       { return m.Called().Error(0) }
func (m *MockConn) LocalAddr() net.Addr                { addr, _ := m.Called().Get(0).(net.Addr); return addr }
func (m *MockConn) RemoteAddr() net.Addr               { addr, _ := m.Called().Get(0).(net.Addr); return addr }
func (m *MockConn) SetDeadline(t time.Time) error      { return m.Called(t).Error(0) }
func (m *MockConn) SetReadDeadline(t time.Time) error  { return m.Called(t).Error(0) }
func (m *MockConn) SetWriteDeadline(t time.Time) error { return m.Called(t).Error(0) }

// testClock is an injectable clock.
type testClock struct {
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *testClock) Now() time.Time          { return c.now }
func (c *testClock) advance(d time.Duration) { c.now = c.now.Add(d) }
