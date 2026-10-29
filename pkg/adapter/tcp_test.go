package adapter

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

const (
	testNetwork    = "tcp"
	testAddress    = "10.0.0.1:443"
	testOpenDur    = 10 * time.Second
	testManyWrites = 1000
)

var errReset = syscall.ECONNRESET

type TCPSuite struct {
	suite.Suite
	clock   *testClock
	events  *MockEvents
	next    *MockContextDialer
	conn    *MockConn
	breaker *breaker.Breaker
}

func TestTCPSuite(t *testing.T) {
	suite.Run(t, new(TCPSuite))
}

func (s *TCPSuite) SetupTest() {
	s.clock = newTestClock()
	s.events = newNoopMockEvents()
	s.next = &MockContextDialer{}
	s.conn = &MockConn{}
	s.breaker = s.newBreaker()
}

func (s *TCPSuite) TearDownTest() {
	s.events.AssertExpectations(s.T())
	s.next.AssertExpectations(s.T())
	s.conn.AssertExpectations(s.T())
}

func (s *TCPSuite) newBreaker() *breaker.Breaker {
	s.T().Helper()
	b, err := breaker.New(&breaker.Config{
		Name:         testName,
		Policies:     []breaker.Policy{breaker.ConsecutiveFailures{Count: 1}},
		OpenDuration: testOpenDur,
		Events:       s.events,
		Clock:        s.clock.Now,
	})
	s.Require().NoError(err)
	return b
}

// tracked dials through the mock dialer with I/O tracking on and returns
// the wrapped mock conn. The dial itself is recorded, so the window is
// cleared afterwards and the counts the tests read come from I/O alone.
func (s *TCPSuite) tracked() net.Conn {
	s.T().Helper()
	d, err := NewTCPDialer(s.next, WithBreaker(s.breaker), WithTCPTrackIO(true))
	s.Require().NoError(err)
	s.next.On("DialContext", mock.Anything, testNetwork, testAddress).Return(s.conn, nil).Once()
	conn, err := d.DialContext(context.Background(), testNetwork, testAddress)
	s.Require().NoError(err)
	s.NotSame(s.conn, conn)
	s.breaker.Reset()
	return conn
}

// listener opens a real local port. closed=true releases it first so the
// address refuses connections.
func (s *TCPSuite) listener(closed bool) string {
	s.T().Helper()
	l, err := net.Listen(testNetwork, "127.0.0.1:0")
	s.Require().NoError(err)
	if closed {
		s.Require().NoError(l.Close())
	} else {
		s.T().Cleanup(func() { _ = l.Close() })
	}
	return l.Addr().String()
}

func (s *TCPSuite) TestOptions() {
	tests := []struct {
		expectedErr error
		name        string
		opts        []Option
	}{
		{name: "none", opts: nil, expectedErr: ErrNoBreaker},
		{name: "grpc option", opts: []Option{WithBreaker(s.breaker), WithGRPCKey(DefaultGRPCKey)}, expectedErr: ErrInvalidOption},
		{name: "nil key", opts: []Option{WithBreaker(s.breaker), WithTCPKey(nil)}, expectedErr: ErrInvalidOption},
		{name: "nil classifier", opts: []Option{WithBreaker(s.breaker), WithTCPClassifier(nil)}, expectedErr: ErrInvalidOption},
		{name: "valid", opts: []Option{WithBreaker(s.breaker), WithTCPTrackIO(true)}, expectedErr: nil},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			_, err := NewTCPDialer(nil, tt.opts...)
			s.ErrorIs(err, tt.expectedErr)
		})
	}
}

func (s *TCPSuite) TestDefaultClassifier() {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		ctx      context.Context
		err      error
		name     string
		expected breaker.Outcome
	}{
		{name: "nil", ctx: context.Background(), err: nil, expected: breaker.OutcomeSuccess},
		{name: "refused", ctx: context.Background(), err: syscall.ECONNREFUSED, expected: breaker.OutcomeFailure},
		{name: "deadline", ctx: context.Background(), err: context.DeadlineExceeded, expected: breaker.OutcomeFailure},
		{name: "caller canceled", ctx: canceled, err: context.Canceled, expected: breaker.OutcomeIgnored},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.Equal(tt.expected, DefaultTCPClassifier(tt.ctx, tt.err))
		})
	}
}

func (s *TCPSuite) TestDialRecorded() {
	tests := []struct {
		name             string
		closed           bool
		expectedErr      bool
		expectedFailures uint32
	}{
		{name: "refused is failure", closed: true, expectedErr: true, expectedFailures: 1},
		{name: "connected is success", closed: false, expectedErr: false, expectedFailures: 0},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.breaker = s.newBreaker()
			d, err := NewTCPDialer(nil, WithBreaker(s.breaker))
			s.Require().NoError(err)
			conn, err := d.DialContext(context.Background(), testNetwork, s.listener(tt.closed))
			if tt.expectedErr {
				s.Error(err)
				s.Nil(conn)
			} else {
				s.Require().NoError(err)
				s.NoError(conn.Close())
			}
			s.Equal(uint32(1), s.breaker.Counts().Total)
			s.Equal(tt.expectedFailures, s.breaker.Counts().Failures)
		})
	}
}

func (s *TCPSuite) TestDialRejectedWhenOpen() {
	d, err := NewTCPDialer(s.next, WithBreaker(s.breaker))
	s.Require().NoError(err)
	s.breaker.ForceOpen()
	conn, err := d.DialContext(context.Background(), testNetwork, testAddress)
	s.Nil(conn)
	s.ErrorIs(err, breaker.ErrOpen)
	s.next.AssertNotCalled(s.T(), "DialContext", mock.Anything, mock.Anything, mock.Anything)
}

func (s *TCPSuite) TestRegistryKeyedByAddress() {
	reg := testRegistry(s.T())
	d, err := NewTCPDialer(s.next, WithRegistry(reg))
	s.Require().NoError(err)
	s.next.On("DialContext", mock.Anything, testNetwork, testAddress).Return(nil, errReset).Once()
	_, err = d.DialContext(context.Background(), testNetwork, testAddress)
	s.ErrorIs(err, errReset)
	b, ok := reg.Lookup(testAddress)
	s.Require().True(ok)
	s.Equal(uint32(1), b.Counts().Failures)
}

func (s *TCPSuite) TestCustomKeyAndClassifier() {
	reg := testRegistry(s.T())
	d, err := NewTCPDialer(s.next, WithRegistry(reg),
		WithTCPKey(func(network, address string) string { return network + "/" + strings.Split(address, ":")[0] }),
		WithTCPClassifier(func(context.Context, error) breaker.Outcome { return breaker.OutcomeIgnored }),
	)
	s.Require().NoError(err)
	s.next.On("DialContext", mock.Anything, testNetwork, testAddress).Return(nil, errReset).Once()
	_, err = d.DialContext(context.Background(), testNetwork, testAddress)
	s.ErrorIs(err, errReset)
	b, ok := reg.Lookup("tcp/10.0.0.1")
	s.Require().True(ok)
	s.Equal(uint32(0), b.Counts().Total)
}

func (s *TCPSuite) TestClassifierPanicIsFailure() {
	d, err := NewTCPDialer(s.next, WithBreaker(s.breaker), WithTCPClassifier(
		func(context.Context, error) breaker.Outcome { panic("classify boom") },
	))
	s.Require().NoError(err)
	s.next.On("DialContext", mock.Anything, testNetwork, testAddress).Return(s.conn, nil).Once()
	conn, err := d.DialContext(context.Background(), testNetwork, testAddress)
	s.Require().NoError(err)
	s.Same(s.conn, conn)
	s.Equal(uint32(1), s.breaker.Counts().Failures)
	s.events.AssertCalled(s.T(), "OnHookPanic", hookClassifier, "classify boom")
}

func (s *TCPSuite) TestRegistryErrorFailsDial() {
	reg := testFailingRegistry(s.T())
	d, err := NewTCPDialer(s.next, WithRegistry(reg))
	s.Require().NoError(err)
	_, err = d.DialContext(context.Background(), testNetwork, testAddress)
	s.ErrorIs(err, ErrRegistry)
}

func (s *TCPSuite) TestTrackIOOffByDefault() {
	d, err := NewTCPDialer(nil, WithBreaker(s.breaker))
	s.Require().NoError(err)
	conn, err := d.DialContext(context.Background(), testNetwork, s.listener(false))
	s.Require().NoError(err)
	defer conn.Close()
	_, ok := conn.(*net.TCPConn)
	s.True(ok, "the raw conn is returned")
}

func (s *TCPSuite) TestUnwrapAndOptionalInterfaces() {
	d, err := NewTCPDialer(nil, WithBreaker(s.breaker), WithTCPTrackIO(true))
	s.Require().NoError(err)
	conn, err := d.DialContext(context.Background(), testNetwork, s.listener(false))
	s.Require().NoError(err)
	defer conn.Close()
	_, isTCP := conn.(*net.TCPConn)
	s.False(isTCP, "the conn is wrapped")
	unwrapper, ok := conn.(interface{ Unwrap() net.Conn })
	s.Require().True(ok)
	_, isTCP = unwrapper.Unwrap().(*net.TCPConn)
	s.True(isTCP)
	closer, ok := conn.(interface{ CloseWrite() error })
	s.Require().True(ok)
	s.NoError(closer.CloseWrite())
	sys, ok := conn.(syscall.Conn)
	s.Require().True(ok)
	raw, err := sys.SyscallConn()
	s.NoError(err)
	s.NotNil(raw)
}

func (s *TCPSuite) TestOptionalInterfacesOnPlainConn() {
	conn := s.tracked()
	closer, ok := conn.(interface{ CloseWrite() error })
	s.Require().True(ok)
	s.ErrorIs(closer.CloseWrite(), errors.ErrUnsupported)
	reader, ok := conn.(interface{ CloseRead() error })
	s.Require().True(ok)
	s.ErrorIs(reader.CloseRead(), errors.ErrUnsupported)
	sys, ok := conn.(syscall.Conn)
	s.Require().True(ok)
	_, err := sys.SyscallConn()
	s.ErrorIs(err, errors.ErrUnsupported)
}

func (s *TCPSuite) TestReadFromAndWriteToGoThroughTracking() {
	conn := s.tracked()
	s.breaker.ForceOpen()
	rf, ok := conn.(io.ReaderFrom)
	s.Require().True(ok)
	n, err := rf.ReadFrom(strings.NewReader("payload"))
	s.Equal(int64(0), n)
	s.ErrorIs(err, breaker.ErrOpen)
	wt, ok := conn.(io.WriterTo)
	s.Require().True(ok)
	n, err = wt.WriteTo(io.Discard)
	s.Equal(int64(0), n)
	s.ErrorIs(err, breaker.ErrOpen)
	s.conn.AssertNotCalled(s.T(), "Write", mock.Anything)
	s.conn.AssertNotCalled(s.T(), "Read", mock.Anything)
}

func (s *TCPSuite) TestTrackIOClassification() {
	timeout := &net.OpError{Op: "read", Err: os.ErrDeadlineExceeded}
	tests := []struct {
		err              error
		name             string
		expectedTotal    uint32
		expectedFailures uint32
	}{
		{name: "reset is failure", err: errReset, expectedTotal: 1, expectedFailures: 1},
		{name: "eof not recorded", err: io.EOF, expectedTotal: 0},
		{name: "closed not recorded", err: net.ErrClosed, expectedTotal: 0},
		{name: "deadline ignored", err: os.ErrDeadlineExceeded, expectedTotal: 0},
		{name: "wrapped timeout ignored", err: timeout, expectedTotal: 0},
		{name: "success ignored when closed", err: nil, expectedTotal: 0},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.breaker = s.newBreaker()
			s.next = &MockContextDialer{}
			s.conn = &MockConn{}
			conn := s.tracked()
			buf := make([]byte, 4)
			s.conn.On("Read", buf).Return(0, tt.err).Once()
			_, err := conn.Read(buf)
			s.ErrorIs(err, tt.err)
			s.Equal(tt.expectedTotal, s.breaker.Counts().Total)
			s.Equal(tt.expectedFailures, s.breaker.Counts().Failures)
			s.next.AssertExpectations(s.T())
			s.conn.AssertExpectations(s.T())
		})
	}
}

func (s *TCPSuite) TestTrackIOFailsFastWhenOpen() {
	conn := s.tracked()
	s.breaker.ForceOpen()
	n, err := conn.Write([]byte("x"))
	s.Equal(0, n)
	s.ErrorIs(err, breaker.ErrOpen)
	var re *breaker.RejectedError
	s.ErrorAs(err, &re)
	var netErr net.Error
	s.Require().ErrorAs(err, &netErr)
	s.False(netErr.Timeout())
	s.conn.AssertNotCalled(s.T(), "Write", mock.Anything)
}

func (s *TCPSuite) TestTrackIOProbeSuccessCloses() {
	conn := s.tracked()
	payload := []byte("x")
	s.conn.On("Write", payload).Return(len(payload), errReset).Once()
	_, err := conn.Write(payload)
	s.ErrorIs(err, errReset)
	s.Require().Equal(breaker.StateOpen, s.breaker.State())

	s.clock.advance(testOpenDur)
	s.conn.On("Write", payload).Return(len(payload), nil).Once()
	n, err := conn.Write(payload)
	s.NoError(err)
	s.Equal(len(payload), n)
	s.Equal(breaker.StateClosed, s.breaker.State())
}

func (s *TCPSuite) TestTrackIONonProbeSuccessIgnored() {
	conn := s.tracked()
	payload := []byte("x")
	s.conn.On("Write", payload).Return(len(payload), nil).Times(testManyWrites)
	for range testManyWrites {
		_, err := conn.Write(payload)
		s.Require().NoError(err)
	}
	s.Equal(uint32(0), s.breaker.Counts().Total)
}

func (s *TCPSuite) TestCloseForwarded() {
	conn := s.tracked()
	s.conn.On("Close").Return(nil).Once()
	s.NoError(conn.Close())
}

func (s *TCPSuite) TestTrackedReadDoesNotHoldProbeSlot() {
	conn := s.tracked()
	payload := []byte("x")
	s.conn.On("Write", payload).Return(len(payload), errReset).Once()
	_, err := conn.Write(payload)
	s.Require().ErrorIs(err, errReset)
	s.clock.advance(testOpenDur)
	s.Require().Equal(breaker.StateHalfOpen, s.breaker.State())

	buf := make([]byte, 1)
	s.conn.On("Read", buf).Run(func(mock.Arguments) {
		p, err := s.breaker.Allow()
		s.NoError(err, "a blocked Read must not occupy the probe slot")
		s.True(p.IsProbe())
		s.breaker.Record(p, breaker.OutcomeIgnored, 0)
	}).Return(1, nil).Once()
	n, err := conn.Read(buf)
	s.NoError(err)
	s.Equal(1, n)
	s.Equal(breaker.StateHalfOpen, s.breaker.State(), "a Read never counts as a probe success")
}
