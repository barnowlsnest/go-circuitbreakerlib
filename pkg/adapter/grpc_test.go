package adapter

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

const (
	testBufSize = 1 << 20
	testTarget  = "passthrough:///bufnet"
)

type GRPCSuite struct {
	suite.Suite
	events  *MockEvents
	service *MockTestService
	breaker *breaker.Breaker
	server  *grpc.Server
	lis     *bufconn.Listener
}

func TestGRPCSuite(t *testing.T) {
	suite.Run(t, new(GRPCSuite))
}

func (s *GRPCSuite) SetupTest() {
	s.events = newNoopMockEvents()
	s.service = &MockTestService{}
	s.breaker = s.newBreaker()
	lis := bufconn.Listen(testBufSize)
	server := grpc.NewServer()
	grpc_testing.RegisterTestServiceServer(server, s.service)
	s.lis, s.server = lis, server
	go func() { _ = server.Serve(lis) }()
}

func (s *GRPCSuite) TearDownTest() {
	s.server.Stop()
	s.events.AssertExpectations(s.T())
	s.service.AssertExpectations(s.T())
}

func (s *GRPCSuite) newBreaker() *breaker.Breaker {
	s.T().Helper()
	return testBreaker(s.T(), breaker.WithEvents(s.events))
}

// client dials the in-memory server with the given interceptors.
func (s *GRPCSuite) client(unary grpc.UnaryClientInterceptor, stream grpc.StreamClientInterceptor) grpc_testing.TestServiceClient {
	s.T().Helper()
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return s.lis.DialContext(ctx) }),
	}
	if unary != nil {
		opts = append(opts, grpc.WithUnaryInterceptor(unary))
	}
	if stream != nil {
		opts = append(opts, grpc.WithStreamInterceptor(stream))
	}
	cc, err := grpc.NewClient(testTarget, opts...)
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(cc.Close()) })
	return grpc_testing.NewTestServiceClient(cc)
}

func (s *GRPCSuite) TestOptions() {
	tests := []struct {
		expectedErr error
		name        string
		opts        []Option
	}{
		{name: "none", opts: nil, expectedErr: ErrNoBreaker},
		{name: "http option", opts: []Option{WithBreaker(s.breaker), WithHTTPKey(DefaultHTTPKey)}, expectedErr: ErrInvalidOption},
		{name: "nil key", opts: []Option{WithBreaker(s.breaker), WithGRPCKey(nil)}, expectedErr: ErrInvalidOption},
		{name: "nil classifier", opts: []Option{WithBreaker(s.breaker), WithGRPCClassifier(nil)}, expectedErr: ErrInvalidOption},
		{name: "ok reject code", opts: []Option{WithBreaker(s.breaker), WithGRPCRejectCode(codes.OK)}, expectedErr: ErrInvalidOption},
		{name: "valid", opts: []Option{WithBreaker(s.breaker), WithGRPCRejectCode(codes.FailedPrecondition)}, expectedErr: nil},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			_, err := NewGRPCUnaryInterceptor(tt.opts...)
			s.ErrorIs(err, tt.expectedErr)
			_, err = NewGRPCStreamInterceptor(tt.opts...)
			s.ErrorIs(err, tt.expectedErr)
		})
	}
}

func (s *GRPCSuite) TestDefaultClassifier() {
	canceled := testCanceledContext()
	failures := map[codes.Code]bool{
		codes.Unavailable: true, codes.DeadlineExceeded: true, codes.ResourceExhausted: true, codes.Internal: true,
		codes.Unknown: true, codes.DataLoss: true, codes.Aborted: true, codes.Canceled: true,
	}
	type testCase struct {
		ctx      context.Context
		err      error
		name     string
		expected breaker.Outcome
	}
	tests := []testCase{
		{name: "nil error", ctx: context.Background(), err: nil, expected: breaker.OutcomeSuccess},
		{name: "plain error", ctx: context.Background(), err: errTransport, expected: breaker.OutcomeFailure},
		{name: "caller canceled", ctx: canceled, err: status.Error(codes.Canceled, "x"), expected: breaker.OutcomeIgnored},
	}
	for code := codes.OK; code <= codes.Unauthenticated; code++ {
		expected := breaker.OutcomeSuccess
		if failures[code] {
			expected = breaker.OutcomeFailure
		}
		tests = append(tests, testCase{name: code.String(), ctx: context.Background(), err: status.Error(code, "x"), expected: expected})
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.Equal(tt.expected, DefaultGRPCClassifier(tt.ctx, tt.err))
		})
	}
}

func (s *GRPCSuite) TestUnaryRecordsOutcome() {
	tests := []struct {
		serverErr        error
		name             string
		expectedCode     codes.Code
		expectedFailures uint32
	}{
		{name: "ok", serverErr: nil, expectedCode: codes.OK, expectedFailures: 0},
		{name: "not found is success", serverErr: status.Error(codes.NotFound, "x"), expectedCode: codes.NotFound, expectedFailures: 0},
		{name: "internal is failure", serverErr: status.Error(codes.Internal, "x"), expectedCode: codes.Internal, expectedFailures: 1},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.breaker = s.newBreaker()
			unary, err := NewGRPCUnaryInterceptor(WithBreaker(s.breaker))
			s.Require().NoError(err)
			s.service.On("UnaryCall", mock.Anything).Return(&grpc_testing.SimpleResponse{}, tt.serverErr).Once()
			_, err = s.client(unary, nil).UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
			s.Equal(tt.expectedCode, status.Code(err))
			s.Equal(uint32(1), s.breaker.Counts().Total)
			s.Equal(tt.expectedFailures, s.breaker.Counts().Failures)
		})
	}
}

func (s *GRPCSuite) TestUnaryRejectedStatus() {
	tests := []struct {
		name         string
		opts         []Option
		expectedCode codes.Code
	}{
		{name: "default code", opts: nil, expectedCode: codes.Unavailable},
		{name: "custom code", opts: []Option{WithGRPCRejectCode(codes.FailedPrecondition)}, expectedCode: codes.FailedPrecondition},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.breaker = s.newBreaker()
			unary, err := NewGRPCUnaryInterceptor(append([]Option{WithBreaker(s.breaker)}, tt.opts...)...)
			s.Require().NoError(err)
			s.breaker.ForceOpen()
			_, err = s.client(unary, nil).UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
			s.Equal(tt.expectedCode, status.Code(err))
			s.ErrorIs(err, breaker.ErrOpen)
			var re *breaker.RejectedError
			s.ErrorAs(err, &re)
			s.Equal(testName, re.Name)
			s.service.AssertNotCalled(s.T(), "UnaryCall", mock.Anything)
		})
	}
}

func (s *GRPCSuite) TestStreamRecordsRecvFailure() {
	stream, err := NewGRPCStreamInterceptor(WithBreaker(s.breaker))
	s.Require().NoError(err)
	s.service.On("StreamingOutputCall", mock.Anything, mock.Anything).Return(status.Error(codes.Internal, "late")).Once()
	out, err := s.client(nil, stream).StreamingOutputCall(context.Background(), &grpc_testing.StreamingOutputCallRequest{})
	s.Require().NoError(err)
	_, err = out.Recv()
	s.Equal(codes.Internal, status.Code(err))
	s.Equal(breaker.Counts{Total: 1, Failures: 1, ConsecutiveFailures: 1}, s.breaker.Counts())
}

func (s *GRPCSuite) TestStreamRecordsCleanEOF() {
	stream, err := NewGRPCStreamInterceptor(WithBreaker(s.breaker))
	s.Require().NoError(err)
	s.service.On("StreamingOutputCall", mock.Anything, mock.Anything).Return(nil).Once()
	out, err := s.client(nil, stream).StreamingOutputCall(context.Background(), &grpc_testing.StreamingOutputCallRequest{})
	s.Require().NoError(err)
	_, err = out.Recv()
	s.ErrorIs(err, io.EOF)
	s.Equal(breaker.Counts{Total: 1, Successes: 1, ConsecutiveSuccesses: 1}, s.breaker.Counts())
}

func (s *GRPCSuite) TestStreamRecordsClientStreamingCloseAndRecv() {
	stream, err := NewGRPCStreamInterceptor(WithBreaker(s.breaker))
	s.Require().NoError(err)
	s.service.On("StreamingInputCall", mock.Anything).Return(&grpc_testing.StreamingInputCallResponse{}, nil).Once()
	in, err := s.client(nil, stream).StreamingInputCall(context.Background())
	s.Require().NoError(err)
	resp, err := in.CloseAndRecv()
	s.Require().NoError(err)
	s.NotNil(resp)
	s.Equal(breaker.Counts{Total: 1, Successes: 1, ConsecutiveSuccesses: 1}, s.breaker.Counts())
}

func (s *GRPCSuite) TestStreamRecordsHeaderFailureOnce() {
	stream, err := NewGRPCStreamInterceptor(
		WithBreaker(s.breaker),
		WithGRPCKey(func(context.Context, string, *grpc.ClientConn) string { return testName }),
	)
	s.Require().NoError(err)
	out, err := stream(context.Background(), &grpc.StreamDesc{}, nil, "/test.Service/Stream",
		func(context.Context, *grpc.StreamDesc, *grpc.ClientConn, string, ...grpc.CallOption) (grpc.ClientStream, error) {
			return &testClientStream{headerErr: status.Error(codes.Internal, "header"), recvErr: io.EOF}, nil
		})
	s.Require().NoError(err)
	_, err = out.Header()
	s.Equal(codes.Internal, status.Code(err))
	err = out.RecvMsg(&grpc_testing.StreamingOutputCallResponse{})
	s.ErrorIs(err, io.EOF)
	s.Equal(breaker.Counts{Total: 1, Failures: 1, ConsecutiveFailures: 1}, s.breaker.Counts())
}

func (s *GRPCSuite) TestStreamSendEOFLeavesOutcomeForRecv() {
	stream, err := NewGRPCStreamInterceptor(
		WithBreaker(s.breaker),
		WithGRPCKey(func(context.Context, string, *grpc.ClientConn) string { return testName }),
	)
	s.Require().NoError(err)
	out, err := stream(context.Background(), &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, nil, "/test.Service/Stream",
		func(context.Context, *grpc.StreamDesc, *grpc.ClientConn, string, ...grpc.CallOption) (grpc.ClientStream, error) {
			return &testClientStream{sendErr: io.EOF, recvErr: io.EOF}, nil
		})
	s.Require().NoError(err)
	err = out.SendMsg(&grpc_testing.StreamingInputCallRequest{})
	s.ErrorIs(err, io.EOF)
	err = out.RecvMsg(&grpc_testing.StreamingOutputCallResponse{})
	s.ErrorIs(err, io.EOF)
	s.Equal(breaker.Counts{Total: 1, Successes: 1, ConsecutiveSuccesses: 1}, s.breaker.Counts())
}

type testClientStream struct {
	headerErr error
	recvErr   error
	sendErr   error
}

func (s *testClientStream) Header() (metadata.MD, error) { return nil, s.headerErr }
func (s *testClientStream) Trailer() metadata.MD         { return nil }
func (s *testClientStream) CloseSend() error             { return nil }
func (s *testClientStream) Context() context.Context     { return context.Background() }
func (s *testClientStream) SendMsg(any) error            { return s.sendErr }
func (s *testClientStream) RecvMsg(any) error            { return s.recvErr }

func (s *GRPCSuite) TestStreamRejected() {
	stream, err := NewGRPCStreamInterceptor(WithBreaker(s.breaker))
	s.Require().NoError(err)
	s.breaker.ForceOpen()
	out, err := s.client(nil, stream).StreamingOutputCall(context.Background(), &grpc_testing.StreamingOutputCallRequest{})
	s.Nil(out)
	s.Equal(codes.Unavailable, status.Code(err))
	s.ErrorIs(err, breaker.ErrOpen)
}

func (s *GRPCSuite) TestKeyDefaultsToTarget() {
	reg := testRegistry(s.T())
	unary, err := NewGRPCUnaryInterceptor(WithRegistry(reg))
	s.Require().NoError(err)
	s.service.On("UnaryCall", mock.Anything).Return(&grpc_testing.SimpleResponse{}, nil).Once()
	_, err = s.client(unary, nil).UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
	s.Require().NoError(err)
	_, ok := reg.Lookup(testTarget)
	s.True(ok)
}

func (s *GRPCSuite) TestCustomKeyByMethod() {
	reg := testRegistry(s.T())
	unary, err := NewGRPCUnaryInterceptor(WithRegistry(reg), WithGRPCKey(
		func(_ context.Context, method string, _ *grpc.ClientConn) string { return method },
	))
	s.Require().NoError(err)
	s.service.On("UnaryCall", mock.Anything).Return(&grpc_testing.SimpleResponse{}, nil).Once()
	_, err = s.client(unary, nil).UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
	s.Require().NoError(err)
	_, ok := reg.Lookup(grpc_testing.TestService_UnaryCall_FullMethodName)
	s.True(ok)
}

func (s *GRPCSuite) TestRegistryErrorFailsCall() {
	reg := testFailingRegistry(s.T())
	unary, err := NewGRPCUnaryInterceptor(WithRegistry(reg))
	s.Require().NoError(err)
	_, err = s.client(unary, nil).UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
	s.ErrorIs(err, ErrRegistry)
	s.service.AssertNotCalled(s.T(), "UnaryCall", mock.Anything)
}

func (s *GRPCSuite) TestClassifierPanicIsFailure() {
	unary, err := NewGRPCUnaryInterceptor(WithBreaker(s.breaker), WithGRPCClassifier(
		func(context.Context, error) breaker.Outcome { panic(testClassifyPanic) },
	))
	s.Require().NoError(err)
	s.service.On("UnaryCall", mock.Anything).Return(&grpc_testing.SimpleResponse{}, nil).Once()
	_, err = s.client(unary, nil).UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
	s.NoError(err)
	s.Equal(uint32(1), s.breaker.Counts().Failures)
	s.events.AssertCalled(s.T(), "OnHookPanic", hookClassifier, testClassifyPanic)
}

func (s *GRPCSuite) TestInvokerPanicRecordsFailure() {
	unary, err := NewGRPCUnaryInterceptor(WithBreaker(s.breaker), WithGRPCKey(
		func(context.Context, string, *grpc.ClientConn) string { return testName },
	))
	s.Require().NoError(err)
	s.Panics(func() {
		_ = unary(context.Background(), "/test.Service/Method", nil, nil, nil,
			func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
				panic("invoker boom")
			})
	})
	s.Equal(uint32(1), s.breaker.Counts().Failures)
	s.Equal(uint32(0), s.breaker.Snapshot().InFlight)
}

func (s *GRPCSuite) TestRejectedErrorUnwraps() {
	re := &breaker.RejectedError{Err: breaker.ErrOpen, Name: testName, State: breaker.StateOpen}
	err := &grpcRejectedError{RejectedError: re, code: codes.Unavailable}
	s.True(errors.Is(err, breaker.ErrOpen))
	s.Equal(codes.Unavailable, err.GRPCStatus().Code())
	s.Equal(re.Error(), err.GRPCStatus().Message())
	s.Equal(re.Error(), err.Error())
}
