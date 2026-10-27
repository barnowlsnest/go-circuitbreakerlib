package adapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

// DefaultGRPCRejectCode is the status code of a rejected call.
const DefaultGRPCRejectCode = codes.Unavailable

// GRPCKeyFunc picks the registry key for a call. The default is cc.Target().
type GRPCKeyFunc func(ctx context.Context, method string, cc *grpc.ClientConn) string

// GRPCClassifier decides the outcome of a call from its error.
type GRPCClassifier func(ctx context.Context, err error) breaker.Outcome

// DefaultGRPCKey keys by cc.Target().
func DefaultGRPCKey(_ context.Context, _ string, cc *grpc.ClientConn) string { return cc.Target() }

// DefaultGRPCClassifier ignores a call the caller canceled and counts these
// status codes as failures: Unavailable, DeadlineExceeded, ResourceExhausted,
// Internal, Unknown, DataLoss, Aborted, and Canceled (peer-side, since the
// caller did not cancel). Every other code, including OK, is a success. A
// non-status error is Unknown, so a failure.
func DefaultGRPCClassifier(ctx context.Context, err error) breaker.Outcome {
	if errors.Is(ctx.Err(), context.Canceled) {
		return breaker.OutcomeIgnored
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Internal,
		codes.Unknown, codes.DataLoss, codes.Aborted, codes.Canceled:
		return breaker.OutcomeFailure
	default:
		return breaker.OutcomeSuccess
	}
}

// WithGRPCKey sets the registry key function.
func WithGRPCKey(fn GRPCKeyFunc) Option {
	return Option{proto: protoGRPC, apply: func(s *settings) error {
		if fn == nil {
			return fmt.Errorf("%w: nil gRPC key", ErrInvalidOption)
		}
		s.grpcKey = fn
		return nil
	}}
}

// WithGRPCClassifier sets the outcome classifier.
func WithGRPCClassifier(fn GRPCClassifier) Option {
	return Option{proto: protoGRPC, apply: func(s *settings) error {
		if fn == nil {
			return fmt.Errorf("%w: nil gRPC classifier", ErrInvalidOption)
		}
		s.grpcClassifier = fn
		return nil
	}}
}

// WithGRPCRejectCode sets the status code of a rejected call. The default,
// Unavailable, is retried by a service-config retry policy that names it,
// which re-enters the interceptor while the breaker is Open; a code such as
// FailedPrecondition avoids that. codes.OK is not allowed.
func WithGRPCRejectCode(code codes.Code) Option {
	return Option{proto: protoGRPC, apply: func(s *settings) error {
		if code == codes.OK {
			return fmt.Errorf("%w: reject code OK", ErrInvalidOption)
		}
		s.grpcRejectCode = code
		return nil
	}}
}

// grpcRejectedError is a *breaker.RejectedError that gRPC reports as a
// status with the configured code.
type grpcRejectedError struct {
	*breaker.RejectedError
	code codes.Code
}

// GRPCStatus lets status.Code and status.FromError read the rejection.
func (e *grpcRejectedError) GRPCStatus() *status.Status {
	return status.New(e.code, e.Error())
}

// Unwrap exposes the *breaker.RejectedError, so errors.Is(err, breaker.ErrOpen)
// holds.
func (e *grpcRejectedError) Unwrap() error { return e.RejectedError }

func newGRPCSettings(opts []Option) (settings, error) {
	s, err := newSettings(protoGRPC, opts)
	if err != nil {
		return s, err
	}
	if s.grpcKey == nil {
		s.grpcKey = DefaultGRPCKey
	}
	if s.grpcClassifier == nil {
		s.grpcClassifier = DefaultGRPCClassifier
	}
	if s.grpcRejectCode == codes.OK {
		s.grpcRejectCode = DefaultGRPCRejectCode
	}
	return s, nil
}

// admit resolves the breaker for the call and takes a permit. A rejection
// comes back as a status error with the configured code.
func (s *settings) admit(ctx context.Context, method string, cc *grpc.ClientConn) (*breaker.Breaker, breaker.Permit, error) {
	b, err := s.resolve(s.grpcKey(ctx, method, cc))
	if err != nil {
		return nil, breaker.Permit{}, err
	}
	p, err := b.Allow()
	if err != nil {
		var re *breaker.RejectedError
		if errors.As(err, &re) {
			return nil, breaker.Permit{}, &grpcRejectedError{RejectedError: re, code: s.grpcRejectCode}
		}
		return nil, breaker.Permit{}, err
	}
	return b, p, nil
}

// NewGRPCUnaryInterceptor returns a client interceptor that records every
// unary call.
func NewGRPCUnaryInterceptor(opts ...Option) (grpc.UnaryClientInterceptor, error) {
	s, err := newGRPCSettings(opts)
	if err != nil {
		return nil, err
	}
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		callOpts ...grpc.CallOption,
	) error {
		b, p, err := s.admit(ctx, method, cc)
		if err != nil {
			return err
		}
		start := time.Now()
		err = invoker(ctx, method, req, reply, cc, callOpts...)
		b.Record(p, classifyGRPC(s.grpcClassifier, ctx, err), time.Since(start))
		return err
	}, nil
}

// NewGRPCStreamInterceptor returns a client interceptor that records the
// outcome of stream establishment only. Errors on the stream afterwards are
// not seen by the breaker.
func NewGRPCStreamInterceptor(opts ...Option) (grpc.StreamClientInterceptor, error) {
	s, err := newGRPCSettings(opts)
	if err != nil {
		return nil, err
	}
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		callOpts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		b, p, err := s.admit(ctx, method, cc)
		if err != nil {
			return nil, err
		}
		start := time.Now()
		stream, err := streamer(ctx, desc, cc, method, callOpts...)
		b.Record(p, classifyGRPC(s.grpcClassifier, ctx, err), time.Since(start))
		return stream, err
	}, nil
}

// classifyGRPC runs fn under recover; a panicking classifier is a failure.
func classifyGRPC(fn GRPCClassifier, ctx context.Context, err error) (o breaker.Outcome) {
	defer func() {
		if r := recover(); r != nil {
			o = breaker.OutcomeFailure
		}
	}()
	return fn(ctx, err)
}
