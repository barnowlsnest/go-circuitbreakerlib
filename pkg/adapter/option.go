package adapter

import (
	"fmt"

	"google.golang.org/grpc/codes"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/registry"
)

const hookClassifier = "classifier"

// proto tags an option with the protocol it belongs to.
type proto uint8

const (
	protoAny proto = iota
	protoHTTP
	protoGRPC
	protoTCP
)

func (p proto) String() string {
	switch p {
	case protoHTTP:
		return "HTTP"
	case protoGRPC:
		return "gRPC"
	case protoTCP:
		return "TCP"
	default:
		return "any"
	}
}

// Option configures an adapter. WithBreaker and WithRegistry apply to every
// protocol; the others are prefixed with their protocol and are rejected
// with ErrInvalidOption by a constructor of another protocol.
type Option struct {
	apply func(*settings) error
	proto proto
}

// settings is the resolved option set.
type settings struct {
	breaker        *breaker.Breaker
	registry       *registry.Registry
	httpKey        HTTPKeyFunc
	httpClassifier HTTPClassifier
	grpcKey        GRPCKeyFunc
	grpcClassifier GRPCClassifier
	tcpKey         TCPKeyFunc
	tcpClassifier  TCPClassifier
	grpcRejectCode codes.Code
	tcpTrackIO     bool
}

// WithBreaker protects every call with one breaker.
func WithBreaker(b *breaker.Breaker) Option {
	return Option{proto: protoAny, apply: func(s *settings) error {
		if b == nil {
			return fmt.Errorf("%w: nil breaker", ErrInvalidOption)
		}
		s.breaker = b
		return nil
	}}
}

// WithRegistry protects each call with the registry breaker for its key.
func WithRegistry(r *registry.Registry) Option {
	return Option{proto: protoAny, apply: func(s *settings) error {
		if r == nil {
			return fmt.Errorf("%w: nil registry", ErrInvalidOption)
		}
		s.registry = r
		return nil
	}}
}

// newSettings applies opts for want and checks that exactly one breaker
// source is set.
func newSettings(want proto, opts []Option) (settings, error) {
	var s settings
	for _, opt := range opts {
		if opt.proto != protoAny && opt.proto != want {
			return s, fmt.Errorf("%w: %s option given to the %s adapter", ErrInvalidOption, opt.proto, want)
		}
		if err := opt.apply(&s); err != nil {
			return s, err
		}
	}
	switch {
	case s.breaker == nil && s.registry == nil:
		return s, ErrNoBreaker
	case s.breaker != nil && s.registry != nil:
		return s, ErrBothBreakerAndRegistry
	}
	return s, nil
}

// resolve returns the breaker for key: the fixed one, or the registry's.
func (s *settings) resolve(key string) (*breaker.Breaker, error) {
	if s.breaker != nil {
		return s.breaker, nil
	}
	b, err := s.registry.Get(key)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrRegistry, err)
	}
	return b, nil
}
