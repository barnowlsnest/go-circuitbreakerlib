package adapter

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

// ContextDialer is what net.Dialer and tls.Dialer implement.
type ContextDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// TCPKeyFunc picks the registry key for a dial. The default is the address.
type TCPKeyFunc func(network, address string) string

// TCPClassifier decides the outcome of a dial from its error.
type TCPClassifier func(ctx context.Context, err error) breaker.Outcome

// DefaultTCPKey keys by address.
func DefaultTCPKey(_, address string) string { return address }

// DefaultTCPClassifier ignores a dial the caller canceled and counts every
// other error, including a deadline, as a failure.
func DefaultTCPClassifier(ctx context.Context, err error) breaker.Outcome {
	return breaker.DefaultClassifier(ctx, err)
}

// WithTCPKey sets the registry key function.
func WithTCPKey(fn TCPKeyFunc) Option {
	return Option{proto: protoTCP, apply: func(s *settings) error {
		if fn == nil {
			return fmt.Errorf("%w: nil TCP key", ErrInvalidOption)
		}
		s.tcpKey = fn
		return nil
	}}
}

// WithTCPClassifier sets the dial outcome classifier.
func WithTCPClassifier(fn TCPClassifier) Option {
	return Option{proto: protoTCP, apply: func(s *settings) error {
		if fn == nil {
			return fmt.Errorf("%w: nil TCP classifier", ErrInvalidOption)
		}
		s.tcpClassifier = fn
		return nil
	}}
}

// WithTCPTrackIO wraps every dialed conn so each Read and Write asks the
// breaker first. Only Writes act as half-open probes; a Read never holds a
// probe slot. Off by default, because the wrapper hides *net.TCPConn from
// TLS and HTTP/2 stacks that type-assert on it.
func WithTCPTrackIO(enabled bool) Option {
	return Option{proto: protoTCP, apply: func(s *settings) error {
		s.tcpTrackIO = enabled
		return nil
	}}
}

// TCPDialer is a breaker-protected ContextDialer.
type TCPDialer struct {
	next     ContextDialer
	settings settings
}

// NewTCPDialer wraps next with a breaker. A nil next means a zero
// net.Dialer. Every dial outcome is recorded.
func NewTCPDialer(next ContextDialer, opts ...Option) (*TCPDialer, error) {
	s, err := newSettings(protoTCP, opts)
	if err != nil {
		return nil, err
	}
	if next == nil {
		next = &net.Dialer{}
	}
	return &TCPDialer{next: next, settings: s}, nil
}

// DialContext dials through the breaker. A rejection returns a
// *breaker.RejectedError without dialing. With WithTCPTrackIO the returned
// conn is wrapped; see trackedConn.
func (d *TCPDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	b, p, err := d.settings.acquire(d.settings.tcpKey(network, address))
	if err != nil {
		return nil, err
	}
	start := time.Now()
	conn, err := d.next.DialContext(ctx, network, address)
	b.Record(p, classify(b, func() breaker.Outcome { return d.settings.tcpClassifier(ctx, err) }), time.Since(start))
	if err != nil {
		return nil, err
	}
	if d.settings.tcpTrackIO {
		return &trackedConn{Conn: conn, breaker: b}, nil
	}
	return conn, nil
}
