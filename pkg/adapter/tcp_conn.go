package adapter

import (
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

// trackedConn asks the breaker before every Read and Write. When the
// breaker is Open the operation fails fast with a *breaker.RejectedError
// that also implements net.Error, without touching the socket. An error
// other than io.EOF, net.ErrClosed, and a deadline is a failure; a deadline
// is ignored because a stalled peer and a short caller deadline look the
// same here. A successful operation counts as a success only when the
// permit is a half-open probe, so pooled connections can close a HalfOpen
// breaker without ordinary traffic flooding the window. Only Writes probe:
// a Read handed a probe slot gives it back at once and runs untracked,
// because a Read on an idle pooled connection can block for as long as the
// peer stays quiet and would hold the HalfOpen breaker hostage.
type trackedConn struct {
	net.Conn
	breaker *breaker.Breaker
}

// connRejectedError is a rejection that bufio, TLS, and HTTP/2 treat as an
// ordinary fatal I/O error.
type connRejectedError struct {
	*breaker.RejectedError
}

func (*connRejectedError) Timeout() bool   { return false }
func (*connRejectedError) Temporary() bool { return false }

// Unwrap exposes the *breaker.RejectedError.
func (e *connRejectedError) Unwrap() error { return e.RejectedError }

// Unwrap returns the conn underneath.
func (c *trackedConn) Unwrap() net.Conn { return c.Conn }

// Read implements net.Conn.
func (c *trackedConn) Read(p []byte) (int, error) {
	return track(c, func() (int, error) { return c.Conn.Read(p) }, readOp)
}

// Write implements net.Conn.
func (c *trackedConn) Write(p []byte) (int, error) {
	return track(c, func() (int, error) { return c.Conn.Write(p) }, writeOp)
}

// ReadFrom implements io.ReaderFrom. A conn that supports it is used
// directly under one permit; otherwise the data goes through Write.
func (c *trackedConn) ReadFrom(r io.Reader) (int64, error) {
	rf, ok := c.Conn.(io.ReaderFrom)
	if !ok {
		return io.Copy(writerOnly{c}, r)
	}
	return track(c, func() (int64, error) { return rf.ReadFrom(r) }, writeOp)
}

// WriteTo implements io.WriterTo. A conn that supports it is used directly
// under one permit; otherwise the data goes through Read.
func (c *trackedConn) WriteTo(w io.Writer) (int64, error) {
	wt, ok := c.Conn.(io.WriterTo)
	if !ok {
		return io.Copy(w, readerOnly{c})
	}
	return track(c, func() (int64, error) { return wt.WriteTo(w) }, readOp)
}

// CloseRead forwards to the conn, or returns errors.ErrUnsupported.
func (c *trackedConn) CloseRead() error {
	if cr, ok := c.Conn.(interface{ CloseRead() error }); ok {
		return cr.CloseRead()
	}
	return errors.ErrUnsupported
}

// CloseWrite forwards to the conn, or returns errors.ErrUnsupported.
func (c *trackedConn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return errors.ErrUnsupported
}

// SyscallConn forwards to the conn, or returns errors.ErrUnsupported.
func (c *trackedConn) SyscallConn() (syscall.RawConn, error) {
	if sc, ok := c.Conn.(syscall.Conn); ok {
		return sc.SyscallConn()
	}
	return nil, errors.ErrUnsupported
}

// ioKind says whether a tracked operation reads from or writes to the peer.
type ioKind bool

const (
	readOp  ioKind = true
	writeOp ioKind = false
)

// track runs op on c under one permit and records its outcome.
func track[N int | int64](c *trackedConn, op func() (N, error), kind ioKind) (N, error) {
	p, tracked, err := c.allow(kind)
	if err != nil {
		return 0, err
	}
	if !tracked {
		return op()
	}
	start := time.Now()
	n, err := op()
	if outcome := classifyIO(p, err); outcome != breaker.OutcomeIgnored || p.IsProbe() {
		c.breaker.Record(p, outcome, time.Since(start))
	}
	return n, err
}

// allow takes a permit for one operation. A read that is handed a probe
// slot returns it immediately and reports tracked=false, so the read runs
// without a permit and never counts as a probe.
func (c *trackedConn) allow(kind ioKind) (p breaker.Permit, tracked bool, err error) {
	p, err = c.breaker.Allow()
	if err != nil {
		return breaker.Permit{}, false, rejectConn(err)
	}
	if kind == readOp && p.IsProbe() {
		c.breaker.Record(p, breaker.OutcomeIgnored, 0)
		return breaker.Permit{}, false, nil
	}
	return p, true, nil
}

// rejectConn wraps a rejection so it satisfies net.Error.
func rejectConn(err error) error {
	if re, ok := asRejected(err); ok {
		return &connRejectedError{RejectedError: re}
	}
	return err
}

// classifyIO decides the outcome of one Read or Write.
func classifyIO(p breaker.Permit, err error) breaker.Outcome {
	if err == nil {
		if p.IsProbe() {
			return breaker.OutcomeSuccess
		}
		return breaker.OutcomeIgnored
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, os.ErrDeadlineExceeded) {
		return breaker.OutcomeIgnored
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return breaker.OutcomeIgnored
	}
	return breaker.OutcomeFailure
}

// writerOnly hides ReadFrom so io.Copy uses Write.
type writerOnly struct {
	io.Writer
}

// readerOnly hides WriteTo so io.Copy uses Read.
type readerOnly struct {
	io.Reader
}
