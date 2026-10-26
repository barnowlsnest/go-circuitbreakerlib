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
// breaker without ordinary traffic flooding the window.
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
	return c.track(func() (int, error) { return c.Conn.Read(p) })
}

// Write implements net.Conn.
func (c *trackedConn) Write(p []byte) (int, error) {
	return c.track(func() (int, error) { return c.Conn.Write(p) })
}

// ReadFrom implements io.ReaderFrom. A conn that supports it is used
// directly under one permit; otherwise the data goes through Write.
func (c *trackedConn) ReadFrom(r io.Reader) (int64, error) {
	rf, ok := c.Conn.(io.ReaderFrom)
	if !ok {
		return io.Copy(writerOnly{c}, r)
	}
	return c.track64(func() (int64, error) { return rf.ReadFrom(r) })
}

// WriteTo implements io.WriterTo. A conn that supports it is used directly
// under one permit; otherwise the data goes through Read.
func (c *trackedConn) WriteTo(w io.Writer) (int64, error) {
	wt, ok := c.Conn.(io.WriterTo)
	if !ok {
		return io.Copy(w, readerOnly{c})
	}
	return c.track64(func() (int64, error) { return wt.WriteTo(w) })
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

// track runs op under one permit and records its outcome.
func (c *trackedConn) track(op func() (int, error)) (int, error) {
	p, err := c.breaker.Allow()
	if err != nil {
		return 0, rejectConn(err)
	}
	start := time.Now()
	n, err := op()
	c.breaker.Record(p, classifyIO(p, err), time.Since(start))
	return n, err
}

// track64 is track for the 64-bit copy interfaces.
func (c *trackedConn) track64(op func() (int64, error)) (int64, error) {
	p, err := c.breaker.Allow()
	if err != nil {
		return 0, rejectConn(err)
	}
	start := time.Now()
	n, err := op()
	c.breaker.Record(p, classifyIO(p, err), time.Since(start))
	return n, err
}

// rejectConn wraps a rejection so it satisfies net.Error.
func rejectConn(err error) error {
	var re *breaker.RejectedError
	if errors.As(err, &re) {
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
