package hooks

import (
	"errors"
	"fmt"
	"time"

	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"
)

const (
	// DefaultRejectEvery is the default minimum gap between rejection lines
	// for one breaker.
	DefaultRejectEvery = time.Second
	// DefaultFailureEvery is the default minimum gap between failure and slow
	// call lines for one breaker.
	DefaultFailureEvery = time.Second
)

var (
	// ErrNilLogger is returned by NewLogEvents when log is nil.
	ErrNilLogger = errors.New("hooks: nil logger")
	// ErrInvalidOption is returned by NewLogEvents when an option value is
	// out of range.
	ErrInvalidOption = errors.New("hooks: invalid option")
)

// Option configures LogEvents.
type Option func(*options) error

type options struct {
	clock        func() time.Time
	rejectEvery  time.Duration
	failureEvery time.Duration
	minLevel     logger.Level
	minLevelSet  bool
}

// WithRejectEvery sets the minimum gap between rejection lines for one
// breaker. Rejections inside the gap are counted and reported as
// "suppressed" on the next line. d must be positive.
func WithRejectEvery(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return fmt.Errorf("%w: reject every %v", ErrInvalidOption, d)
		}
		o.rejectEvery = d
		return nil
	}
}

// WithFailureEvery sets the minimum gap between failure and slow call lines
// for one breaker. d must be positive.
func WithFailureEvery(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return fmt.Errorf("%w: failure every %v", ErrInvalidOption, d)
		}
		o.failureEvery = d
		return nil
	}
}

// WithMinLevel overrides the level below which events are dropped before
// any field is built. The default is the logger's own level; set this only
// when the logger is wrapped and its level is not the effective one.
func WithMinLevel(l logger.Level) Option {
	return func(o *options) error {
		o.minLevel = l
		o.minLevelSet = true
		return nil
	}
}

// WithClock sets the time source that drives the rate limiters. The
// default is time.Now.
func WithClock(fn func() time.Time) Option {
	return func(o *options) error {
		if fn == nil {
			return fmt.Errorf("%w: nil clock", ErrInvalidOption)
		}
		o.clock = fn
		return nil
	}
}
