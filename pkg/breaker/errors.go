package breaker

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidConfig is joined with a specific sentinel by New when an
	// option resolves to an unusable configuration, so errors.Is matches both.
	ErrInvalidConfig = errors.New("invalid breaker configuration")
	// ErrInvalidOption is returned by New when an option value is unusable.
	ErrInvalidOption = errors.New("breaker: invalid option")
	// ErrEmptyName is joined with ErrInvalidConfig when Name is empty.
	ErrEmptyName = errors.New("name is empty")
	// ErrNegativeDuration is joined with ErrInvalidConfig when a duration is negative.
	ErrNegativeDuration = errors.New("duration must not be negative")
	// ErrPolicyThreshold is joined with ErrInvalidConfig when a rate threshold is outside (0, 1].
	ErrPolicyThreshold = errors.New("policy threshold must be in (0, 1]")
	// ErrPolicyCount is joined with ErrInvalidConfig when a policy count or MinCalls is zero.
	ErrPolicyCount = errors.New("policy count must be positive")
	// ErrNilPolicy is joined with ErrInvalidConfig when Policies contains nil.
	ErrNilPolicy = errors.New("policy is nil")
	// ErrOpen is the cause of a RejectedError while the breaker is Open.
	ErrOpen = errors.New("circuit breaker is open")
	// ErrTooManyProbes is the cause of a RejectedError while the breaker is
	// HalfOpen and every probe slot is taken.
	ErrTooManyProbes = errors.New("circuit breaker is half-open and probe limit reached")
	// ErrNilFunc is returned by Execute and Do when fn is nil.
	ErrNilFunc = errors.New("nil function")
)

// RejectedError is returned by Allow, Execute, and Do when the breaker
// refuses a call. It unwraps to ErrOpen or ErrTooManyProbes.
type RejectedError struct {
	Err   error
	Name  string
	State State
}

// Error implements error.
func (e *RejectedError) Error() string {
	return fmt.Sprintf("circuit breaker %s is %s: %v", e.Name, e.State, e.Err)
}

// Unwrap returns the cause.
func (e *RejectedError) Unwrap() error { return e.Err }
