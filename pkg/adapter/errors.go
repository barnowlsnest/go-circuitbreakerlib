package adapter

import "errors"

var (
	// ErrInvalidOption means an option is nil, out of range, or meant for
	// another protocol.
	ErrInvalidOption = errors.New("adapter: invalid option")
	// ErrNoBreaker means neither WithBreaker nor WithRegistry was given.
	ErrNoBreaker = errors.New("adapter: WithBreaker or WithRegistry is required")
	// ErrBothBreakerAndRegistry means both WithBreaker and WithRegistry were
	// given.
	ErrBothBreakerAndRegistry = errors.New("adapter: WithBreaker and WithRegistry are exclusive")
	// ErrRegistry wraps a registry.Registry.Get error; the request fails.
	ErrRegistry = errors.New("adapter: registry")
)
