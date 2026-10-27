package registry

import "errors"

var (
	// ErrNilFactory is the panic value of New when factory is nil.
	ErrNilFactory = errors.New("registry: nil factory")
	// ErrInvalidOption is the panic value of New when an option value is out
	// of range.
	ErrInvalidOption = errors.New("registry: invalid option")
)
