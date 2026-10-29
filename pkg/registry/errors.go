package registry

import "errors"

var (
	// ErrNilFactory means New was given a nil factory.
	ErrNilFactory = errors.New("registry: nil factory")
	// ErrInvalidOption means an option value given to New is out of range.
	ErrInvalidOption = errors.New("registry: invalid option")
)
