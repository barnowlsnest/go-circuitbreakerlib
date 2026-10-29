package registry

import "errors"

var (
	// ErrInvalidOption means an option value given to New is out of range.
	ErrInvalidOption = errors.New("registry: invalid option")
)
