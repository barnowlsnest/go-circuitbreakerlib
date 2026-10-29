package composite

import (
	"errors"
	"fmt"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

// ErrInvalidConfig is breaker.ErrInvalidConfig; New wraps every
// configuration error in it.
var ErrInvalidConfig = breaker.ErrInvalidConfig

var (
	// ErrInvalidOption means an option value given to New is unusable.
	ErrInvalidOption = errors.New("composite: invalid option")
	// ErrDuplicateChild means a child with that name is already attached.
	ErrDuplicateChild = errors.New("composite: duplicate child")
	// ErrNilChild means AttachChild was given nil.
	ErrNilChild = errors.New("composite: nil child")
	// ErrAlreadyAttached means the composite already has a parent.
	ErrAlreadyAttached = errors.New("composite: already attached to a parent")
	// ErrCycle means the child is the composite itself or one of its
	// ancestors.
	ErrCycle = errors.New("composite: child is an ancestor")
	// ErrTooDeep means attaching the child would put a leaf deeper than
	// MaxDepth.
	ErrTooDeep = fmt.Errorf("composite: tree deeper than %d", MaxDepth)
	// ErrNoHealthyChildren means every child rejected the call. It unwraps
	// to breaker.ErrOpen.
	ErrNoHealthyChildren = fmt.Errorf("composite: no healthy children: %w", breaker.ErrOpen)
)
