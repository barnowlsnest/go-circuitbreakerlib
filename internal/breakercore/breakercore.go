package breakercore

import (
	"time"
	_ "unsafe"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

// Settings is the resolved breaker metadata used by sibling packages.
type Settings struct {
	Clock      func() time.Time
	Classifier breaker.Classifier
	Events     breaker.Events
	Name       string
}

// FillEvents fills an unset breaker event sink.
//
//go:linkname FillEvents github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker.fillEvents
func FillEvents(ev breaker.Events) breaker.Option

// ValidateNoPolicies rejects parent policy overrides.
//
//go:linkname ValidateNoPolicies github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker.validateNoPolicies
func ValidateNoPolicies(opts ...breaker.Option) error

// ValidateNoReopenEarly rejects parent reopen overrides.
//
//go:linkname ValidateNoReopenEarly github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker.validateNoReopenEarly
func ValidateNoReopenEarly(opts ...breaker.Option) error

// ResolveSettings resolves breaker metadata without construction.
//
//go:linkname ResolveSettings github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker.resolveSettings
func ResolveSettings(opts ...breaker.Option) (Settings, error)
