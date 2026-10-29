package composite

import (
	"fmt"
	"time"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

// Option configures a Composite. An option with an invalid value makes New
// return ErrInvalidConfig or ErrInvalidOption.
type Option func(*options) error

type options struct {
	events       breaker.Events
	trip         TripFunc
	name         string
	parentOpts   []breaker.Option
	childOpts    []breaker.Option
	scanInterval time.Duration
	nameSet      bool
	scanSet      bool
}

// WithName sets the composite (parent breaker) name. When unset, WithParent
// may supply one via breaker.WithName.
func WithName(name string) Option {
	return func(o *options) error {
		if name == "" {
			return fmt.Errorf("%w: empty name", ErrInvalidConfig)
		}
		o.name = name
		o.nameSet = true
		return nil
	}
}

// WithEvents sets the Events used for the parent and for children that do
// not set their own. The default is NoopEvents.
func WithEvents(ev breaker.Events) Option {
	return func(o *options) error {
		if ev == nil {
			return fmt.Errorf("%w: nil events", ErrInvalidOption)
		}
		o.events = ev
		return nil
	}
}

// WithTrip sets the parent trip decision. The default is
// AllOf(OpenChildrenRatio(0.5), AggregateFailureRate(0.5, 50)).
func WithTrip(trip TripFunc) Option {
	return func(o *options) error {
		if trip == nil {
			return fmt.Errorf("%w: nil trip", ErrInvalidOption)
		}
		o.trip = trip
		return nil
	}
}

// WithScanInterval sets how stale the open-children cache may be before a
// Record or Pick recounts. The default is DefaultScanInterval.
func WithScanInterval(d time.Duration) Option {
	return func(o *options) error {
		if d < 0 {
			return fmt.Errorf("%w: ScanInterval %v", ErrInvalidConfig, d)
		}
		o.scanInterval = d
		o.scanSet = true
		return nil
	}
}

// WithParent sets options for the parent breaker. Policies must not be set;
// Trip is the parent policy. ReopenEarly is reserved by the composite.
func WithParent(opts ...breaker.Option) Option {
	return func(o *options) error {
		if err := breaker.ValidateNoPolicies(opts...); err != nil {
			return err
		}
		o.parentOpts = opts
		return nil
	}
}

// WithChild sets the options applied to every leaf AddChild creates. The
// leaf name is the child id; FillEvents from WithEvents still applies when
// these options leave Events unset.
func WithChild(opts ...breaker.Option) Option {
	return func(o *options) error {
		o.childOpts = opts
		return nil
	}
}
