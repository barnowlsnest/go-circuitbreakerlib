package composite

import (
	"fmt"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

// TripFunc decides whether the parent opens. openChildren counts children
// whose State is Open; totalChildren is the number of children; aggregate
// is the parent's own counts (live in Closed, frozen at trip time while
// Open and HalfOpen). It runs on every parent Record and on every parent
// Allow while Open.
type TripFunc func(openChildren, totalChildren int, aggregate breaker.Counts) bool

// OpenChildren trips when at least minOpen children are Open. It returns
// ErrInvalidConfig if minOpen is less than 1, since 0 would mean "always
// tripped".
func OpenChildren(minOpen int) (TripFunc, error) {
	if minOpen < 1 {
		return nil, fmt.Errorf("%w: OpenChildren min %d, want >= 1", ErrInvalidConfig, minOpen)
	}
	return func(open, _ int, _ breaker.Counts) bool {
		return open >= minOpen
	}, nil
}

// OpenChildrenRatio trips when open/total >= ratio. With no children it
// never trips. It returns ErrInvalidConfig unless 0 <= ratio <= 1.
func OpenChildrenRatio(ratio float64) (TripFunc, error) {
	if ratio < 0 || ratio > 1 {
		return nil, fmt.Errorf("%w: OpenChildrenRatio %v, want 0..1", ErrInvalidConfig, ratio)
	}
	return openChildrenRatio(ratio), nil
}

func openChildrenRatio(ratio float64) TripFunc {
	return func(open, total int, _ breaker.Counts) bool {
		return total > 0 && float64(open)/float64(total) >= ratio
	}
}

// AggregateFailureRate trips when the parent's aggregate has at least
// minCalls calls and failures/total >= threshold. With no calls it never
// trips. It returns ErrInvalidConfig unless 0 <= threshold <= 1.
func AggregateFailureRate(threshold float64, minCalls uint32) (TripFunc, error) {
	if threshold < 0 || threshold > 1 {
		return nil, fmt.Errorf("%w: AggregateFailureRate %v, want 0..1", ErrInvalidConfig, threshold)
	}
	return aggregateFailureRate(threshold, minCalls), nil
}

func aggregateFailureRate(threshold float64, minCalls uint32) TripFunc {
	rate := breaker.FailureRate{Threshold: threshold, MinCalls: minCalls}
	return func(_, _ int, agg breaker.Counts) bool { return rate.ShouldTrip(agg) }
}

// AllOf trips when every fn trips, stopping at the first that does not.
// With no fns it never trips. It returns ErrInvalidConfig on a nil fn.
func AllOf(fns ...TripFunc) (TripFunc, error) {
	if err := rejectNil(fns); err != nil {
		return nil, err
	}
	return allOf(fns...), nil
}

func allOf(fns ...TripFunc) TripFunc {
	if len(fns) == 0 {
		return func(int, int, breaker.Counts) bool { return false }
	}
	return func(open, total int, agg breaker.Counts) bool {
		for _, fn := range fns {
			if !fn(open, total, agg) {
				return false
			}
		}
		return true
	}
}

// AnyOf trips when any fn trips, stopping at the first that does. With no
// fns it never trips. It returns ErrInvalidConfig on a nil fn.
func AnyOf(fns ...TripFunc) (TripFunc, error) {
	if err := rejectNil(fns); err != nil {
		return nil, err
	}
	return func(open, total int, agg breaker.Counts) bool {
		for _, fn := range fns {
			if fn(open, total, agg) {
				return true
			}
		}
		return false
	}, nil
}

func rejectNil(fns []TripFunc) error {
	for i, fn := range fns {
		if fn == nil {
			return fmt.Errorf("%w: nil TripFunc at %d", ErrInvalidConfig, i)
		}
	}
	return nil
}
