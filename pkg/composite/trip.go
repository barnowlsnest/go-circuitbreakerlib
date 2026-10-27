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

// OpenChildren trips when at least minOpen children are Open. It panics if
// minOpen is less than 1, since 0 would mean "always tripped". Like
// regexp.MustCompile, a bad argument fails at startup, never at request
// time.
func OpenChildren(minOpen int) TripFunc {
	if minOpen < 1 {
		panic(fmt.Errorf("%w: OpenChildren min %d, want >= 1", breaker.ErrInvalidConfig, minOpen))
	}
	return func(open, _ int, _ breaker.Counts) bool {
		return open >= minOpen
	}
}

// OpenChildrenRatio trips when open/total >= ratio. With no children it
// never trips. It panics unless 0 <= ratio <= 1.
func OpenChildrenRatio(ratio float64) TripFunc {
	if ratio < 0 || ratio > 1 {
		panic(fmt.Errorf("%w: OpenChildrenRatio %v, want 0..1", breaker.ErrInvalidConfig, ratio))
	}
	return func(open, total int, _ breaker.Counts) bool {
		return total > 0 && float64(open)/float64(total) >= ratio
	}
}

// AggregateFailureRate trips when the parent's aggregate has at least
// minCalls calls and failures/total >= threshold. With no calls it never
// trips. It panics unless 0 <= threshold <= 1.
func AggregateFailureRate(threshold float64, minCalls uint32) TripFunc {
	if threshold < 0 || threshold > 1 {
		panic(fmt.Errorf("%w: AggregateFailureRate %v, want 0..1", breaker.ErrInvalidConfig, threshold))
	}
	return func(_, _ int, agg breaker.Counts) bool {
		return agg.Total > 0 && agg.Total >= minCalls && float64(agg.Failures)/float64(agg.Total) >= threshold
	}
}

// AllOf trips when every fn trips, stopping at the first that does not.
// With no fns it never trips. It panics on a nil fn.
func AllOf(fns ...TripFunc) TripFunc {
	rejectNil(fns)
	return func(open, total int, agg breaker.Counts) bool {
		if len(fns) == 0 {
			return false
		}
		for _, fn := range fns {
			if !fn(open, total, agg) {
				return false
			}
		}
		return true
	}
}

// AnyOf trips when any fn trips, stopping at the first that does. With no
// fns it never trips. It panics on a nil fn.
func AnyOf(fns ...TripFunc) TripFunc {
	rejectNil(fns)
	return func(open, total int, agg breaker.Counts) bool {
		for _, fn := range fns {
			if fn(open, total, agg) {
				return true
			}
		}
		return false
	}
}

func rejectNil(fns []TripFunc) {
	for i, fn := range fns {
		if fn == nil {
			panic(fmt.Errorf("%w: nil TripFunc at %d", breaker.ErrInvalidConfig, i))
		}
	}
}
