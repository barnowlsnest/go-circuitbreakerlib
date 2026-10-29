package composite

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

const (
	tripOpen     = 2
	tripTotal    = 4
	tripMinCalls = 10
)

type TripSuite struct {
	suite.Suite
}

func TestTripSuite(t *testing.T) {
	suite.Run(t, new(TripSuite))
}

func (s *TripSuite) TestOpenChildren() {
	tests := []struct {
		name     string
		min      int
		open     int
		total    int
		expected bool
	}{
		{name: "below min", min: 2, open: 1, total: 4, expected: false},
		{name: "at min", min: 2, open: 2, total: 4, expected: true},
		{name: "above min", min: 2, open: 3, total: 4, expected: true},
		{name: "no children", min: 1, open: 0, total: 0, expected: false},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			trip, err := OpenChildren(tt.min)
			s.Require().NoError(err)
			s.Equal(tt.expected, trip(tt.open, tt.total, breaker.Counts{}))
		})
	}
}

func (s *TripSuite) TestOpenChildrenRatio() {
	tests := []struct {
		name     string
		ratio    float64
		open     int
		total    int
		expected bool
	}{
		{name: "below ratio", ratio: 0.5, open: 1, total: 4, expected: false},
		{name: "at ratio", ratio: 0.5, open: 2, total: 4, expected: true},
		{name: "above ratio", ratio: 0.5, open: 3, total: 4, expected: true},
		{name: "zero ratio with none open", ratio: 0, open: 0, total: 4, expected: true},
		{name: "no children is never tripped", ratio: 0, open: 0, total: 0, expected: false},
		{name: "ratio one needs all", ratio: 1, open: 3, total: 4, expected: false},
		{name: "ratio one all open", ratio: 1, open: 4, total: 4, expected: true},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			trip, err := OpenChildrenRatio(tt.ratio)
			s.Require().NoError(err)
			s.Equal(tt.expected, trip(tt.open, tt.total, breaker.Counts{}))
		})
	}
}

func (s *TripSuite) TestAggregateFailureRate() {
	tests := []struct {
		name      string
		threshold float64
		minCalls  uint32
		agg       breaker.Counts
		expected  bool
	}{
		{name: "below min calls", threshold: 0.5, minCalls: tripMinCalls, agg: breaker.Counts{Total: 9, Failures: 9}, expected: false},
		{name: "at min calls below rate", threshold: 0.5, minCalls: tripMinCalls, agg: breaker.Counts{Total: 10, Failures: 4}, expected: false},
		{name: "at min calls at rate", threshold: 0.5, minCalls: tripMinCalls, agg: breaker.Counts{Total: 10, Failures: 5}, expected: true},
		{name: "zero threshold no calls", threshold: 0, minCalls: 0, agg: breaker.Counts{}, expected: false},
		{name: "zero threshold one call", threshold: 0, minCalls: 0, agg: breaker.Counts{Total: 1}, expected: true},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			trip, err := AggregateFailureRate(tt.threshold, tt.minCalls)
			s.Require().NoError(err)
			s.Equal(tt.expected, trip(0, 0, tt.agg))
		})
	}
}

func (s *TripSuite) TestAllOfAnyOf() {
	agg := breaker.Counts{Total: tripMinCalls}
	tests := []struct {
		name     string
		combine  func(...TripFunc) (TripFunc, error)
		results  []bool
		expected bool
	}{
		{name: "all of true true", combine: AllOf, results: []bool{true, true}, expected: true},
		{name: "all of true false", combine: AllOf, results: []bool{true, false}, expected: false},
		{name: "all of empty", combine: AllOf, results: nil, expected: false},
		{name: "any of false true", combine: AnyOf, results: []bool{false, true}, expected: true},
		{name: "any of false false", combine: AnyOf, results: []bool{false, false}, expected: false},
		{name: "any of empty", combine: AnyOf, results: nil, expected: false},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			var fns []TripFunc
			var mocks []*MockTrip
			for _, r := range tt.results {
				m := &MockTrip{}
				m.On("Trip", tripOpen, tripTotal, agg).Return(r).Maybe()
				mocks = append(mocks, m)
				fns = append(fns, m.Trip)
			}
			trip, err := tt.combine(fns...)
			s.Require().NoError(err)
			s.Equal(tt.expected, trip(tripOpen, tripTotal, agg))
			for _, m := range mocks {
				m.AssertExpectations(s.T())
			}
		})
	}
}

func (s *TripSuite) TestAllOfStopsAtFirstFalse() {
	first := &MockTrip{}
	second := &MockTrip{}
	first.On("Trip", tripOpen, tripTotal, breaker.Counts{}).Return(false).Once()
	trip, err := AllOf(first.Trip, second.Trip)
	s.Require().NoError(err)
	s.False(trip(tripOpen, tripTotal, breaker.Counts{}))
	first.AssertExpectations(s.T())
	second.AssertNotCalled(s.T(), "Trip", mock.Anything, mock.Anything, mock.Anything)
}

func (s *TripSuite) TestInvalidArguments() {
	tests := []struct {
		build func() (TripFunc, error)
		name  string
	}{
		{name: "open children zero", build: func() (TripFunc, error) { return OpenChildren(0) }},
		{name: "open children negative", build: func() (TripFunc, error) { return OpenChildren(-1) }},
		{name: "ratio above one", build: func() (TripFunc, error) { return OpenChildrenRatio(1.1) }},
		{name: "ratio negative", build: func() (TripFunc, error) { return OpenChildrenRatio(-0.1) }},
		{name: "threshold above one", build: func() (TripFunc, error) { return AggregateFailureRate(1.1, 0) }},
		{name: "threshold negative", build: func() (TripFunc, error) { return AggregateFailureRate(-0.1, 0) }},
		{name: "all of nil func", build: func() (TripFunc, error) { return AllOf(nil) }},
		{name: "any of nil func", build: func() (TripFunc, error) { return AnyOf(nil) }},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			trip, err := tt.build()
			s.ErrorIs(err, ErrInvalidConfig)
			s.Nil(trip)
		})
	}
}
