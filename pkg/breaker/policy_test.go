package breaker

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type PolicySuite struct {
	suite.Suite
}

func TestPolicySuite(t *testing.T) {
	suite.Run(t, new(PolicySuite))
}

func (s *PolicySuite) TestShouldTrip() {
	const (
		minCalls  = 10
		threshold = 0.5
	)
	cases := []struct {
		policy   Policy
		name     string
		counts   Counts
		expected bool
	}{
		{name: "rate below min calls", policy: FailureRate{Threshold: threshold, MinCalls: minCalls},
			counts: Counts{Total: minCalls - 1, Failures: minCalls - 1}, expected: false},
		{name: "rate at min calls and threshold", policy: FailureRate{Threshold: threshold, MinCalls: minCalls},
			counts: Counts{Total: minCalls, Failures: 5}, expected: true},
		{name: "rate under threshold", policy: FailureRate{Threshold: threshold, MinCalls: minCalls},
			counts: Counts{Total: minCalls, Failures: 4}, expected: false},
		{name: "consecutive reached", policy: ConsecutiveFailures{Count: 3},
			counts: Counts{ConsecutiveFailures: 3}, expected: true},
		{name: "consecutive not reached", policy: ConsecutiveFailures{Count: 3},
			counts: Counts{ConsecutiveFailures: 2}, expected: false},
		{name: "slow rate", policy: SlowCallRate{Threshold: threshold, MinCalls: 2},
			counts: Counts{Total: 2, Slow: 1}, expected: true},
		{name: "slow rate below min", policy: SlowCallRate{Threshold: threshold, MinCalls: 2},
			counts: Counts{Total: 1, Slow: 1}, expected: false},
		{name: "failure count", policy: FailureCount{Count: 2},
			counts: Counts{Failures: 2}, expected: true},
		{name: "failure count below", policy: FailureCount{Count: 2},
			counts: Counts{Failures: 1}, expected: false},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.Equal(tc.expected, tc.policy.ShouldTrip(tc.counts))
		})
	}
}

func (s *PolicySuite) TestNames() {
	cases := []struct {
		policy   Policy
		expected string
	}{
		{policy: FailureRate{}, expected: "failure-rate"},
		{policy: ConsecutiveFailures{}, expected: "consecutive-failures"},
		{policy: SlowCallRate{}, expected: "slow-call-rate"},
		{policy: FailureCount{}, expected: "failure-count"},
	}
	for _, tc := range cases {
		s.Run(tc.expected, func() {
			s.Equal(tc.expected, tc.policy.Name())
		})
	}
}
