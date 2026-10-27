package breaker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type BackoffSuite struct {
	suite.Suite
}

func TestBackoffSuite(t *testing.T) {
	suite.Run(t, new(BackoffSuite))
}

func (s *BackoffSuite) TestConstant() {
	const d = 30 * time.Second
	s.Equal(d, ConstantBackoff(d).Delay(5))
}

func (s *BackoffSuite) TestExponential() {
	b := ExponentialBackoff{Base: time.Second, Max: 8 * time.Second}
	cases := []struct {
		attempt  int
		expected time.Duration
	}{
		{attempt: 0, expected: time.Second},
		{attempt: 1, expected: 2 * time.Second},
		{attempt: 2, expected: 4 * time.Second},
		{attempt: 3, expected: 8 * time.Second},
		{attempt: 4, expected: 8 * time.Second},
		{attempt: -1, expected: 0},
	}
	for _, tc := range cases {
		s.Equal(tc.expected, b.Delay(tc.attempt))
	}
}

func (s *BackoffSuite) TestExponentialJitterWithinCap() {
	const samples = 100
	b := ExponentialBackoff{Base: time.Second, Max: 8 * time.Second, Jitter: true}
	for range samples {
		d := b.Delay(3)
		s.GreaterOrEqual(d, time.Duration(0))
		s.LessOrEqual(d, 8*time.Second)
	}
}
