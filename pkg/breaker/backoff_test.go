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

func (s *BackoffSuite) TestExponentialMaxZeroUncapped() {
	b := ExponentialBackoff{Base: time.Second, Max: 0}
	s.Equal(16*time.Second, b.Delay(4))
}

func (s *BackoffSuite) TestExponentialUncappedJitterHighAttempt() {
	const (
		samples = 100
		attempt = 40
	)
	b := ExponentialBackoff{Base: time.Second, Max: 0, Jitter: true}
	for range samples {
		var d time.Duration
		s.NotPanics(func() { d = b.Delay(attempt) })
		s.GreaterOrEqual(d, time.Duration(0))
		s.LessOrEqual(d, maxJitterDelay)
	}
}

func (s *BackoffSuite) TestExponentialValidate() {
	tests := []struct {
		name string
		b    ExponentialBackoff
		ok   bool
	}{
		{name: "valid", b: ExponentialBackoff{Base: time.Second, Max: 8 * time.Second}, ok: true},
		{name: "uncapped", b: ExponentialBackoff{Base: time.Second, Max: 0}, ok: true},
		{name: "base zero", b: ExponentialBackoff{Base: 0, Max: time.Second}},
		{name: "base negative", b: ExponentialBackoff{Base: -time.Second, Max: time.Second}},
		{name: "max below base", b: ExponentialBackoff{Base: 2 * time.Second, Max: time.Second}},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			err := tt.b.validate()
			if tt.ok {
				s.NoError(err)
				return
			}
			s.ErrorIs(err, ErrInvalidConfig)
			s.ErrorIs(err, ErrInvalidBackoff)
		})
	}
}

func (s *BackoffSuite) TestWithBackoffRejectsInvalid() {
	_, err := New(WithName("x"), WithBackoff(ExponentialBackoff{Base: 0, Max: time.Second}))
	s.ErrorIs(err, ErrInvalidConfig)
	s.ErrorIs(err, ErrInvalidBackoff)
}

func (s *BackoffSuite) TestWithBackoffRejectsInvalidPointer() {
	_, err := New(WithName("x"), WithBackoff(&ExponentialBackoff{Base: 0, Max: time.Second}))
	s.ErrorIs(err, ErrInvalidConfig)
	s.ErrorIs(err, ErrInvalidBackoff)
}

func (s *BackoffSuite) TestWithBackoffRejectsNilPointer() {
	var b *ExponentialBackoff
	_, err := New(WithName("x"), WithBackoff(b))
	s.ErrorIs(err, ErrInvalidOption)
}
