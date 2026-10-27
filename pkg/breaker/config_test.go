package breaker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type ConfigSuite struct {
	suite.Suite
}

func TestConfigSuite(t *testing.T) {
	suite.Run(t, new(ConfigSuite))
}

func (s *ConfigSuite) TestEmptyName() {
	_, err := (&Config{}).withDefaults()
	s.ErrorIs(err, ErrInvalidConfig)
	s.ErrorIs(err, ErrEmptyName)
	s.Equal("invalid breaker configuration\nname is empty", err.Error())
}

func (s *ConfigSuite) TestInvalid() {
	cases := []struct {
		name string
		cfg  Config
	}{
		{name: "rate over one", cfg: Config{Name: "x", Policies: []Policy{FailureRate{Threshold: 1.5, MinCalls: 1}}}},
		{name: "negative open duration", cfg: Config{Name: "x", OpenDuration: -time.Second}},
		{name: "bad window", cfg: Config{Name: "x", Window: SlidingCountWindow{Size: 0}}},
		{name: "zero successes with max calls", cfg: Config{Name: "x", HalfOpenMaxCalls: 2, HalfOpenSuccesses: 0, Policies: nil}},
	}
	for i := range cases {
		tc := &cases[i]
		s.Run(tc.name, func() {
			if tc.name == "zero successes with max calls" {
				cfg, err := tc.cfg.withDefaults()
				s.NoError(err)
				s.Equal(uint32(2), cfg.HalfOpenSuccesses)
				return
			}
			_, err := tc.cfg.withDefaults()
			s.ErrorIs(err, ErrInvalidConfig)
		})
	}
}

func (s *ConfigSuite) TestDefaults() {
	cfg, err := (&Config{Name: "x"}).withDefaults()
	s.Require().NoError(err)
	s.Equal(SlidingTimeWindow{Size: 10 * time.Second, Buckets: 10}, cfg.Window)
	s.Equal([]Policy{FailureRate{Threshold: 0.5, MinCalls: 20}, ConsecutiveFailures{Count: 5}}, cfg.Policies)
	s.Equal(30*time.Second, cfg.OpenDuration)
	s.Equal(60*time.Second, cfg.ProbeTimeout)
	s.Equal(uint32(1), cfg.HalfOpenMaxCalls)
	s.Equal(uint32(1), cfg.HalfOpenSuccesses)
	s.NotNil(cfg.Classifier)
	s.NotNil(cfg.Events)
	s.NotNil(cfg.Clock)
	s.Equal(30*time.Second, cfg.Backoff.Delay(0))
}

func (s *ConfigSuite) TestSuccessesIndependentOfMaxCalls() {
	cfg, err := (&Config{Name: "x", HalfOpenMaxCalls: 1, HalfOpenSuccesses: 3}).withDefaults()
	s.NoError(err)
	s.Equal(uint32(3), cfg.HalfOpenSuccesses)
}

func (s *ConfigSuite) TestRejectedErrorUnwraps() {
	e := &RejectedError{Err: ErrOpen, Name: "x", State: StateOpen}
	s.ErrorIs(e, ErrOpen)
	s.Equal("circuit breaker x is open: circuit breaker is open", e.Error())
}

type ClassifierSuite struct {
	suite.Suite
}

func TestClassifierSuite(t *testing.T) {
	suite.Run(t, new(ClassifierSuite))
}

func (s *ClassifierSuite) TestDefaultClassifier() {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancelExpired()
	errBoom := errors.New("boom")
	cases := []struct {
		ctx      context.Context
		err      error
		name     string
		expected Outcome
	}{
		{name: "canceled", ctx: canceled, err: context.Canceled, expected: OutcomeIgnored},
		{name: "deadline", ctx: expired, err: context.DeadlineExceeded, expected: OutcomeFailure},
		{name: "live nil", ctx: context.Background(), err: nil, expected: OutcomeSuccess},
		{name: "live err", ctx: context.Background(), err: errBoom, expected: OutcomeFailure},
		{name: "live deadline err", ctx: context.Background(), err: context.DeadlineExceeded, expected: OutcomeFailure},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.Equal(tc.expected, DefaultClassifier(tc.ctx, tc.err))
		})
	}
}
