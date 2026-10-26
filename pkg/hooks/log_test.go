package hooks

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"
	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

const (
	testName       = "dep"
	testRejections = 10_000
	testElapsed    = 1500 * time.Millisecond
	testAttempt    = 2
	testEpoch      = 7
	testTotal      = 10
	testFailures   = 6
)

var errBoom = errors.New("boom")

type LogEventsSuite struct {
	suite.Suite
	clock *testClock
	buf   *bytes.Buffer
}

func TestLogEventsSuite(t *testing.T) {
	suite.Run(t, new(LogEventsSuite))
}

func (s *LogEventsSuite) SetupTest() {
	s.clock = newTestClock()
	s.buf = &bytes.Buffer{}
}

func (s *LogEventsSuite) logger(level logger.Level) *logger.Logger {
	return logger.New(logger.Config{
		Level:  level,
		Format: logger.JSONFormat,
		Output: s.buf,
	})
}

func (s *LogEventsSuite) mustNew(level logger.Level, opts ...Option) *LogEvents {
	s.T().Helper()
	ev, err := NewLogEvents(s.logger(level), append([]Option{WithClock(s.clock.Now)}, opts...)...)
	s.Require().NoError(err)
	return ev
}

// lines decodes every JSON line written so far.
func (s *LogEventsSuite) lines() []map[string]any {
	s.T().Helper()
	var out []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(s.buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		s.Require().NoError(json.Unmarshal([]byte(line), &m), line)
		out = append(out, m)
	}
	return out
}

func (s *LogEventsSuite) stateChange(from, to breaker.State) breaker.StateChangeEvent {
	return breaker.StateChangeEvent{
		At:      s.clock.Now(),
		Name:    testName,
		Reason:  breaker.ReasonTimeout,
		Counts:  breaker.Counts{Total: testTotal, Failures: testFailures},
		Attempt: testAttempt,
		Epoch:   testEpoch,
		From:    from,
		To:      to,
	}
}

func (s *LogEventsSuite) reject() breaker.RejectEvent {
	return breaker.RejectEvent{At: s.clock.Now(), Err: breaker.ErrOpen, Name: testName, State: breaker.StateOpen}
}

func (s *LogEventsSuite) call(outcome breaker.Outcome, err error, slow bool) breaker.CallEvent {
	return breaker.CallEvent{
		At:      s.clock.Now(),
		Err:     err,
		Name:    testName,
		Elapsed: testElapsed,
		State:   breaker.StateClosed,
		Outcome: outcome,
		Slow:    slow,
	}
}

func (s *LogEventsSuite) TestNilLoggerErrors() {
	ev, err := NewLogEvents(nil)
	s.Require().ErrorIs(err, ErrNilLogger)
	s.Nil(ev)
}

func (s *LogEventsSuite) TestInvalidOptionErrors() {
	tests := []struct {
		opt  Option
		name string
	}{
		{name: "reject every zero", opt: WithRejectEvery(0)},
		{name: "reject every negative", opt: WithRejectEvery(-time.Second)},
		{name: "failure every zero", opt: WithFailureEvery(0)},
		{name: "nil clock", opt: WithClock(nil)},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			_, err := NewLogEvents(s.logger(logger.InfoLevel), tt.opt)
			s.Require().ErrorIs(err, ErrInvalidOption)
		})
	}
}

func (s *LogEventsSuite) TestStateChangeIntoOpenIsError() {
	ev := s.mustNew(logger.InfoLevel)
	ev.OnStateChange(s.stateChange(breaker.StateClosed, breaker.StateOpen))
	lines := s.lines()
	s.Require().Len(lines, 1)
	line := lines[0]
	s.Equal("ERROR", line["level"])
	s.Equal(testName, line[FieldBreaker])
	s.Equal(breaker.StateClosed.String(), line[FieldFrom])
	s.Equal(breaker.StateOpen.String(), line[FieldTo])
	s.Equal(string(breaker.ReasonTimeout), line[FieldReason])
	s.InDelta(testAttempt, line[FieldAttempt], 0)
	s.InDelta(testEpoch, line[FieldEpoch], 0)
	s.InDelta(testTotal, line[FieldTotal], 0)
	s.InDelta(testFailures, line[FieldFailures], 0)
}

func (s *LogEventsSuite) TestOtherTransitionsAreInfo() {
	tests := []struct {
		name string
		from breaker.State
		to   breaker.State
	}{
		{name: "open to half-open", from: breaker.StateOpen, to: breaker.StateHalfOpen},
		{name: "half-open to closed", from: breaker.StateHalfOpen, to: breaker.StateClosed},
		{name: "half-open to open", from: breaker.StateHalfOpen, to: breaker.StateOpen},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.buf.Reset()
			ev := s.mustNew(logger.InfoLevel)
			ev.OnStateChange(s.stateChange(tt.from, tt.to))
			lines := s.lines()
			s.Require().Len(lines, 1)
			expected := "INFO"
			if tt.to == breaker.StateOpen {
				expected = "ERROR"
			}
			s.Equal(expected, lines[0]["level"])
		})
	}
}

func (s *LogEventsSuite) TestFailureIsWarnWithErrorAndElapsed() {
	ev := s.mustNew(logger.InfoLevel)
	ev.OnCall(s.call(breaker.OutcomeFailure, errBoom, false))
	lines := s.lines()
	s.Require().Len(lines, 1)
	line := lines[0]
	s.Equal("WARN", line["level"])
	s.Equal(errBoom.Error(), line[FieldError])
	s.Equal(testElapsed.String(), line[FieldElapsed])
	s.Equal(breaker.OutcomeFailure.String(), line[FieldOutcome])
	s.Equal(false, line[FieldSlow])
	s.InDelta(0, line[FieldSuppressed], 0)
}

func (s *LogEventsSuite) TestSlowSuccessIsWarn() {
	ev := s.mustNew(logger.InfoLevel)
	ev.OnCall(s.call(breaker.OutcomeSuccess, nil, true))
	lines := s.lines()
	s.Require().Len(lines, 1)
	s.Equal("WARN", lines[0]["level"])
	s.Equal(true, lines[0][FieldSlow])
	s.NotContains(lines[0], FieldError)
}

func (s *LogEventsSuite) TestSuccessIsDebugAndFilteredByLevel() {
	ev := s.mustNew(logger.InfoLevel)
	ev.OnCall(s.call(breaker.OutcomeSuccess, nil, false))
	s.Zero(s.buf.Len())

	ev = s.mustNew(logger.DebugLevel)
	ev.OnCall(s.call(breaker.OutcomeSuccess, nil, false))
	lines := s.lines()
	s.Require().Len(lines, 1)
	s.Equal("DEBUG", lines[0]["level"])
	s.Equal(breaker.OutcomeSuccess.String(), lines[0][FieldOutcome])
}

func (s *LogEventsSuite) TestMinLevelOverridesLoggerLevel() {
	ev := s.mustNew(logger.DebugLevel, WithMinLevel(logger.ErrorLevel))
	ev.OnCall(s.call(breaker.OutcomeFailure, errBoom, false))
	ev.OnReject(s.reject())
	ev.OnStateChange(s.stateChange(breaker.StateOpen, breaker.StateHalfOpen))
	s.Zero(s.buf.Len())
	ev.OnStateChange(s.stateChange(breaker.StateClosed, breaker.StateOpen))
	s.Len(s.lines(), 1)
}

func (s *LogEventsSuite) TestRejectRateLimited() {
	ev := s.mustNew(logger.InfoLevel)
	for range testRejections {
		ev.OnReject(s.reject())
	}
	lines := s.lines()
	s.Require().Len(lines, 1)
	s.Equal("WARN", lines[0]["level"])
	s.Equal(breaker.StateOpen.String(), lines[0][FieldState])
	s.Equal(breaker.ErrOpen.Error(), lines[0][FieldError])
	s.InDelta(0, lines[0][FieldSuppressed], 0)

	s.clock.advance(time.Second)
	ev.OnReject(s.reject())
	lines = s.lines()
	s.Require().Len(lines, 2)
	s.InDelta(testRejections-1, lines[1][FieldSuppressed], 0)
}

func (s *LogEventsSuite) TestRejectEveryOption() {
	ev := s.mustNew(logger.InfoLevel, WithRejectEvery(time.Minute))
	ev.OnReject(s.reject())
	s.clock.advance(time.Second)
	ev.OnReject(s.reject())
	s.Len(s.lines(), 1)
	s.clock.advance(time.Minute)
	ev.OnReject(s.reject())
	s.Len(s.lines(), 2)
}

func (s *LogEventsSuite) TestFailureRateLimitedSeparately() {
	ev := s.mustNew(logger.InfoLevel)
	for range testRejections {
		ev.OnReject(s.reject())
	}
	s.Require().Len(s.lines(), 1)
	ev.OnCall(s.call(breaker.OutcomeFailure, errBoom, false))
	ev.OnCall(s.call(breaker.OutcomeFailure, errBoom, false))
	lines := s.lines()
	s.Require().Len(lines, 2)
	s.Equal(breaker.OutcomeFailure.String(), lines[1][FieldOutcome])

	s.clock.advance(time.Second)
	ev.OnCall(s.call(breaker.OutcomeFailure, errBoom, false))
	lines = s.lines()
	s.Require().Len(lines, 3)
	s.InDelta(1, lines[2][FieldSuppressed], 0)
}

func (s *LogEventsSuite) TestLimitersArePerBreaker() {
	ev := s.mustNew(logger.InfoLevel)
	ev.OnReject(s.reject())
	other := s.reject()
	other.Name = "other"
	ev.OnReject(other)
	lines := s.lines()
	s.Require().Len(lines, 2)
	s.Equal("other", lines[1][FieldBreaker])
}

func (s *LogEventsSuite) TestForgetDropsState() {
	ev := s.mustNew(logger.InfoLevel)
	ev.OnReject(s.reject())
	ev.OnReject(s.reject())
	ev.Forget(testName)
	ev.OnReject(s.reject())
	lines := s.lines()
	s.Require().Len(lines, 2)
	s.InDelta(0, lines[1][FieldSuppressed], 0)
}

func (s *LogEventsSuite) TestStateChangesAreNeverSuppressed() {
	ev := s.mustNew(logger.InfoLevel)
	for range testRejections {
		ev.OnStateChange(s.stateChange(breaker.StateOpen, breaker.StateHalfOpen))
	}
	s.Len(s.lines(), testRejections)
}

func (s *LogEventsSuite) TestHookPanicIsError() {
	ev := s.mustNew(logger.InfoLevel)
	ev.OnHookPanic("policy", errBoom)
	lines := s.lines()
	s.Require().Len(lines, 1)
	s.Equal("ERROR", lines[0]["level"])
	s.Equal("policy", lines[0][FieldHook])
	s.Equal(errBoom.Error(), lines[0][FieldError])
}
