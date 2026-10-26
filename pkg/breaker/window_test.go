package breaker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

const (
	testWindowSize    = 3
	testTimeWindow    = 10 * time.Second
	testTimeBuckets   = 10
	testDefaultBucket = 10
)

type WindowSuite struct {
	suite.Suite
	base time.Time
}

func TestWindowSuite(t *testing.T) {
	suite.Run(t, new(WindowSuite))
}

func (s *WindowSuite) SetupTest() {
	s.base = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
}

func (s *WindowSuite) newCount() window {
	return SlidingCountWindow{Size: testWindowSize}.newWindow()
}

func (s *WindowSuite) newTime() window {
	return SlidingTimeWindow{Size: testTimeWindow, Buckets: testTimeBuckets}.newWindow()
}

func (s *WindowSuite) TestCountWindowEvictsOldest() {
	w := s.newCount()
	for _, o := range []Outcome{OutcomeFailure, OutcomeFailure, OutcomeSuccess, OutcomeSuccess} {
		w.record(s.base, o, false)
	}
	c := w.counts(s.base)
	s.Equal(uint32(1), c.Failures)
	s.Equal(uint32(2), c.Successes)
	s.Equal(uint32(testWindowSize), c.Total)
}

func (s *WindowSuite) TestConsecutiveCounters() {
	cases := []struct {
		name                 string
		outcomes             []Outcome
		expectedFailures     uint32
		expectedConsecutiveF uint32
		expectedConsecutiveS uint32
	}{
		{name: "failures then success", outcomes: []Outcome{OutcomeFailure, OutcomeFailure, OutcomeSuccess},
			expectedFailures: 2, expectedConsecutiveF: 0, expectedConsecutiveS: 1},
		{name: "two failures", outcomes: []Outcome{OutcomeFailure, OutcomeFailure},
			expectedFailures: 2, expectedConsecutiveF: 2, expectedConsecutiveS: 0},
	}
	for _, tc := range cases {
		for name, mk := range map[string]func() window{"count": s.newCount, "time": s.newTime} {
			s.Run(tc.name+"/"+name, func() {
				w := mk()
				for _, o := range tc.outcomes {
					w.record(s.base, o, false)
				}
				c := w.counts(s.base)
				s.Equal(tc.expectedFailures, c.Failures)
				s.Equal(tc.expectedConsecutiveF, c.ConsecutiveFailures)
				s.Equal(tc.expectedConsecutiveS, c.ConsecutiveSuccesses)
			})
		}
	}
}

func (s *WindowSuite) TestTimeWindowExpiresWithoutRecord() {
	w := s.newTime()
	w.record(s.base, OutcomeFailure, false)
	s.Equal(uint32(1), w.counts(s.base).Failures)
	c := w.counts(s.base.Add(testTimeWindow + time.Second))
	s.Equal(uint32(0), c.Total)
	s.Equal(uint32(0), c.Failures)
}

func (s *WindowSuite) TestTimeWindowPartialExpiry() {
	w := s.newTime()
	w.record(s.base, OutcomeFailure, false)
	w.record(s.base.Add(5*time.Second), OutcomeFailure, false)
	c := w.counts(s.base.Add(testTimeWindow + time.Millisecond))
	s.Equal(uint32(1), c.Failures)
	s.Equal(uint32(1), c.Total)
}

func (s *WindowSuite) TestTimeWindowRecordAdvancesAndClears() {
	w := s.newTime()
	w.record(s.base, OutcomeFailure, false)
	w.record(s.base.Add(testTimeWindow+time.Second), OutcomeSuccess, false)
	c := w.counts(s.base.Add(testTimeWindow + time.Second))
	s.Equal(uint32(0), c.Failures)
	s.Equal(uint32(1), c.Successes)
	s.Equal(uint32(1), c.Total)
}

func (s *WindowSuite) TestSlowCounted() {
	for name, mk := range map[string]func() window{"count": s.newCount, "time": s.newTime} {
		s.Run(name, func() {
			w := mk()
			w.record(s.base, OutcomeSuccess, true)
			c := w.counts(s.base)
			s.Equal(uint32(1), c.Slow)
			s.Equal(uint32(1), c.Successes)
		})
	}
}

func (s *WindowSuite) TestReset() {
	for name, mk := range map[string]func() window{"count": s.newCount, "time": s.newTime} {
		s.Run(name, func() {
			w := mk()
			w.record(s.base, OutcomeFailure, true)
			w.reset()
			s.Equal(Counts{}, w.counts(s.base))
		})
	}
}

func (s *WindowSuite) TestValidate() {
	cases := []struct {
		spec        WindowSpec
		name        string
		expectedErr bool
	}{
		{name: "count zero", spec: SlidingCountWindow{Size: 0}, expectedErr: true},
		{name: "count negative", spec: SlidingCountWindow{Size: -1}, expectedErr: true},
		{name: "count ok", spec: SlidingCountWindow{Size: 1}, expectedErr: false},
		{name: "time zero", spec: SlidingTimeWindow{Size: 0}, expectedErr: true},
		{name: "time negative buckets", spec: SlidingTimeWindow{Size: time.Second, Buckets: -1}, expectedErr: true},
		{name: "time default buckets", spec: SlidingTimeWindow{Size: time.Second, Buckets: 0}, expectedErr: false},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			err := tc.spec.validate()
			if tc.expectedErr {
				s.Error(err)
				return
			}
			s.NoError(err)
		})
	}
}

func (s *WindowSuite) TestTimeWindowDefaultBuckets() {
	w, ok := SlidingTimeWindow{Size: time.Second}.newWindow().(*timeWindow)
	s.True(ok)
	s.Len(w.buckets, testDefaultBucket)
}
