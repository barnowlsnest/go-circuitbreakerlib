package registry

import (
	"math/rand/v2"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

const (
	testGoroutines = 100
	testEntries    = 10
	testSamples    = 3
	testSeed1      = 11
	testSeed2      = 22
	testRaceKeys   = 500
	testRaceMax    = 20
	testRegistries = 2

	testGranularity  = time.Second
	testInsideWindow = 500 * time.Millisecond
	testPastWindow   = 600 * time.Millisecond
)

type RegistrySuite struct {
	suite.Suite
	clock   *testClock
	onEvict *MockOnEvict
	events  *MockEvents
}

func TestRegistrySuite(t *testing.T) {
	suite.Run(t, new(RegistrySuite))
}

func (s *RegistrySuite) SetupTest() {
	s.clock = newTestClock()
	s.onEvict = &MockOnEvict{}
	s.events = &MockEvents{}
}

func (s *RegistrySuite) TearDownTest() {
	s.onEvict.AssertExpectations(s.T())
	s.events.AssertExpectations(s.T())
}

func newTestRand() *rand.Rand {
	return rand.New(rand.NewPCG(testSeed1, testSeed2))
}

func tripOnFirstFailure() Option {
	return WithBreaker(breaker.WithPolicies(breaker.ConsecutiveFailures{Count: 1}))
}

func (s *RegistrySuite) newRegistry(opts ...Option) *Registry {
	s.T().Helper()
	base := []Option{tripOnFirstFailure(), WithClock(s.clock.Now), WithOnEvict(s.onEvict.Evict)}
	r, err := New(append(base, opts...)...)
	s.Require().NoError(err)
	return r
}

// mustGet creates key and advances the clock so the next entry is younger.
func (s *RegistrySuite) mustGet(r *Registry, key string) *breaker.Breaker {
	s.T().Helper()
	b, err := r.Get(key)
	s.Require().NoError(err)
	s.clock.advance(time.Second)
	return b
}

func (s *RegistrySuite) TestGetCreatesOnce() {
	r := s.newRegistry()
	var (
		wg     sync.WaitGroup
		actual [testGoroutines]*breaker.Breaker
		errs   [testGoroutines]error
	)
	for i := range testGoroutines {
		wg.Go(func() { actual[i], errs[i] = r.Get("a") })
	}
	wg.Wait()
	for i := range testGoroutines {
		s.Require().NoError(errs[i])
		s.Same(actual[0], actual[i])
	}
	s.Equal(1, r.Len())
	s.Equal("a", actual[0].Name())
}

func (s *RegistrySuite) TestNameIsKey() {
	r := s.newRegistry(WithBreaker(breaker.WithName("ignored"), breaker.WithPolicies(breaker.ConsecutiveFailures{Count: 1})))
	b, err := r.Get("a")
	s.Require().NoError(err)
	s.Equal("a", b.Name())
}

func (s *RegistrySuite) TestBreakerErrorNotMemoized() {
	r := s.newRegistry(WithBreaker(breaker.WithWindow(breaker.SlidingCountWindow{Size: 0})))
	for range 2 {
		b, err := r.Get("a")
		s.ErrorIs(err, breaker.ErrInvalidConfig)
		s.Nil(b)
	}
	s.Equal(0, r.Len())
	_, ok := r.Lookup("a")
	s.False(ok)
}

func (s *RegistrySuite) TestInvalidOption() {
	tests := []struct {
		opt  Option
		name string
	}{
		{name: "negative max entries", opt: WithMaxEntries(-1)},
		{name: "zero evict samples", opt: WithEvictSamples(0)},
		{name: "negative granularity", opt: WithAccessGranularity(-time.Second)},
		{name: "nil clock", opt: WithClock(nil)},
		{name: "nil rand", opt: WithRand(nil)},
		{name: "nil events", opt: WithEvents(nil)},
		{name: "nil on evict", opt: WithOnEvict(nil)},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			r, err := New(tt.opt)
			s.ErrorIs(err, ErrInvalidOption)
			s.Nil(r)
		})
	}
}

func (s *RegistrySuite) TestEventsInjectedWhenNil() {
	own := &MockEvents{}
	tests := []struct {
		expectedEvents *MockEvents
		name           string
		breakerOpts    []breaker.Option
	}{
		{name: "nil breaker events get registry events", expectedEvents: s.events},
		{name: "breaker events are kept", breakerOpts: []breaker.Option{breaker.WithEvents(own)}, expectedEvents: own},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			opts := append([]breaker.Option{breaker.WithPolicies(breaker.ConsecutiveFailures{Count: 1})}, tt.breakerOpts...)
			r := s.newRegistry(WithEvents(s.events), WithBreaker(opts...))
			tt.expectedEvents.On("OnCall", mock.Anything).Once()
			tt.expectedEvents.On("OnStateChange", mock.Anything).Once()
			b, err := r.Get("a")
			s.Require().NoError(err)
			p, err := b.Allow()
			s.Require().NoError(err)
			b.Record(p, breaker.OutcomeFailure, 0, nil)
			tt.expectedEvents.AssertExpectations(s.T())
		})
	}
}

func (s *RegistrySuite) TestLookup() {
	r := s.newRegistry()
	a := s.mustGet(r, "a")
	tests := []struct {
		expected   *breaker.Breaker
		name       string
		key        string
		expectedOK bool
	}{
		{name: "hit", key: "a", expected: a, expectedOK: true},
		{name: "miss", key: "b", expected: nil, expectedOK: false},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			b, ok := r.Lookup(tt.key)
			s.Equal(tt.expectedOK, ok)
			s.Same(tt.expected, b)
		})
	}
	s.Equal(1, r.Len())
}

func (s *RegistrySuite) TestEvictPrefersClosed() {
	tests := []struct {
		name           string
		expectedVictim string
		keys           []string
		open           []string
	}{
		{name: "oldest closed over older open", keys: []string{"b", "c", "a"}, open: []string{"a"}, expectedVictim: "b"},
		{name: "oldest open when all open", keys: []string{"a", "b"}, open: []string{"a", "b"}, expectedVictim: "a"},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			r := s.newRegistry(WithMaxEntries(len(tt.keys)), WithEvictSamples(len(tt.keys)))
			breakers := make(map[string]*breaker.Breaker, len(tt.keys)+1)
			for _, key := range tt.keys {
				breakers[key] = s.mustGet(r, key)
			}
			for _, key := range tt.open {
				breakers[key].ForceOpen()
			}
			s.onEvict.On("Evict", tt.expectedVictim, breakers[tt.expectedVictim]).Once()
			breakers["new"] = s.mustGet(r, "new")
			delete(breakers, tt.expectedVictim)
			s.Equal(len(tt.keys), r.Len())
			_, ok := r.Lookup(tt.expectedVictim)
			s.False(ok)
			for key, expected := range breakers {
				actual, ok := r.Lookup(key)
				s.True(ok, key)
				s.Same(expected, actual)
			}
		})
	}
}

func (s *RegistrySuite) TestSampledEvictionUsesRand() {
	keys := [testEntries]string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	r := s.newRegistry(
		WithMaxEntries(testEntries),
		WithEvictSamples(testSamples),
		WithRand(newTestRand()),
	)
	var breakers [testEntries]*breaker.Breaker
	for i, key := range keys {
		breakers[i] = s.mustGet(r, key)
	}
	// The registry draws one index per sample from the same generator, so
	// the oldest of these draws is the victim.
	g := newTestRand()
	victim := testEntries
	for range testSamples {
		if i := g.IntN(testEntries); i < victim {
			victim = i
		}
	}
	s.onEvict.On("Evict", keys[victim], breakers[victim]).Once()
	s.mustGet(r, "k")
	_, ok := r.Lookup(keys[victim])
	s.False(ok)
	s.Equal(testEntries, r.Len())
}

func (s *RegistrySuite) TestUnboundedNeverEvicts() {
	r := s.newRegistry()
	keys := []string{"a", "b", "c", "d"}
	for _, key := range keys {
		s.mustGet(r, key)
	}
	s.Equal(len(keys), r.Len())
}

func (s *RegistrySuite) TestRemoveIsSwapRemove() {
	r := s.newRegistry()
	s.mustGet(r, "a")
	b := s.mustGet(r, "b")
	s.mustGet(r, "c")
	s.onEvict.On("Evict", "b", b).Once()
	s.True(r.Remove("b"))
	s.False(r.Remove("b"))
	s.Equal(2, r.Len())
	var keys []string
	r.Range(func(key string, _ *breaker.Breaker) bool {
		keys = append(keys, key)
		return true
	})
	s.ElementsMatch([]string{"a", "c"}, keys)
	ia := r.index("a")
	ic := r.index("c")
	s.ElementsMatch([]int{0, 1}, []int{ia, ic})
}

func (s *RegistrySuite) TestRangeStops() {
	r := s.newRegistry()
	s.mustGet(r, "a")
	s.mustGet(r, "b")
	n := 0
	r.Range(func(string, *breaker.Breaker) bool {
		n++
		return false
	})
	s.Equal(1, n)
}

func (s *RegistrySuite) TestAccessGranularity() {
	start := s.clock.Now()
	r := s.newRegistry(WithAccessGranularity(testGranularity))
	_, err := r.Get("a")
	s.Require().NoError(err)
	stored := r.lastAccess("a")
	s.Equal(s.clock.Now().Sub(start).Nanoseconds(), stored)

	tests := []struct {
		name           string
		advance        time.Duration
		expectedStored bool
	}{
		{name: "inside the window is not stored", advance: testInsideWindow},
		{name: "past the window is stored", advance: testPastWindow, expectedStored: true},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.clock.advance(tt.advance)
			_, err := r.Get("a")
			s.Require().NoError(err)
			if tt.expectedStored {
				stored = s.clock.Now().Sub(start).Nanoseconds()
			}
			s.Equal(stored, r.lastAccess("a"))
		})
	}
}

func (s *RegistrySuite) TestDefaultRandIsPerRegistry() {
	s.onEvict.On("Evict", mock.Anything, mock.Anything).Maybe()
	var wg sync.WaitGroup
	for range testRegistries {
		r := s.newRegistry(WithMaxEntries(testRaceMax))
		wg.Go(func() {
			for i := range testRaceKeys {
				_, err := r.Get(strconv.Itoa(i))
				if err != nil {
					s.T().Errorf("Get: %v", err)
				}
			}
		})
	}
	wg.Wait()
}
