package registry

import (
	"math/rand/v2"
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
)

type RegistrySuite struct {
	suite.Suite
	clock   *testClock
	factory *MockFactory
	onEvict *MockOnEvict
	events  *MockEvents
}

func TestRegistrySuite(t *testing.T) {
	suite.Run(t, new(RegistrySuite))
}

func (s *RegistrySuite) SetupTest() {
	s.clock = newTestClock()
	s.factory = &MockFactory{}
	s.onEvict = &MockOnEvict{}
	s.events = &MockEvents{}
}

func (s *RegistrySuite) TearDownTest() {
	s.factory.AssertExpectations(s.T())
	s.onEvict.AssertExpectations(s.T())
	s.events.AssertExpectations(s.T())
}

// config is a valid breaker config that trips on the first failure.
func config() breaker.Config {
	return breaker.Config{Policies: []breaker.Policy{breaker.ConsecutiveFailures{Count: 1}}}
}

func (s *RegistrySuite) newRegistry(opts ...Option) *Registry {
	return New(s.factory.Make, append([]Option{WithClock(s.clock.Now), WithOnEvict(s.onEvict.Evict)}, opts...)...)
}

// mustGet creates key and advances the clock so the next entry is younger.
func (s *RegistrySuite) mustGet(r *Registry, key string) *breaker.Breaker {
	s.T().Helper()
	s.factory.On("Make", key).Return(config()).Once()
	b, err := r.Get(key)
	s.Require().NoError(err)
	s.clock.advance(time.Second)
	return b
}

func (s *RegistrySuite) TestGetCreatesOnce() {
	r := s.newRegistry()
	s.factory.On("Make", "a").Return(config()).Once()
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

func (s *RegistrySuite) TestFactoryNameKept() {
	r := s.newRegistry()
	cfg := config()
	cfg.Name = "custom"
	s.factory.On("Make", "a").Return(cfg).Once()
	b, err := r.Get("a")
	s.Require().NoError(err)
	s.Equal("custom", b.Name())
}

func (s *RegistrySuite) TestFactoryErrorNotMemoized() {
	r := s.newRegistry()
	bad := config()
	bad.OpenDuration = -time.Second
	s.factory.On("Make", "a").Return(bad).Times(2)
	for range 2 {
		b, err := r.Get("a")
		s.ErrorIs(err, breaker.ErrNegativeDuration)
		s.Nil(b)
	}
	s.Equal(0, r.Len())
	_, ok := r.Lookup("a")
	s.False(ok)
}

func (s *RegistrySuite) TestNilFactoryPanics() {
	s.PanicsWithError(ErrNilFactory.Error(), func() { New(nil) })
}

func (s *RegistrySuite) TestInvalidOptionPanics() {
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
			s.Panics(func() { New(s.factory.Make, tt.opt) })
		})
	}
}

func (s *RegistrySuite) TestEventsInjectedWhenNil() {
	own := &MockEvents{}
	tests := []struct {
		cfgEvents      breaker.Events
		expectedEvents *MockEvents
		name           string
	}{
		{name: "nil config events get registry events", cfgEvents: nil, expectedEvents: s.events},
		{name: "config events are kept", cfgEvents: own, expectedEvents: own},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			r := s.newRegistry(WithEvents(s.events))
			cfg := config()
			cfg.Events = tt.cfgEvents
			s.factory.On("Make", "a").Return(cfg).Once()
			tt.expectedEvents.On("OnCall", mock.Anything).Once()
			tt.expectedEvents.On("OnStateChange", mock.Anything).Once()
			b, err := r.Get("a")
			s.Require().NoError(err)
			p, err := b.Allow()
			s.Require().NoError(err)
			b.Record(p, breaker.OutcomeFailure, 0)
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
	r := s.newRegistry(WithMaxEntries(3), WithEvictSamples(3))
	b := s.mustGet(r, "b")
	c := s.mustGet(r, "c")
	a := s.mustGet(r, "a")
	a.ForceOpen()
	s.onEvict.On("Evict", "b", b).Once()
	d := s.mustGet(r, "d")
	s.Equal(3, r.Len())
	_, ok := r.Lookup("b")
	s.False(ok)
	for key, expected := range map[string]*breaker.Breaker{"a": a, "c": c, "d": d} {
		actual, ok := r.Lookup(key)
		s.True(ok, key)
		s.Same(expected, actual)
	}
}

func (s *RegistrySuite) TestEvictOpenOnlyIfAllSamplesOpen() {
	r := s.newRegistry(WithMaxEntries(2), WithEvictSamples(2))
	a := s.mustGet(r, "a")
	b := s.mustGet(r, "b")
	a.ForceOpen()
	b.ForceOpen()
	s.onEvict.On("Evict", "a", a).Once()
	s.mustGet(r, "c")
	_, ok := r.Lookup("a")
	s.False(ok)
	_, ok = r.Lookup("b")
	s.True(ok)
}

func (s *RegistrySuite) TestSampledEvictionUsesRand() {
	keys := [testEntries]string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	r := s.newRegistry(
		WithMaxEntries(testEntries),
		WithEvictSamples(testSamples),
		WithRand(rand.New(rand.NewPCG(testSeed1, testSeed2))),
	)
	var breakers [testEntries]*breaker.Breaker
	for i, key := range keys {
		breakers[i] = s.mustGet(r, key)
	}
	// The registry draws one index per sample from the same generator, so
	// the oldest of these draws is the victim.
	g := rand.New(rand.NewPCG(testSeed1, testSeed2))
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
	for _, key := range []string{"a", "b", "c", "d"} {
		s.mustGet(r, key)
	}
	s.Equal(4, r.Len())
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
	r := s.newRegistry(WithAccessGranularity(time.Second))
	s.factory.On("Make", "a").Return(config()).Once()
	_, err := r.Get("a")
	s.Require().NoError(err)
	created := r.lastAccess("a")
	s.Equal(s.clock.Now().UnixNano(), created)

	s.clock.advance(500 * time.Millisecond)
	_, err = r.Get("a")
	s.Require().NoError(err)
	actual := r.lastAccess("a")
	s.Equal(created, actual, "access inside the granularity window is not stored")

	s.clock.advance(600 * time.Millisecond)
	_, err = r.Get("a")
	s.Require().NoError(err)
	actual = r.lastAccess("a")
	s.Equal(s.clock.Now().UnixNano(), actual, "access past the window is stored")
}
