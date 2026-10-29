package adapter

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/registry"
)

type OptionSuite struct {
	suite.Suite
	breaker  *breaker.Breaker
	registry *registry.Registry
}

func TestOptionSuite(t *testing.T) {
	suite.Run(t, new(OptionSuite))
}

func (s *OptionSuite) SetupTest() {
	b, err := breaker.New(breaker.WithName(testName))
	s.Require().NoError(err)
	s.breaker = b
	s.registry = testRegistry(s.T())
}

func (s *OptionSuite) TestExactlyOneSource() {
	tests := []struct {
		expectedErr error
		name        string
		opts        []Option
	}{
		{name: "none", opts: nil, expectedErr: ErrNoBreaker},
		{name: "both", opts: []Option{WithBreaker(s.breaker), WithRegistry(s.registry)}, expectedErr: ErrBothBreakerAndRegistry},
		{name: "nil breaker", opts: []Option{WithBreaker(nil)}, expectedErr: ErrInvalidOption},
		{name: "nil registry", opts: []Option{WithRegistry(nil)}, expectedErr: ErrInvalidOption},
		{name: "breaker only", opts: []Option{WithBreaker(s.breaker)}, expectedErr: nil},
		{name: "registry only", opts: []Option{WithRegistry(s.registry)}, expectedErr: nil},
		{name: "wrong protocol", opts: []Option{WithBreaker(s.breaker), tcpOnly()}, expectedErr: ErrInvalidOption},
		{name: "nil key", opts: []Option{WithBreaker(s.breaker), WithHTTPKey(nil)}, expectedErr: ErrInvalidOption},
		{name: "nil classifier", opts: []Option{WithBreaker(s.breaker), WithHTTPClassifier(nil)}, expectedErr: ErrInvalidOption},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			_, err := NewHTTPRoundTripper(nil, tt.opts...)
			s.ErrorIs(err, tt.expectedErr)
		})
	}
}

// tcpOnly is a placeholder option tagged for another protocol.
func tcpOnly() Option {
	return Option{proto: protoTCP, apply: func(*settings) error { return nil }}
}

func (s *OptionSuite) TestResolve() {
	tests := []struct {
		expectedErr  error
		name         string
		key          string
		expectedName string
		opts         []Option
	}{
		{name: "fixed breaker ignores key", opts: []Option{WithBreaker(s.breaker)}, key: "other", expectedName: testName},
		{name: "registry uses key", opts: []Option{WithRegistry(s.registry)}, key: "host-a", expectedName: "host-a"},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			st, err := newSettings(protoHTTP, tt.opts)
			s.Require().NoError(err)
			b, err := st.resolve(tt.key)
			s.Require().ErrorIs(err, tt.expectedErr)
			s.Equal(tt.expectedName, b.Name())
		})
	}
}

func (s *OptionSuite) TestResolveRegistryError() {
	bad := testFailingRegistry(s.T())
	st, err := newSettings(protoHTTP, []Option{WithRegistry(bad)})
	s.Require().NoError(err)
	b, err := st.resolve("x")
	s.Nil(b)
	s.ErrorIs(err, ErrRegistry)
	s.ErrorIs(err, breaker.ErrWindowSize)
}
