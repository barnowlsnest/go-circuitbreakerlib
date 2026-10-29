package composite

import (
	"testing"

	"github.com/barnowlsnest/go-datalib/v5/pkg/tree"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

const testMaxBreadth = 8

type HierarchySuite struct {
	cfg *MockPathConfig
	baseSuite
}

func TestHierarchySuite(t *testing.T) {
	suite.Run(t, new(HierarchySuite))
}

func (s *HierarchySuite) SetupTest() {
	s.baseSuite.SetupTest()
	s.cfg = &MockPathConfig{}
}

func (s *HierarchySuite) TearDownTest() {
	s.baseSuite.TearDownTest()
	s.cfg.AssertExpectations(s.T())
}

func (s *HierarchySuite) model() tree.HierarchyModel {
	return tree.HierarchyModel{
		tree.RootTag: {"payments"},
		"payments":   {"eu", "us"},
		"eu":         {"h1", "h2"},
		"us":         {"h3"},
	}
}

func (s *HierarchySuite) TestBuildsTree() {
	for _, path := range [][]string{{"payments"}, {"payments", "eu"}, {"payments", "us"}} {
		s.cfg.On("Composite", path).Return(*s.config("", s.openChildren(1))).Once()
	}
	for _, path := range [][]string{{"payments", "eu", "h1"}, {"payments", "eu", "h2"}, {"payments", "us", "h3"}} {
		s.cfg.On("Leaf", path).Return(s.template("")).Once()
	}
	root, err := FromHierarchy(s.model(), testMaxBreadth, s.cfg.Composite, s.cfg.Leaf)
	s.Require().NoError(err)
	s.Equal("payments", root.Name())
	s.ElementsMatch([]string{"eu", "us"}, root.Children())

	expectedLeaves := map[string][]string{"eu": {"h1", "h2"}, "us": {"h3"}}
	for region, leaves := range expectedLeaves {
		child, ok := root.Child(region)
		s.Require().True(ok, region)
		sub, ok := child.(*Composite)
		s.Require().True(ok, region)
		s.ElementsMatch(leaves, sub.Children())
		for _, id := range leaves {
			leaf, ok := sub.Child(id)
			s.Require().True(ok, id)
			_, ok = leaf.(*breaker.Breaker)
			s.True(ok, id)
		}
	}
	l, err := root.Pick()
	s.Require().NoError(err)
	s.Len(l.Path(), 3)
	root.RecordLease(l, breaker.OutcomeSuccess, 0)
}

func (s *HierarchySuite) TestNamesFromConfigWin() {
	cfg := s.config("custom-root", nil)
	s.cfg.On("Composite", []string{"payments"}).Return(*cfg).Once()
	leafCfg := s.template("custom-leaf")
	s.cfg.On("Leaf", []string{"payments", "h1"}).Return(leafCfg).Once()
	single := tree.HierarchyModel{tree.RootTag: {"payments"}, "payments": {"h1"}}
	root, err := FromHierarchy(single, testMaxBreadth, s.cfg.Composite, s.cfg.Leaf)
	s.Require().NoError(err)
	s.Equal("custom-root", root.Name())
	s.Equal([]string{"custom-leaf"}, root.Children())
}

func (s *HierarchySuite) TestLeafEventsDefaultToComposite() {
	s.cfg.On("Composite", mock.Anything).Return(*s.config("", s.openChildren(1))).Once()
	s.cfg.On("Leaf", mock.Anything).Return(s.template("")).Once()
	single := tree.HierarchyModel{tree.RootTag: {"payments"}, "payments": {"h1"}}
	root, err := FromHierarchy(single, testMaxBreadth, s.cfg.Composite, s.cfg.Leaf)
	s.Require().NoError(err)
	leaf, _ := root.Child("h1")
	s.trip(leaf.(*breaker.Breaker))
	s.events.AssertCalled(s.T(), "OnStateChange", mock.MatchedBy(func(ev breaker.StateChangeEvent) bool {
		return ev.Name == "h1" && ev.To == breaker.StateOpen
	}))
}

func (s *HierarchySuite) TestInvalidInput() {
	cyclic := tree.HierarchyModel{tree.RootTag: {"a"}, "a": {"b"}, "b": {"a"}}
	deep := tree.HierarchyModel{
		tree.RootTag: {"l1"},
		"l1":         {"l2"},
		"l2":         {"l3"},
		"l3":         {"l4"},
		"l4":         {"leaf"},
	}
	bad := s.template("")
	bad.OpenDuration = -1
	badComposite := s.config("", nil)
	badComposite.Parent.Policies = []breaker.Policy{breaker.ConsecutiveFailures{Count: 1}}
	tests := []struct {
		expectedErr  error
		model        tree.HierarchyModel
		compositeCfg func([]string) Config
		leafCfg      func([]string) breaker.Config
		composite    *Config
		leaf         *breaker.Config
		name         string
		maxBreadth   int
	}{
		{name: "cycle", model: cyclic, maxBreadth: testMaxBreadth, expectedErr: ErrInvalidConfig},
		{name: "no root", model: tree.HierarchyModel{"a": {"b"}}, maxBreadth: testMaxBreadth, expectedErr: ErrInvalidConfig},
		{name: "breadth exceeded", model: s.model(), maxBreadth: 1, expectedErr: ErrInvalidConfig},
		{name: "too deep", model: deep, maxBreadth: testMaxBreadth, expectedErr: ErrTooDeep},
		{
			name:        "nil composite config",
			model:       s.model(),
			maxBreadth:  testMaxBreadth,
			leafCfg:     s.cfg.Leaf,
			expectedErr: ErrInvalidConfig,
		},
		{
			name:         "nil leaf config",
			model:        s.model(),
			maxBreadth:   testMaxBreadth,
			compositeCfg: s.cfg.Composite,
			expectedErr:  ErrInvalidConfig,
		},
		{name: "bad leaf config", model: s.model(), maxBreadth: testMaxBreadth, leaf: &bad, expectedErr: breaker.ErrNegativeDuration},
		{name: "bad composite config", model: s.model(), maxBreadth: testMaxBreadth, composite: badComposite, expectedErr: ErrInvalidConfig},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.cfg = &MockPathConfig{}
			compositeCfg, leafCfg := tt.compositeCfg, tt.leafCfg
			if compositeCfg == nil && leafCfg == nil {
				compositeCfg, leafCfg = s.cfg.Composite, s.cfg.Leaf
				composite := *s.config("", s.openChildren(1))
				if tt.composite != nil {
					composite = *tt.composite
				}
				leaf := s.template("")
				if tt.leaf != nil {
					leaf = *tt.leaf
				}
				s.cfg.On("Composite", mock.Anything).Return(composite).Maybe()
				s.cfg.On("Leaf", mock.Anything).Return(leaf).Maybe()
			}
			root, err := FromHierarchy(tt.model, tt.maxBreadth, compositeCfg, leafCfg)
			s.ErrorIs(err, tt.expectedErr)
			s.Nil(root)
		})
	}
}
