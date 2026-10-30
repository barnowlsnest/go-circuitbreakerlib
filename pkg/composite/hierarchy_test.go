package composite

import (
	"testing"
	"time"

	"github.com/barnowlsnest/go-datalib/v5/pkg/tree"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

const testMaxBreadth = 8

type HierarchySuite struct {
	baseSuite
}

func TestHierarchySuite(t *testing.T) {
	suite.Run(t, new(HierarchySuite))
}

func (s *HierarchySuite) model() tree.HierarchyModel {
	return tree.HierarchyModel{
		tree.RootTag: {"payments"},
		"payments":   {"eu", "us"},
		"eu":         {"h1", "h2"},
		"us":         {"h3"},
	}
}

func (s *HierarchySuite) hierarchyOpts() []HierarchyOption {
	return []HierarchyOption{
		WithComposite(append(s.opts("", s.openChildren(1)), WithEvents(s.events))...),
		WithLeaf(s.childOpts()...),
	}
}

func (s *HierarchySuite) TestBuildsTree() {
	root, err := FromHierarchy(s.model(), testMaxBreadth, s.hierarchyOpts()...)
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
	root.RecordLease(l, breaker.OutcomeSuccess, 0, nil)
}

func (s *HierarchySuite) TestNamesFromNodes() {
	single := tree.HierarchyModel{tree.RootTag: {"payments"}, "payments": {"h1"}}
	root, err := FromHierarchy(single, testMaxBreadth, s.hierarchyOpts()...)
	s.Require().NoError(err)
	s.Equal("payments", root.Name())
	s.Equal([]string{"h1"}, root.Children())
}

func (s *HierarchySuite) TestLeafEventsDefaultToComposite() {
	single := tree.HierarchyModel{tree.RootTag: {"payments"}, "payments": {"h1"}}
	root, err := FromHierarchy(single, testMaxBreadth, s.hierarchyOpts()...)
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
	tests := []struct {
		expectedErr error
		model       tree.HierarchyModel
		name        string
		opts        []HierarchyOption
		maxBreadth  int
	}{
		{name: "cycle", model: cyclic, maxBreadth: testMaxBreadth, expectedErr: ErrInvalidConfig},
		{name: "no root", model: tree.HierarchyModel{"a": {"b"}}, maxBreadth: testMaxBreadth, expectedErr: ErrInvalidConfig},
		{name: "breadth exceeded", model: s.model(), maxBreadth: 1, expectedErr: ErrInvalidConfig},
		{name: "too deep", model: deep, maxBreadth: testMaxBreadth, opts: s.hierarchyOpts(), expectedErr: ErrTooDeep},
		{
			name:       "bad leaf option",
			model:      s.model(),
			maxBreadth: testMaxBreadth,
			opts: []HierarchyOption{
				WithComposite(s.opts("", s.openChildren(1))...),
				WithLeaf(breaker.WithOpenDuration(-time.Second)),
			},
			expectedErr: breaker.ErrNegativeDuration,
		},
		{
			name:       "bad composite option",
			model:      s.model(),
			maxBreadth: testMaxBreadth,
			opts: []HierarchyOption{
				WithComposite(WithParent(breaker.WithPolicies(breaker.ConsecutiveFailures{Count: 1}))),
				WithLeaf(s.childOpts()...),
			},
			expectedErr: ErrInvalidConfig,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			root, err := FromHierarchy(tt.model, tt.maxBreadth, tt.opts...)
			s.ErrorIs(err, tt.expectedErr)
			s.Nil(root)
		})
	}
}
