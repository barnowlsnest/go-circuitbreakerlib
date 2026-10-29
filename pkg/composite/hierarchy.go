package composite

import (
	"fmt"
	"slices"

	"github.com/barnowlsnest/go-datalib/v5/pkg/tree"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

// FromHierarchy builds nested composites from a tree.HierarchyModel. A
// node with children becomes a *Composite, a node without becomes a
// *breaker.Breaker leaf. compositeCfg and leafCfg receive the chain of node
// values from the root, for example ["payments", "eu-west", "10.0.1.5:443"];
// an empty Name in what they return becomes the node value, and a nil leaf
// Events becomes its composite's Events. Model errors from tree.Hierarchy
// (missing root, cycle, breadth over maxBreadth) are wrapped in
// ErrInvalidConfig; a leaf deeper than MaxDepth returns ErrTooDeep.
func FromHierarchy(
	model tree.HierarchyModel,
	maxBreadth int,
	compositeCfg func(path []string) Config,
	leafCfg func(path []string) breaker.Config,
) (*Composite, error) {
	if compositeCfg == nil || leafCfg == nil {
		return nil, fmt.Errorf("%w: nil config function", ErrInvalidConfig)
	}
	var next uint64
	root, err := tree.Hierarchy(model, maxBreadth, func() uint64 {
		next++
		return next
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	return buildComposite(root, nil, compositeCfg, leafCfg)
}

// buildComposite builds n and its subtree bottom-up: children first, then
// attached to the new composite, so AttachChild enforces MaxDepth.
func buildComposite(
	n *tree.Node[string],
	parentPath []string,
	compositeCfg func(path []string) Config,
	leafCfg func(path []string) breaker.Config,
) (*Composite, error) {
	path := slices.Concat(parentPath, []string{n.Val()})
	cfg := compositeCfg(slices.Clone(path))
	if cfg.Name == "" {
		cfg.Name = n.Val()
	}
	c, err := New(cfg)
	if err != nil {
		return nil, err
	}
	for _, child := range n.ChildrenIter() {
		if !child.HasChildren() {
			if _, err := c.attachLeaf(child.Val(), leafCfg(slices.Concat(path, []string{child.Val()}))); err != nil {
				return nil, err
			}
			continue
		}
		sub, err := buildComposite(child, path, compositeCfg, leafCfg)
		if err != nil {
			return nil, err
		}
		if err := c.AttachChild(sub); err != nil {
			return nil, err
		}
	}
	return c, nil
}
