package composite

import (
	"fmt"

	"github.com/barnowlsnest/go-datalib/v5/pkg/tree"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

// HierarchyOption configures FromHierarchy.
type HierarchyOption func(*hierarchyOptions) error

type hierarchyOptions struct {
	compositeOpts []Option
	leafOpts      []breaker.Option
}

// WithComposite sets options applied to every composite node. Name comes
// from the node value.
func WithComposite(opts ...Option) HierarchyOption {
	return func(o *hierarchyOptions) error {
		o.compositeOpts = opts
		return nil
	}
}

// WithLeaf sets options applied to every leaf breaker. Name comes from the
// node value.
func WithLeaf(opts ...breaker.Option) HierarchyOption {
	return func(o *hierarchyOptions) error {
		o.leafOpts = opts
		return nil
	}
}

// FromHierarchy builds nested composites from a tree.HierarchyModel. A
// node with children becomes a *Composite, a node without becomes a
// *breaker.Breaker leaf. Names come from node values; leaf Events default
// to the enclosing composite's Events. Model errors from tree.Hierarchy
// (missing root, cycle, breadth over maxBreadth) are wrapped in
// ErrInvalidConfig; a leaf deeper than MaxDepth returns ErrTooDeep.
func FromHierarchy(
	model tree.HierarchyModel,
	maxBreadth int,
	opts ...HierarchyOption,
) (*Composite, error) {
	var o hierarchyOptions
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	var next uint64
	root, err := tree.Hierarchy(model, maxBreadth, func() uint64 {
		next++
		return next
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	return buildComposite(root, o.compositeOpts, o.leafOpts)
}

// buildComposite builds n and its subtree bottom-up: children first, then
// attached to the new composite, so AttachChild enforces MaxDepth.
func buildComposite(
	n *tree.Node[string],
	compositeOpts []Option,
	leafOpts []breaker.Option,
) (*Composite, error) {
	opts := append([]Option{WithName(n.Val())}, compositeOpts...)
	c, err := New(opts...)
	if err != nil {
		return nil, err
	}
	for _, child := range n.ChildrenIter() {
		if !child.HasChildren() {
			if _, err := c.attachLeaf(child.Val(), leafOpts...); err != nil {
				return nil, err
			}
			continue
		}
		sub, err := buildComposite(child, compositeOpts, leafOpts)
		if err != nil {
			return nil, err
		}
		if err := c.AttachChild(sub); err != nil {
			return nil, err
		}
	}
	return c, nil
}
