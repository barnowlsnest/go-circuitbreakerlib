package composite

import (
	"fmt"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

// topology serializes attach and detach across every composite, so the
// cycle and depth checks of two concurrent attaches cannot each pass
// against the tree as it was before the other finished.
var topology sync.Mutex

// Child is one member of a composite: a *breaker.Breaker leaf or a nested
// *Composite. State is the lazy read-only view used by the open-children
// scan; a child whose backoff has elapsed reports HalfOpen and does not
// count as open.
type Child interface {
	Name() string
	State() breaker.State
	Allow() (breaker.Permit, error)
	Record(p breaker.Permit, outcome breaker.Outcome, elapsed time.Duration, err error)
}

// AddChild builds a leaf from WithChild options and attaches it. The leaf
// name is id; a nil Events becomes the composite's Events.
func (c *Composite) AddChild(id string) (*breaker.Breaker, error) {
	return c.attachLeaf(id, c.childOpts...)
}

// attachLeaf builds a leaf from opts, names it id, fills Events from the
// composite when unset, and attaches it.
func (c *Composite) attachLeaf(id string, opts ...breaker.Option) (*breaker.Breaker, error) {
	leafOpts := make([]breaker.Option, 0, len(opts)+2)
	leafOpts = append(leafOpts, opts...)
	leafOpts = append(leafOpts, breaker.WithName(id), breaker.FillEvents(c.events))
	b, err := breaker.New(leafOpts...)
	if err != nil {
		return nil, err
	}
	if err := c.AttachChild(b); err != nil {
		return nil, err
	}
	return b, nil
}

// AttachChild adds an existing child, keyed by its Name. A *Composite can
// have one parent: attaching it a second time returns ErrAlreadyAttached,
// because a shared sub-composite would record twice per call and break the
// depth accounting. A *breaker.Breaker leaf may be attached to any number
// of composites, since every path takes its own permit on it.
func (c *Composite) AttachChild(child Child) error {
	if isNil(child) {
		return ErrNilChild
	}
	topology.Lock()
	defer topology.Unlock()
	name := child.Name()
	current := *c.children.Load()
	if indexOf(current, name) >= 0 {
		return fmt.Errorf("%w: %q", ErrDuplicateChild, name)
	}
	sub, isComposite := child.(*Composite)
	if isComposite && (sub == c || c.hasAncestor(sub)) {
		return ErrCycle
	}
	if c.rootDistance()+heightOf(child) > MaxDepth {
		return ErrTooDeep
	}
	if isComposite && !sub.owner.CompareAndSwap(nil, c) {
		return ErrAlreadyAttached
	}
	next := slices.Concat(current, []Child{child})
	c.children.Store(&next)
	c.invalidateScan()
	return nil
}

// RemoveChild detaches the child named id and reports whether it was
// present. A removed *Composite can be attached elsewhere afterwards.
func (c *Composite) RemoveChild(id string) bool {
	topology.Lock()
	defer topology.Unlock()
	current := *c.children.Load()
	i := indexOf(current, id)
	if i < 0 {
		return false
	}
	next := slices.Concat(current[:i], current[i+1:])
	c.children.Store(&next)
	if sub, ok := current[i].(*Composite); ok {
		sub.owner.Store(nil)
	}
	c.invalidateScan()
	return true
}

// Child returns the child named id.
func (c *Composite) Child(id string) (Child, bool) {
	current := *c.children.Load()
	if i := indexOf(current, id); i >= 0 {
		return current[i], true
	}
	return nil, false
}

// Children returns the child names in attachment order.
func (c *Composite) Children() []string {
	current := *c.children.Load()
	names := make([]string, len(current))
	for i, child := range current {
		names[i] = child.Name()
	}
	return names
}

// rootDistance is 1 for a root composite, 2 for its composite children,
// and so on.
func (c *Composite) rootDistance() int {
	n := 1
	for p := c.owner.Load(); p != nil; p = p.owner.Load() {
		n++
	}
	return n
}

// height is 1 for a composite with no children, 2 with leaves only, and so
// on: the number of hops a Lease needs from here to the deepest leaf.
func (c *Composite) height() int {
	h := 1
	for _, child := range *c.children.Load() {
		h = max(h, heightOf(child)+1)
	}
	return h
}

// heightOf is 1 for a leaf and the composite's height otherwise.
func heightOf(child Child) int {
	if sub, ok := child.(*Composite); ok {
		return sub.height()
	}
	return 1
}

func indexOf(children []Child, name string) int {
	return slices.IndexFunc(children, func(child Child) bool { return child.Name() == name })
}

func (c *Composite) hasAncestor(target *Composite) bool {
	for p := c.owner.Load(); p != nil; p = p.owner.Load() {
		if p == target {
			return true
		}
	}
	return false
}

// isNil catches a nil interface and a typed nil pointer inside one.
func isNil(child Child) bool {
	if child == nil {
		return true
	}
	v := reflect.ValueOf(child)
	return v.Kind() == reflect.Pointer && v.IsNil()
}
