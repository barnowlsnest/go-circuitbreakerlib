package composite

import (
	"context"
	"time"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

// Lease holds the permits taken along the path from a composite down to a
// leaf. It is a value type: Pick allocates nothing.
type Lease struct {
	// Leaf is the child that admitted the call.
	Leaf    Child
	hops    [MaxDepth - 1]*Composite
	permits [MaxDepth]breaker.Permit
	depth   uint8
}

// ID returns the leaf's name.
func (l Lease) ID() string {
	if l.Leaf == nil {
		return ""
	}
	return l.Leaf.Name()
}

// Path returns the names from the root composite down to the leaf. It
// allocates; use it for logging.
func (l Lease) Path() []string {
	if l.Leaf == nil {
		return nil
	}
	path := make([]string, 0, int(l.depth)+1)
	for i := range int(l.depth) {
		path = append(path, l.hops[i].Name())
	}
	return append(path, l.Leaf.Name())
}

// Pick takes a permit from the parent and then from the first child, in
// round-robin order starting after the last child that admitted, that
// admits the call; composite children are walked
// the same way. A parent rejection returns its *breaker.RejectedError.
// When no child admits, every permit taken is released and the error is a
// *breaker.RejectedError wrapping ErrNoHealthyChildren, whose State is the
// parent's own state, usually Closed.
func (c *Composite) Pick() (Lease, error) {
	var l Lease
	if err := c.pick(&l); err != nil {
		return Lease{}, err
	}
	return l, nil
}

// pick appends this composite's hop to l and recurses. On failure it
// releases its own permit so the caller moves on to the next sibling.
func (c *Composite) pick(l *Lease) error {
	c.refreshScan()
	p, err := c.parent.Allow()
	if err != nil {
		return err
	}
	level := int(l.depth)
	l.hops[level] = c
	l.permits[level] = p
	l.depth++
	children := *c.children.Load()
	n := len(children)
	start := int(c.rr.Load())
	for i := range n {
		idx := (start + i) % n
		child := children[idx]
		if sub, ok := child.(*Composite); ok {
			if sub.pick(l) == nil {
				c.rr.Store(int64(idx + 1))
				return nil
			}
			continue
		}
		cp, err := child.Allow()
		if err != nil {
			continue
		}
		l.Leaf = child
		l.permits[l.depth] = cp
		c.rr.Store(int64(idx + 1))
		return nil
	}
	l.depth--
	l.hops[level] = nil
	c.parent.Record(p, breaker.OutcomeIgnored, 0)
	state := c.parent.State()
	c.rejectNoHealthy(c.clock(), state)
	return c.noHealthy[state]
}

// RecordLease records outcome on the leaf and then on every composite along
// the path, bottom-up, so each parent's scan sees the leaf's new state. Each
// hop checks its own permit epoch, so a straggler may count on some levels
// and be discarded on others; the parent aggregate can therefore differ
// from the sum of its leaves by in-flight stragglers.
func (c *Composite) RecordLease(l Lease, outcome breaker.Outcome, elapsed time.Duration) {
	if l.Leaf == nil {
		return
	}
	l.Leaf.Record(l.permits[l.depth], outcome, elapsed)
	for i := int(l.depth) - 1; i >= 0; i-- {
		l.hops[i].Record(l.permits[i], outcome, elapsed)
	}
}

// Execute runs fn against the leaf Pick chooses and records the outcome,
// classified by the parent's Classifier with ctx, in a defer, so a
// panicking fn records a failure before the panic propagates. A canceled
// ctx returns ctx.Err() without touching any breaker.
func (c *Composite) Execute(ctx context.Context, fn func(ctx context.Context, leaf string) error) error {
	if fn == nil {
		return breaker.ErrNilFunc
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	l, err := c.Pick()
	if err != nil {
		return err
	}
	start := c.clock()
	done := false
	var callErr error
	defer func() {
		elapsed := c.clock().Sub(start)
		if !done {
			c.RecordLease(l, breaker.OutcomeFailure, elapsed)
			return
		}
		c.RecordLease(l, c.classify(ctx, callErr), elapsed)
	}()
	callErr = fn(ctx, l.ID())
	done = true
	return callErr
}

func (c *Composite) classify(ctx context.Context, err error) (o breaker.Outcome) {
	defer func() {
		if r := recover(); r != nil {
			o = breaker.OutcomeFailure
			c.fireHookPanic(c.events, hookClassifier, r)
		}
	}()
	return c.classifier(ctx, err)
}
