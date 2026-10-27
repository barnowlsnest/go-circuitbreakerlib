// Package composite groups breakers into a tree and opens a parent when
// enough of its children are down.
//
// A Composite owns a real breaker.Breaker as its parent plus a set of
// children, each a *breaker.Breaker or another *Composite. Pick walks the
// tree to the first healthy leaf and returns a Lease holding one permit per
// level; Record settles every permit on that path. The parent's trip
// decision is a TripFunc over the number of open children and the parent's
// aggregate counts. FromHierarchy builds a nested tree from a declarative
// go-datalib tree.HierarchyModel.
package composite
