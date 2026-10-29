// Package registry keeps breakers by key: one per dependency, route,
// tenant, or whatever the caller groups by.
//
// Get creates a breaker on first use from WithBreaker options and returns
// the same breaker afterwards. The hit path is a sync.Map lookup with no
// allocation. With WithMaxEntries the registry is bounded: when full, a
// create samples a few random entries and evicts the least recently used
// Closed one, so a dependency that is currently down is not forgotten.
package registry
