package composite

import "math"

// scanNever marks a cache that must be rebuilt on the next refresh.
const scanNever = math.MinInt64

// refreshScan recounts open children when the cache is older than
// Config.ScanInterval. It runs outside every lock; two callers racing
// simply compute the same answer twice.
func (c *Composite) refreshScan() {
	now := c.clock().UnixNano()
	if at := c.scanAt.Load(); at != scanNever && now-at < c.scanInterval {
		return
	}
	children := *c.children.Load()
	open := 0
	for _, child := range children {
		if child.State() == breakerOpen {
			open++
		}
	}
	c.scanCache.Store(uint64(open)<<32 | uint64(len(children)))
	c.scanAt.Store(now)
}

// invalidateScan forces a rebuild and performs it now. Called under mu.
func (c *Composite) invalidateScan() {
	c.scanAt.Store(scanNever)
	c.refreshScan()
}

// scanned returns the cached open and total child counts.
func (c *Composite) scanned() (open, total int) {
	v := c.scanCache.Load()
	return int(v >> 32), int(v & (1<<32 - 1))
}
