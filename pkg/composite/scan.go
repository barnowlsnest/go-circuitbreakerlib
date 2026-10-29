package composite

import (
	"time"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

// scanResult is one count of the children, taken at a point in time.
type scanResult struct {
	at    time.Time
	open  int
	total int
}

// refreshScan recounts open children when the cache is older than
// Config.ScanInterval. It runs outside every lock; two callers racing
// simply compute the same answer twice.
func (c *Composite) refreshScan() {
	now := c.clock()
	if now.Sub(c.scan.Load().at) < c.scanInterval {
		return
	}
	c.rescan(now)
}

// rescan counts open children as of now and stores the result.
func (c *Composite) rescan(now time.Time) {
	children := *c.children.Load()
	result := &scanResult{at: now, total: len(children)}
	for _, child := range children {
		if child.State() == breaker.StateOpen {
			result.open++
		}
	}
	c.scan.Store(result)
}

// invalidateScan rebuilds the cache now. Called under topology.
func (c *Composite) invalidateScan() { c.rescan(c.clock()) }

// scanned returns the cached open and total child counts.
func (c *Composite) scanned() (open, total int) {
	last := c.scan.Load()
	return last.open, last.total
}
