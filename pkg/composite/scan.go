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
	last := c.scan.Load()
	if last != nil {
		age := now.Sub(last.at)
		if age < c.scanInterval {
			return
		}
	}
	children := *c.children.Load()
	result := &scanResult{at: now, total: len(children)}
	for _, child := range children {
		if child.State() == breaker.StateOpen {
			result.open++
		}
	}
	c.scan.Store(result)
}

// invalidateScan drops the cache and rebuilds it now. Called under mu.
func (c *Composite) invalidateScan() {
	c.scan.Store(nil)
	c.refreshScan()
}

// scanned returns the cached open and total child counts.
func (c *Composite) scanned() (open, total int) {
	last := c.scan.Load()
	if last == nil {
		return 0, 0
	}
	return last.open, last.total
}
