package breaker

import "time"

// Snapshot is one consistent read of a breaker's observable state, for
// operators and dashboards.
type Snapshot struct {
	OpenedAt time.Time
	ReopenAt time.Time
	Counts   Counts
	Epoch    uint64
	Attempt  int
	InFlight uint32
	State    State
	Forced   bool
}
