package breaker

// Permit is returned by Allow and must be passed back to Record. It carries
// the state epoch it was issued in, so Record can discard the outcome of a
// call that finished after the breaker moved on, and the half-open probe
// slot it holds, if any. A zero Permit is always stale.
type Permit struct {
	epoch uint64
	slot  uint32
}

// IsProbe reports whether the permit was issued in HalfOpen and holds a
// probe slot.
func (p Permit) IsProbe() bool { return p.slot != 0 }
