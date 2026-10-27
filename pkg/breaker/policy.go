package breaker

// Policy decides whether a Closed breaker should trip. ShouldTrip runs under
// the breaker lock after each recorded outcome in Closed and once on the
// closing probe in HalfOpen; Name becomes the Reason of the resulting
// state change.
type Policy interface {
	ShouldTrip(c Counts) bool
	Name() string
}

// FailureRate trips when Failures/Total >= Threshold and Total >= MinCalls.
type FailureRate struct {
	Threshold float64
	MinCalls  uint32
}

// ShouldTrip implements Policy.
func (p FailureRate) ShouldTrip(c Counts) bool {
	return c.Total >= p.MinCalls && c.Total > 0 && float64(c.Failures)/float64(c.Total) >= p.Threshold
}

// Name implements Policy.
func (FailureRate) Name() string { return "failure-rate" }

// ConsecutiveFailures trips when ConsecutiveFailures >= Count.
type ConsecutiveFailures struct {
	Count uint32
}

// ShouldTrip implements Policy.
func (p ConsecutiveFailures) ShouldTrip(c Counts) bool { return c.ConsecutiveFailures >= p.Count }

// Name implements Policy.
func (ConsecutiveFailures) Name() string { return "consecutive-failures" }

// SlowCallRate trips when Slow/Total >= Threshold and Total >= MinCalls.
type SlowCallRate struct {
	Threshold float64
	MinCalls  uint32
}

// ShouldTrip implements Policy.
func (p SlowCallRate) ShouldTrip(c Counts) bool {
	return c.Total >= p.MinCalls && c.Total > 0 && float64(c.Slow)/float64(c.Total) >= p.Threshold
}

// Name implements Policy.
func (SlowCallRate) Name() string { return "slow-call-rate" }

// FailureCount trips when Failures >= Count within the window.
type FailureCount struct {
	Count uint32
}

// ShouldTrip implements Policy.
func (p FailureCount) ShouldTrip(c Counts) bool { return c.Failures >= p.Count }

// Name implements Policy.
func (FailureCount) Name() string { return "failure-count" }

func validatePolicy(p Policy) error {
	switch v := p.(type) {
	case FailureRate:
		return validateRate(v.Threshold, v.MinCalls)
	case SlowCallRate:
		return validateRate(v.Threshold, v.MinCalls)
	case ConsecutiveFailures:
		if v.Count == 0 {
			return ErrPolicyCount
		}
	case FailureCount:
		if v.Count == 0 {
			return ErrPolicyCount
		}
	case nil:
		return ErrNilPolicy
	}
	return nil
}

func validateRate(threshold float64, minCalls uint32) error {
	if threshold <= 0 || threshold > 1 {
		return ErrPolicyThreshold
	}
	if minCalls == 0 {
		return ErrPolicyCount
	}
	return nil
}
