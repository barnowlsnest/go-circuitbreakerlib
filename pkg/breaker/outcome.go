package breaker

// Outcome is the classification of one call, as decided by a Classifier.
type Outcome uint8

const (
	// OutcomeSuccess counts toward the window as a success.
	OutcomeSuccess Outcome = iota
	// OutcomeFailure counts toward the window as a failure.
	OutcomeFailure
	// OutcomeIgnored is not counted at all. In HalfOpen it releases the probe
	// slot the permit held without counting as a probe result.
	OutcomeIgnored
)

// String returns the outcome name.
func (o Outcome) String() string {
	switch o {
	case OutcomeSuccess:
		return "success"
	case OutcomeFailure:
		return "failure"
	case OutcomeIgnored:
		return "ignored"
	default:
		return "unknown"
	}
}

// Counts is a snapshot of a sliding window.
type Counts struct {
	Total                uint32
	Successes            uint32
	Failures             uint32
	Slow                 uint32
	ConsecutiveFailures  uint32
	ConsecutiveSuccesses uint32
}
