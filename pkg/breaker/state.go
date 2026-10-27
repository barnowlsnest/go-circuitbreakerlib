package breaker

// State is the breaker's position in the Closed / Open / HalfOpen cycle.
// It is packed with the epoch into one atomic word so Allow reads both
// with a single load.
type State uint32

const (
	// StateClosed admits every call and records outcomes.
	StateClosed State = iota
	// StateOpen rejects every call until the backoff elapses.
	StateOpen
	// StateHalfOpen admits a bounded number of probe calls.
	StateHalfOpen
)

// String returns the lower-case state name.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return unknownName
	}
}

// Reason explains a state change. Policy trips use Reason(policy.Name()).
type Reason string

const (
	// ReasonTimeout is Open to HalfOpen after the backoff elapsed.
	ReasonTimeout Reason = "timeout"
	// ReasonEarlyHalfOpen is Open to HalfOpen because Config.ReopenEarly returned true.
	ReasonEarlyHalfOpen Reason = "early-half-open"
	// ReasonHalfOpenSuccess is HalfOpen to Closed after enough probe successes.
	ReasonHalfOpenSuccess Reason = "half-open-success"
	// ReasonHalfOpenFailure is HalfOpen to Open after a probe failure.
	ReasonHalfOpenFailure Reason = "half-open-failure"
	// ReasonProbeTimeout is HalfOpen to Open because a probe was never recorded.
	ReasonProbeTimeout Reason = "probe-timeout"
	// ReasonForced is a ForceOpen or ForceClose call.
	ReasonForced Reason = "forced"
	// ReasonReset is a Reset call.
	ReasonReset Reason = "reset"
)
