package breaker

import "github.com/barnowlsnest/go-circuitbreakerlib/internal/breakercore"

// State is the breaker's position in the Closed / Open / HalfOpen cycle.
// It is packed with the epoch into one atomic word so Allow reads both
// with a single load.
type State = breakercore.State

const (
	// StateClosed admits every call and records outcomes.
	StateClosed = breakercore.StateClosed
	// StateOpen rejects every call until the backoff elapses.
	StateOpen = breakercore.StateOpen
	// StateHalfOpen admits a bounded number of probe calls.
	StateHalfOpen = breakercore.StateHalfOpen
)

// Reason explains a state change. Policy trips use Reason(policy.Name()).
type Reason = breakercore.Reason

const (
	// ReasonTimeout is Open to HalfOpen after the backoff elapsed.
	ReasonTimeout = breakercore.ReasonTimeout
	// ReasonEarlyHalfOpen is Open to HalfOpen because Config.ReopenEarly returned true.
	ReasonEarlyHalfOpen = breakercore.ReasonEarlyHalfOpen
	// ReasonHalfOpenSuccess is HalfOpen to Closed after enough probe successes.
	ReasonHalfOpenSuccess = breakercore.ReasonHalfOpenSuccess
	// ReasonHalfOpenFailure is HalfOpen to Open after a probe failure.
	ReasonHalfOpenFailure = breakercore.ReasonHalfOpenFailure
	// ReasonProbeTimeout is HalfOpen to Open because a probe was never recorded.
	ReasonProbeTimeout = breakercore.ReasonProbeTimeout
	// ReasonForced is a ForceOpen or ForceClose call.
	ReasonForced = breakercore.ReasonForced
	// ReasonReset is a Reset call.
	ReasonReset = breakercore.ReasonReset
)
