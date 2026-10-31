package breaker

import "github.com/barnowlsnest/go-circuitbreakerlib/internal/breakercore"

// Outcome is the classification of one call, as decided by a Classifier.
type Outcome = breakercore.Outcome

const (
	// OutcomeSuccess counts toward the window as a success.
	OutcomeSuccess = breakercore.OutcomeSuccess
	// OutcomeFailure counts toward the window as a failure.
	OutcomeFailure = breakercore.OutcomeFailure
	// OutcomeIgnored is not counted at all. In HalfOpen it releases the probe
	// slot the permit held without counting as a probe result.
	OutcomeIgnored = breakercore.OutcomeIgnored
)

// Counts is a snapshot of a sliding window.
type Counts = breakercore.Counts
