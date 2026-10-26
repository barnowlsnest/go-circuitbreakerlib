// Package breaker implements a circuit breaker for calls to remote
// dependencies.
//
// A Breaker has three states. In Closed it admits every call and records
// outcomes in a sliding window; when a Policy decides the window looks
// unhealthy the breaker trips to Open. In Open it rejects calls immediately
// with ErrOpen until a Backoff-derived delay has elapsed. It then moves to
// HalfOpen and admits a bounded number of probe calls; enough successes
// close it again, any failure reopens it with a longer delay.
//
// Allow returns a Permit that the caller passes back to Record. The permit
// carries the state epoch it was issued in, so a slow call that completes
// after the breaker has moved on is discarded instead of corrupting the
// new state.
//
// Every state change, recorded call, and rejection is reported through the
// Events interface. NoopEvents discards them; the hooks package provides a
// logging implementation.
package breaker
