// Package hooks ships ready-made breaker.Events implementations.
//
// LogEvents writes state changes, call outcomes, and rejections to a
// go-logslib logger with fixed levels and per-breaker rate limits on the
// noisy classes. MultiEvents fans one event stream out to several observers,
// recovering each one independently.
package hooks
