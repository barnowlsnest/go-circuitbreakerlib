// Package adapter wraps HTTP, gRPC, and TCP clients with a breaker.
//
// Every constructor takes the shared options WithBreaker or WithRegistry,
// exactly one of them, plus protocol-specific options for the key and the
// classifier. Rejections surface as *breaker.RejectedError (a gRPC status
// for the interceptors). A registry error fails the request wrapped in
// ErrRegistry; a call is never passed through unprotected.
package adapter
