package adapter

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

// HTTPKeyFunc picks the registry key for a request. The default is
// req.URL.Host.
type HTTPKeyFunc func(req *http.Request) string

// HTTPClassifier decides the outcome of a round trip. resp may be nil.
type HTTPClassifier func(ctx context.Context, resp *http.Response, err error) breaker.Outcome

// DefaultHTTPKey keys by req.URL.Host.
func DefaultHTTPKey(req *http.Request) string { return req.URL.Host }

// DefaultHTTPClassifier ignores a call the caller canceled, counts any
// transport error (including a deadline) as a failure, counts status 5xx
// and 408 as failures, and everything else, including 429, as success.
func DefaultHTTPClassifier(ctx context.Context, resp *http.Response, err error) breaker.Outcome {
	if errors.Is(ctx.Err(), context.Canceled) {
		return breaker.OutcomeIgnored
	}
	if err != nil || resp == nil {
		return breaker.OutcomeFailure
	}
	if resp.StatusCode >= http.StatusInternalServerError || resp.StatusCode == http.StatusRequestTimeout {
		return breaker.OutcomeFailure
	}
	return breaker.OutcomeSuccess
}

// WithHTTPKey sets the registry key function.
func WithHTTPKey(fn HTTPKeyFunc) Option {
	return funcOption(protoHTTP, "HTTP key", fn == nil, func(s *settings) { s.httpKey = fn })
}

// WithHTTPClassifier sets the outcome classifier.
func WithHTTPClassifier(fn HTTPClassifier) Option {
	return funcOption(protoHTTP, "HTTP classifier", fn == nil, func(s *settings) { s.httpClassifier = fn })
}

// roundTripper is the breaker-protected http.RoundTripper.
type roundTripper struct {
	next     http.RoundTripper
	settings settings
}

// NewHTTPRoundTripper wraps next with a breaker. A nil next means
// http.DefaultTransport. Elapsed time covers next.RoundTrip only, up to the
// response headers, not the body read.
func NewHTTPRoundTripper(next http.RoundTripper, opts ...Option) (http.RoundTripper, error) {
	s, err := newSettings(protoHTTP, opts)
	if err != nil {
		return nil, err
	}
	if next == nil {
		next = http.DefaultTransport
	}
	return &roundTripper{next: next, settings: s}, nil
}

// RoundTrip implements http.RoundTripper. A rejection returns a
// *breaker.RejectedError without calling next. If next breaks the
// http.RoundTripper contract and returns both a response and an error, the
// body is closed and only the error is returned.
func (t *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	b, p, err := t.settings.acquire(t.settings.httpKey(req))
	if err != nil {
		return nil, err
	}
	start := time.Now()
	resp, err := t.next.RoundTrip(req)
	elapsed := time.Since(start)
	b.Record(p, classify(b, func() breaker.Outcome { return t.settings.httpClassifier(req.Context(), resp, err) }), elapsed)
	if err != nil && resp != nil {
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
		return nil, err
	}
	return resp, err
}
