package adapter

import (
	"context"
	"errors"
	"fmt"
	"net/http"

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
	return Option{proto: protoHTTP, apply: func(s *settings) error {
		if fn == nil {
			return fmt.Errorf("%w: nil HTTP key", ErrInvalidOption)
		}
		s.httpKey = fn
		return nil
	}}
}

// WithHTTPClassifier sets the outcome classifier.
func WithHTTPClassifier(fn HTTPClassifier) Option {
	return Option{proto: protoHTTP, apply: func(s *settings) error {
		if fn == nil {
			return fmt.Errorf("%w: nil HTTP classifier", ErrInvalidOption)
		}
		s.httpClassifier = fn
		return nil
	}}
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

// RoundTrip implements http.RoundTripper through the breaker.
func (t *roundTripper) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	b, p, err := t.settings.acquire(t.settings.httpKey(req))
	if err != nil {
		return nil, err
	}
	start := b.Now()
	done := false
	defer func() {
		elapsed := b.Now().Sub(start)
		if !done {
			b.Record(p, breaker.OutcomeFailure, elapsed, err)
			return
		}
		b.Record(p, classifyHTTP(b, t.settings.httpClassifier, req.Context(), resp, err), elapsed, err)
	}()
	resp, err = t.next.RoundTrip(req)
	done = true
	if err != nil && resp != nil {
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
		return nil, err
	}
	return resp, err
}
