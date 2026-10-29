package adapter

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

const (
	testBodyDelay   = 50 * time.Millisecond
	testElapsedMax  = testBodyDelay / 2
	testStatusParam = "status"
	testURL         = "http://dep.internal/"
)

var errTransport = errors.New("transport down")

type HTTPSuite struct {
	suite.Suite
	events  *MockEvents
	next    *MockRoundTripper
	body    *MockBody
	breaker *breaker.Breaker
}

func TestHTTPSuite(t *testing.T) {
	suite.Run(t, new(HTTPSuite))
}

func (s *HTTPSuite) SetupTest() {
	s.events = newNoopMockEvents()
	s.next = &MockRoundTripper{}
	s.body = &MockBody{}
	b, err := breaker.New(&breaker.Config{
		Name:     testName,
		Policies: []breaker.Policy{breaker.ConsecutiveFailures{Count: 1}},
		Events:   s.events,
	})
	s.Require().NoError(err)
	s.breaker = b
}

func (s *HTTPSuite) TearDownTest() {
	s.events.AssertExpectations(s.T())
	s.next.AssertExpectations(s.T())
	s.body.AssertExpectations(s.T())
}

func (s *HTTPSuite) roundTripFails(rt http.RoundTripper, req *http.Request) error {
	s.T().Helper()
	resp, err := rt.RoundTrip(req)
	if resp != nil {
		s.NoError(resp.Body.Close())
	}
	s.Nil(resp)
	return err
}

func (s *HTTPSuite) request(ctx context.Context, url string) *http.Request {
	s.T().Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	s.Require().NoError(err)
	return req
}

// server answers with the status named in the query.
func (s *HTTPSuite) server() *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get(testStatusParam) {
		case "500":
			w.WriteHeader(http.StatusInternalServerError)
		case "slow-body":
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			time.Sleep(testBodyDelay)
			_, _ = io.WriteString(w, "late")
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	s.T().Cleanup(srv.CloseClientConnections)
	s.T().Cleanup(srv.Close)
	return srv
}

func (s *HTTPSuite) TestNilNextUsesDefaultTransport() {
	rt, err := NewHTTPRoundTripper(nil, WithBreaker(s.breaker))
	s.Require().NoError(err)
	srv := s.server()
	resp, err := rt.RoundTrip(s.request(context.Background(), srv.URL))
	s.Require().NoError(err)
	s.NoError(resp.Body.Close())
	s.Equal(http.StatusOK, resp.StatusCode)
	s.Equal(uint32(1), s.breaker.Counts().Successes)
}

func (s *HTTPSuite) TestDefaultClassifier() {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancelExpired()
	tests := []struct {
		ctx      context.Context
		err      error
		name     string
		status   int
		expected breaker.Outcome
	}{
		{name: "200", ctx: context.Background(), status: http.StatusOK, expected: breaker.OutcomeSuccess},
		{name: "404", ctx: context.Background(), status: http.StatusNotFound, expected: breaker.OutcomeSuccess},
		{name: "429", ctx: context.Background(), status: http.StatusTooManyRequests, expected: breaker.OutcomeSuccess},
		{name: "408", ctx: context.Background(), status: http.StatusRequestTimeout, expected: breaker.OutcomeFailure},
		{name: "500", ctx: context.Background(), status: http.StatusInternalServerError, expected: breaker.OutcomeFailure},
		{name: "503", ctx: context.Background(), status: http.StatusServiceUnavailable, expected: breaker.OutcomeFailure},
		{name: "transport error", ctx: context.Background(), err: errTransport, expected: breaker.OutcomeFailure},
		{name: "nil response and nil error", ctx: context.Background(), expected: breaker.OutcomeFailure},
		{name: "canceled context", ctx: canceled, err: context.Canceled, expected: breaker.OutcomeIgnored},
		{name: "deadline with error", ctx: expired, err: context.DeadlineExceeded, expected: breaker.OutcomeFailure},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			var resp *http.Response
			if tt.status != 0 {
				resp = &http.Response{StatusCode: tt.status}
			}
			s.Equal(tt.expected, DefaultHTTPClassifier(tt.ctx, resp, tt.err))
		})
	}
}

func (s *HTTPSuite) TestOutcomeRecorded() {
	tests := []struct {
		err              error
		expectedErr      error
		name             string
		status           int
		expectedFailures uint32
	}{
		{name: "success", status: http.StatusOK, expectedFailures: 0},
		{name: "server error", status: http.StatusBadGateway, expectedFailures: 1},
		{name: "transport error", err: errTransport, expectedErr: errTransport, expectedFailures: 1},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.SetupTest()
			defer s.TearDownTest()
			rt, err := NewHTTPRoundTripper(s.next, WithBreaker(s.breaker))
			s.Require().NoError(err)
			req := s.request(context.Background(), testURL)
			var resp *http.Response
			if tt.status != 0 {
				resp = &http.Response{StatusCode: tt.status, Body: http.NoBody}
			}
			s.next.On("RoundTrip", req).Return(resp, tt.err).Once()
			actual, err := rt.RoundTrip(req)
			s.ErrorIs(err, tt.expectedErr)
			s.Same(resp, actual)
			if actual != nil {
				s.NoError(actual.Body.Close())
			}
			s.Equal(uint32(1), s.breaker.Counts().Total)
			s.Equal(tt.expectedFailures, s.breaker.Counts().Failures)
		})
	}
}

func (s *HTTPSuite) TestCustomClassifier() {
	rt, err := NewHTTPRoundTripper(s.next, WithBreaker(s.breaker), WithHTTPClassifier(
		func(context.Context, *http.Response, error) breaker.Outcome { return breaker.OutcomeIgnored },
	))
	s.Require().NoError(err)
	req := s.request(context.Background(), testURL)
	s.next.On("RoundTrip", req).Return(nil, errTransport).Once()
	s.ErrorIs(s.roundTripFails(rt, req), errTransport)
	s.Equal(uint32(0), s.breaker.Counts().Total)
}

func (s *HTTPSuite) TestClassifierPanicIsFailure() {
	rt, err := NewHTTPRoundTripper(s.next, WithBreaker(s.breaker), WithHTTPClassifier(
		func(context.Context, *http.Response, error) breaker.Outcome { panic(testClassifyPanic) },
	))
	s.Require().NoError(err)
	req := s.request(context.Background(), testURL)
	s.next.On("RoundTrip", req).Return(&http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil).Once()
	resp, err := rt.RoundTrip(req)
	s.Require().NoError(err)
	s.NoError(resp.Body.Close())
	s.Equal(uint32(1), s.breaker.Counts().Failures)
	s.events.AssertCalled(s.T(), "OnHookPanic", hookClassifier, testClassifyPanic)
}

func (s *HTTPSuite) TestRejectedWhenOpen() {
	rt, err := NewHTTPRoundTripper(s.next, WithBreaker(s.breaker))
	s.Require().NoError(err)
	s.breaker.ForceOpen()
	err = s.roundTripFails(rt, s.request(context.Background(), testURL))
	s.ErrorIs(err, breaker.ErrOpen)
	var re *breaker.RejectedError
	s.ErrorAs(err, &re)
	s.next.AssertNotCalled(s.T(), "RoundTrip", mock.Anything)
}

func (s *HTTPSuite) TestRegistryKeyedByHost() {
	reg := testRegistry(s.T())
	rt, err := NewHTTPRoundTripper(s.next, WithRegistry(reg))
	s.Require().NoError(err)
	for _, host := range []string{"a.internal", "b.internal"} {
		req := s.request(context.Background(), "http://"+host+"/path")
		s.next.On("RoundTrip", req).Return(&http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil).Once()
		resp, err := rt.RoundTrip(req)
		s.Require().NoError(err)
		s.NoError(resp.Body.Close())
	}
	s.Equal(2, reg.Len())
	b, ok := reg.Lookup("a.internal")
	s.True(ok)
	s.Equal(uint32(1), b.Counts().Successes)
}

func (s *HTTPSuite) TestCustomKey() {
	reg := testRegistry(s.T())
	rt, err := NewHTTPRoundTripper(s.next, WithRegistry(reg), WithHTTPKey(func(r *http.Request) string { return r.URL.Path }))
	s.Require().NoError(err)
	req := s.request(context.Background(), "http://a.internal/by-path")
	s.next.On("RoundTrip", req).Return(&http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil).Once()
	resp, err := rt.RoundTrip(req)
	s.Require().NoError(err)
	s.NoError(resp.Body.Close())
	_, ok := reg.Lookup("/by-path")
	s.True(ok)
}

func (s *HTTPSuite) TestRegistryErrorFailsRequest() {
	reg := testFailingRegistry(s.T())
	rt, err := NewHTTPRoundTripper(s.next, WithRegistry(reg))
	s.Require().NoError(err)
	s.ErrorIs(s.roundTripFails(rt, s.request(context.Background(), testURL)), ErrRegistry)
	s.next.AssertNotCalled(s.T(), "RoundTrip", mock.Anything)
}

func (s *HTTPSuite) TestResponseAndErrorBothSet() {
	rt, err := NewHTTPRoundTripper(s.next, WithBreaker(s.breaker))
	s.Require().NoError(err)
	req := s.request(context.Background(), testURL)
	s.body.On("Close").Return(nil).Once()
	s.next.On("RoundTrip", req).Return(&http.Response{StatusCode: http.StatusOK, Body: s.body}, errTransport).Once()
	s.ErrorIs(s.roundTripFails(rt, req), errTransport)
	s.Equal(uint32(1), s.breaker.Counts().Failures)
}

func (s *HTTPSuite) TestElapsedExcludesBody() {
	rt, err := NewHTTPRoundTripper(nil, WithBreaker(s.breaker))
	s.Require().NoError(err)
	srv := s.server()
	resp, err := rt.RoundTrip(s.request(context.Background(), srv.URL+"/?"+testStatusParam+"=slow-body"))
	s.Require().NoError(err)
	body, err := io.ReadAll(resp.Body)
	s.Require().NoError(err)
	s.NoError(resp.Body.Close())
	s.Equal("late", strings.TrimSpace(string(body)))
	s.events.AssertCalled(s.T(), "OnCall", mock.MatchedBy(func(ev breaker.CallEvent) bool {
		return ev.Outcome == breaker.OutcomeSuccess && ev.Elapsed < testElapsedMax
	}))
}
