package breaker_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

func ExampleNew() {
	b, err := breaker.New(
		breaker.WithName("payments"),
		breaker.WithWindow(breaker.SlidingTimeWindow{Size: 10 * time.Second, Buckets: 10}),
		breaker.WithPolicies(
			breaker.FailureRate{Threshold: 0.5, MinCalls: 20},
			breaker.ConsecutiveFailures{Count: 5},
		),
		breaker.WithBackoff(breaker.ExponentialBackoff{Base: 5 * time.Second, Max: time.Minute, Jitter: true}),
		breaker.WithSlowCallDuration(2*time.Second),
		breaker.WithHalfOpenMaxCalls(1),
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(b.Name(), b.State())
	// Output: payments closed
}

func ExampleBreaker_Execute() {
	b, err := breaker.New(breaker.WithName("inventory"))
	if err != nil {
		panic(err)
	}
	err = b.Execute(context.Background(), func(ctx context.Context) error {
		return errors.New("upstream unavailable")
	})
	fmt.Println(err, b.State())
	// Output: upstream unavailable closed
}

func ExampleDo() {
	b, err := breaker.New(breaker.WithName("pricing"))
	if err != nil {
		panic(err)
	}
	price, err := breaker.Do(context.Background(), b, func(ctx context.Context) (int, error) {
		return 1999, nil
	})
	fmt.Println(price, err)
	// Output: 1999 <nil>
}

// metricsEvents forwards breaker events to a metrics client. Hooks run on
// the calling goroutine after the breaker lock is released, so they must
// not block: counters are incremented here and exported elsewhere.
type metricsEvents struct {
	breaker.NoopEvents
	transitions map[string]int
	rejections  map[string]int
}

func (m *metricsEvents) OnStateChange(ev breaker.StateChangeEvent) {
	m.transitions[ev.Name+":"+ev.To.String()]++
}

func (m *metricsEvents) OnReject(ev breaker.RejectEvent) {
	m.rejections[ev.Name]++
}

func ExampleEvents() {
	metrics := &metricsEvents{transitions: map[string]int{}, rejections: map[string]int{}}
	b, err := breaker.New(
		breaker.WithName("search"),
		breaker.WithPolicies(breaker.ConsecutiveFailures{Count: 1}),
		breaker.WithEvents(metrics),
	)
	if err != nil {
		panic(err)
	}
	p, _ := b.Allow()
	b.Record(p, breaker.OutcomeFailure, 0, nil)
	_, err = b.Allow()
	fmt.Println(err != nil, metrics.transitions["search:open"], metrics.rejections["search"])
	// Output: true 1 1
}
