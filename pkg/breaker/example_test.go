package breaker_test

import (
	"fmt"
	"time"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

func ExampleConfig() {
	cfg := breaker.Config{
		Name:             "payments",
		Window:           breaker.SlidingTimeWindow{Size: 10 * time.Second, Buckets: 10},
		Policies:         []breaker.Policy{breaker.FailureRate{Threshold: 0.5, MinCalls: 20}, breaker.ConsecutiveFailures{Count: 5}},
		Backoff:          breaker.ExponentialBackoff{Base: 5 * time.Second, Max: time.Minute, Jitter: true},
		SlowCallDuration: 2 * time.Second,
		HalfOpenMaxCalls: 1,
	}
	fmt.Println(cfg.Name, cfg.Policies[0].Name(), cfg.Policies[1].Name())
	// Output: payments failure-rate consecutive-failures
}
