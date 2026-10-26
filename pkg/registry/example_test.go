package registry_test

import (
	"fmt"
	"time"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/registry"
)

func ExampleRegistry() {
	// One breaker per upstream host, at most 100 of them.
	r := registry.New(func(host string) breaker.Config {
		return breaker.Config{
			Policies:     []breaker.Policy{breaker.FailureRate{Threshold: 0.5, MinCalls: 20}},
			OpenDuration: 30 * time.Second,
		}
	}, registry.WithMaxEntries(100))

	b, err := r.Get("payments.internal")
	if err != nil {
		panic(err)
	}
	again, _ := r.Get("payments.internal")
	fmt.Println(b.Name(), b == again, r.Len())
	// Output:
	// payments.internal true 1
}
