package adapter_test

import (
	"fmt"
	"net/http"
	"time"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/adapter"
	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/registry"
)

func ExampleNewHTTPRoundTripper() {
	// One breaker per host, created on first use.
	hosts, err := registry.New(
		registry.WithBreaker(
			breaker.WithPolicies(breaker.FailureRate{Threshold: 0.5, MinCalls: 20}),
			breaker.WithOpenDuration(30*time.Second),
		),
	)
	if err != nil {
		panic(err)
	}
	transport, err := adapter.NewHTTPRoundTripper(http.DefaultTransport, adapter.WithRegistry(hosts))
	if err != nil {
		panic(err)
	}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	fmt.Println(client.Transport != nil, hosts.Len())
	// Output:
	// true 0
}
