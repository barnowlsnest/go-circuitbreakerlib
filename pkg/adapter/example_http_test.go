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
	hosts := registry.New(func(host string) breaker.Config {
		return breaker.Config{
			Policies:     []breaker.Policy{breaker.FailureRate{Threshold: 0.5, MinCalls: 20}},
			OpenDuration: 30 * time.Second,
		}
	})
	transport, err := adapter.NewHTTPRoundTripper(nil, adapter.WithRegistry(hosts))
	if err != nil {
		panic(err)
	}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	fmt.Println(client.Transport != nil, hosts.Len())
	// Output:
	// true 0
}
