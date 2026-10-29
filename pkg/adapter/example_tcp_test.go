package adapter_test

import (
	"fmt"
	"net"
	"time"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/adapter"
	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/registry"
)

func ExampleNewTCPDialer() {
	// One breaker per address. Dial outcomes are always recorded; with
	// WithTCPTrackIO every Read and Write on the returned conn also asks the
	// breaker, so an open breaker fails established connections fast.
	addresses, err := registry.New(
		registry.WithBreaker(
			breaker.WithPolicies(breaker.ConsecutiveFailures{Count: 5}),
			breaker.WithOpenDuration(15*time.Second),
		),
	)
	if err != nil {
		panic(err)
	}
	dialer, err := adapter.NewTCPDialer(
		&net.Dialer{Timeout: 3 * time.Second},
		adapter.WithRegistry(addresses),
		adapter.WithTCPTrackIO(true),
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(dialer != nil, addresses.Len())
	// Output:
	// true 0
}
