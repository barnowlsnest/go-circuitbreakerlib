package registry_test

import (
	"fmt"
	"os"
	"time"

	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/hooks"
	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/registry"
)

func ExampleRegistry() {
	// One breaker per upstream host, at most 100 of them.
	r, err := registry.New(func(host string) breaker.Config {
		return breaker.Config{
			Policies:     []breaker.Policy{breaker.FailureRate{Threshold: 0.5, MinCalls: 20}},
			OpenDuration: 30 * time.Second,
		}
	}, registry.WithMaxEntries(100))
	if err != nil {
		panic(err)
	}

	b, err := r.Get("payments.internal")
	if err != nil {
		panic(err)
	}
	again, _ := r.Get("payments.internal")
	fmt.Println(b.Name(), b == again, r.Len())
	// Output:
	// payments.internal true 1
}

func ExampleWithOnEvict() {
	log := logger.New(logger.Config{
		Level:      logger.InfoLevel,
		Format:     logger.JSONFormat,
		Output:     os.Stderr,
		BufferSize: 64 << 10, // lines are written on the caller's goroutine; buffer them
	})
	events, err := hooks.NewLogEvents(log)
	if err != nil {
		panic(err)
	}
	// Every breaker gets the log hook, and the hook's per-breaker
	// rate-limiter state is dropped when the registry evicts a breaker.
	hosts, err := registry.New(
		func(host string) breaker.Config { return breaker.Config{} },
		registry.WithEvents(events),
		registry.WithMaxEntries(1000),
		registry.WithOnEvict(func(key string, _ *breaker.Breaker) { events.Forget(key) }),
	)
	if err != nil {
		panic(err)
	}
	b, err := hosts.Get("payments.internal")
	if err != nil {
		panic(err)
	}
	fmt.Println(b.Name(), hosts.Len())
	// Output:
	// payments.internal 1
}
