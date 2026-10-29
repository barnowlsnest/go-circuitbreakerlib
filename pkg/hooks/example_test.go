package hooks_test

import (
	"fmt"
	"os"
	"time"

	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/hooks"
)

func ExampleNewLogEvents() {
	// Log lines carry timestamps, so they go to stderr here to keep the
	// example's output stable. In a program use the logger you already have.
	log := logger.New(logger.Config{
		Level:  logger.InfoLevel,
		Format: logger.TextFormat,
		Output: os.Stderr,
	})
	events, err := hooks.NewLogEvents(log,
		hooks.WithRejectEvery(5*time.Second),
		hooks.WithFailureEvery(time.Second),
	)
	if err != nil {
		panic(err)
	}
	b, err := breaker.New(&breaker.Config{
		Name:     "payments",
		Policies: []breaker.Policy{breaker.ConsecutiveFailures{Count: 1}},
		Events:   events,
	})
	if err != nil {
		panic(err)
	}
	p, _ := b.Allow()
	b.Record(p, breaker.OutcomeFailure, 0)
	_, err = b.Allow()
	fmt.Println(err)
	// Output:
	// circuit breaker payments is open: circuit breaker is open
}
