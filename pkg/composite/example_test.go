package composite_test

import (
	"context"
	"fmt"
	"time"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/composite"
)

func ExampleComposite() {
	// One breaker per instance of a dependency, and a parent that opens when
	// half of the instances are down.
	trip, err := composite.OpenChildrenRatio(0.5)
	if err != nil {
		panic(err)
	}
	dep, err := composite.New(
		composite.WithName("search"),
		composite.WithTrip(trip),
		composite.WithChild(
			breaker.WithPolicies(breaker.ConsecutiveFailures{Count: 3}),
			breaker.WithOpenDuration(10*time.Second),
		),
	)
	if err != nil {
		panic(err)
	}
	for _, instance := range []string{"10.0.0.1:9200", "10.0.0.2:9200"} {
		if _, err := dep.AddChild(instance); err != nil {
			panic(err)
		}
	}

	// Execute picks a healthy instance and records the outcome on it and on
	// the parent.
	err = dep.Execute(context.Background(), func(ctx context.Context, instance string) error {
		fmt.Println("calling", instance)
		return nil
	})
	fmt.Println(err, dep.State())
	// Output:
	// calling 10.0.0.1:9200
	// <nil> closed
}
