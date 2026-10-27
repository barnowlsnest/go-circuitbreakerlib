package composite_test

import (
	"fmt"
	"time"

	"github.com/barnowlsnest/go-datalib/v5/pkg/tree"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/composite"
)

func ExampleFromHierarchy() {
	// A dependency with two regions, each with its own hosts.
	model := tree.HierarchyModel{
		tree.RootTag: {"payments"},
		"payments":   {"eu-west", "us-east"},
		"eu-west":    {"10.0.1.5:443", "10.0.1.6:443"},
		"us-east":    {"10.0.2.5:443"},
	}
	root, err := composite.FromHierarchy(model, 16,
		func(path []string) composite.Config {
			return composite.Config{Trip: composite.OpenChildrenRatio(0.5)}
		},
		func(path []string) breaker.Config {
			return breaker.Config{
				Policies:     []breaker.Policy{breaker.ConsecutiveFailures{Count: 3}},
				OpenDuration: 10 * time.Second,
			}
		},
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(root.Name(), len(root.Children()))
	// Output:
	// payments 2
}
