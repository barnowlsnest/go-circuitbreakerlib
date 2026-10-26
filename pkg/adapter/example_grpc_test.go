package adapter_test

import (
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/adapter"
	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

func ExampleNewGRPCUnaryInterceptor() {
	b, err := breaker.New(breaker.Config{
		Name:         "inventory",
		Policies:     []breaker.Policy{breaker.FailureRate{Threshold: 0.5, MinCalls: 20}},
		OpenDuration: 30 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	// FailedPrecondition is not retried by a service-config retry policy,
	// so a rejected call does not re-enter the interceptor while Open.
	unary, err := adapter.NewGRPCUnaryInterceptor(
		adapter.WithBreaker(b),
		adapter.WithGRPCRejectCode(codes.FailedPrecondition),
	)
	if err != nil {
		panic(err)
	}
	cc, err := grpc.NewClient("dns:///inventory.internal:443",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(unary),
	)
	if err != nil {
		panic(err)
	}
	defer cc.Close()
	fmt.Println(cc.Target())
	// Output:
	// dns:///inventory.internal:443
}
