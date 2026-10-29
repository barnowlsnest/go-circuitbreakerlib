package composite

import (
	"strconv"
	"testing"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

func BenchmarkPickHit(b *testing.B) {
	c, err := New(
		WithName("bench"),
		WithChild(breaker.WithPolicies(breaker.ConsecutiveFailures{Count: 1 << 30})),
	)
	if err != nil {
		b.Fatal(err)
	}
	for i := range 8 {
		if _, err := c.AddChild(strconv.Itoa(i)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		l, err := c.Pick()
		if err != nil {
			b.Fatal(err)
		}
		c.RecordLease(l, breaker.OutcomeSuccess, 0)
	}
}
