package composite

import (
	"strconv"
	"testing"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

const benchChildren = 10

func benchComposite(b *testing.B) *Composite {
	b.Helper()
	c, err := New(Config{
		Name: "bench",
		ChildTemplate: func(string) breaker.Config {
			return breaker.Config{Policies: []breaker.Policy{breaker.ConsecutiveFailures{Count: 1 << 30}}}
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for i := range benchChildren {
		if _, err := c.AddChild(strconv.Itoa(i)); err != nil {
			b.Fatal(err)
		}
	}
	return c
}

// BenchmarkPickRecord10Children is the hot path with every child healthy.
// It must not allocate.
func BenchmarkPickRecord10Children(b *testing.B) {
	c := benchComposite(b)
	b.ReportAllocs()
	for b.Loop() {
		l, err := c.Pick()
		if err != nil {
			b.Fatal(err)
		}
		c.RecordLease(l, breaker.OutcomeSuccess, 0)
	}
}

// BenchmarkPickRecordParallel runs the same path from every P at once.
func BenchmarkPickRecordParallel(b *testing.B) {
	c := benchComposite(b)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l, err := c.Pick()
			if err != nil {
				b.Fatal(err)
			}
			c.RecordLease(l, breaker.OutcomeSuccess, 0)
		}
	})
}
