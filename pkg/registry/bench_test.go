package registry

import (
	"strconv"
	"testing"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

const benchKeys = 1024

func benchFactory(string) breaker.Config {
	return breaker.Config{Policies: []breaker.Policy{breaker.ConsecutiveFailures{Count: 1 << 30}}}
}

// BenchmarkGetHit is the hot path: an existing key. It must not allocate.
func BenchmarkGetHit(b *testing.B) {
	r := New(benchFactory)
	if _, err := r.Get("hot"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := r.Get("hot"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetHitParallel hits one key from every P at once; the
// conditional access store keeps the cache line quiet.
func BenchmarkGetHitParallel(b *testing.B) {
	r := New(benchFactory)
	if _, err := r.Get("hot"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := r.Get("hot"); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkGetCreateBounded floods a bounded registry with unique keys so
// every call creates and evicts.
func BenchmarkGetCreateBounded(b *testing.B) {
	r := New(benchFactory, WithMaxEntries(benchKeys))
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if _, err := r.Get(strconv.Itoa(i)); err != nil {
			b.Fatal(err)
		}
		i++
	}
}
