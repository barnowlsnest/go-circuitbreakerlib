package breaker

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

func benchBreaker(b *testing.B, w WindowSpec) *Breaker {
	b.Helper()
	br, err := New(&Config{Name: "bench", Window: w})
	if err != nil {
		b.Fatal(err)
	}
	return br
}

func BenchmarkAllowRecordClosedCount(b *testing.B) {
	br := benchBreaker(b, SlidingCountWindow{Size: 100})
	b.ReportAllocs()
	for b.Loop() {
		p, _ := br.Allow()
		br.Record(p, OutcomeSuccess, time.Millisecond)
	}
}

func BenchmarkAllowRecordClosedTime(b *testing.B) {
	br := benchBreaker(b, SlidingTimeWindow{Size: 10 * time.Second, Buckets: 10})
	b.ReportAllocs()
	for b.Loop() {
		p, _ := br.Allow()
		br.Record(p, OutcomeSuccess, time.Millisecond)
	}
}

func BenchmarkAllowOpen(b *testing.B) {
	br := benchBreaker(b, nil)
	br.ForceOpen()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = br.Allow()
	}
}

// benchParallel runs exactly n goroutines sharing b.N iterations.
func benchParallel(b *testing.B, n int) {
	b.Helper()
	br := benchBreaker(b, nil)
	ctx := context.Background()
	fn := func(context.Context) error { return nil }
	b.ReportAllocs()
	b.ReportMetric(float64(runtime.GOMAXPROCS(0)), "gomaxprocs")
	b.ResetTimer()
	var wg sync.WaitGroup
	per := b.N / n
	for i := range n {
		iters := per
		if i == n-1 {
			iters = b.N - per*(n-1)
		}
		wg.Go(func() {
			for range iters {
				_ = br.Execute(ctx, fn)
			}
		})
	}
	wg.Wait()
}

func BenchmarkExecuteParallel8(b *testing.B)  { benchParallel(b, 8) }
func BenchmarkExecuteParallel32(b *testing.B) { benchParallel(b, 32) }
