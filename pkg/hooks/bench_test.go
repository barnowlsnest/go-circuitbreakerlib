package hooks

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

const benchBufferSize = 64 << 10

var errBench = errors.New("bench")

func benchBreaker(b *testing.B, level logger.Level, out io.Writer) *breaker.Breaker {
	b.Helper()
	log := logger.New(logger.Config{
		Level:      level,
		Format:     logger.JSONFormat,
		Output:     out,
		BufferSize: benchBufferSize,
	})
	ev, err := NewLogEvents(log)
	if err != nil {
		b.Fatal(err)
	}
	br, err := breaker.New(
		breaker.WithName("bench"),
		breaker.WithPolicies(breaker.ConsecutiveFailures{Count: 1 << 30}),
		breaker.WithEvents(ev),
	)
	if err != nil {
		b.Fatal(err)
	}
	return br
}

func benchAllowRecord(b *testing.B, br *breaker.Breaker, outcome breaker.Outcome) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		p, err := br.Allow()
		if err != nil {
			b.Fatal(err)
		}
		br.Record(p, outcome, 0)
	}
}

// BenchmarkAllowRecordLogInfo is the hot path with successes filtered out
// by the logger level. It must not allocate.
func BenchmarkAllowRecordLogInfo(b *testing.B) {
	benchAllowRecord(b, benchBreaker(b, logger.InfoLevel, io.Discard), breaker.OutcomeSuccess)
}

// BenchmarkAllowRecordLogDebugTempFile logs every success to a temp file.
func BenchmarkAllowRecordLogDebugTempFile(b *testing.B) {
	f, err := os.CreateTemp(b.TempDir(), "bench-*.log")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	benchAllowRecord(b, benchBreaker(b, logger.DebugLevel, f), breaker.OutcomeSuccess)
}

// BenchmarkAllowRecordLogWarnAllFailures records only failures; the limiter
// drops all but one line per second.
func BenchmarkAllowRecordLogWarnAllFailures(b *testing.B) {
	br := benchBreaker(b, logger.WarnLevel, io.Discard)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = br.Execute(context.Background(), func(context.Context) error { return errBench })
	}
}
