package breaker

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs every test in this package under goleak. A breaker never
// owns a goroutine, so any leak points at a test helper that did not stop.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
