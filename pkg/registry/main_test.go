package registry

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs every test in this package under goleak. The registry never
// starts goroutines.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
