package composite

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs every test in this package under goleak. A composite never
// owns a goroutine.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
