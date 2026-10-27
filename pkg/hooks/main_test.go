package hooks

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs every test in this package under goleak. LogEvents and
// MultiEvents never start goroutines.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
