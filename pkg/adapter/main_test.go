package adapter

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs every test in this package under goleak. Adapters never
// start goroutines; httptest servers are closed by the tests that start
// them.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
	)
}
