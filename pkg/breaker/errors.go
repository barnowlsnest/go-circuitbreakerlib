package breaker

import "errors"

// ErrInvalidConfig is joined with a specific sentinel by New when a Config
// is unusable, so errors.Is matches both.
var ErrInvalidConfig = errors.New("invalid breaker configuration")
