## Fix Report: remove unsafe breakercore bridge

- Replaced the `go:linkname` bridge with an ordinary `internal/breakercore` option surface.
- `pkg/breaker` now aliases `breakercore.Option` and keeps public `With*` constructors as typed wrappers.
- `pkg/registry` and `pkg/composite` continue to use `breakercore` for `FillEvents`, validation, and settings resolution.
- Confirmed `breaker.FillEvents`, `breaker.ValidateNoPolicies`, `breaker.ValidateNoReopenEarly`, `breaker.ResolveSettings`, and `breaker.Settings` remain absent from public docs.
- Confirmed no `unsafe` or `go:linkname` references remain.

Verification:
- `go test ./pkg/breaker/ ./pkg/registry/ ./pkg/composite/ ./pkg/adapter/ ./pkg/hooks/` passed.
- `task sanity` passed.
