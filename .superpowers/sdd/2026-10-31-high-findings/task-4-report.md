## Fix Report: defer Goal 3 hide

- Reverted Task 4 commits `b00b542` and `1759237`.
- Restored `breaker.FillEvents`, `breaker.ValidateNoPolicies`, `breaker.ValidateNoReopenEarly`, `breaker.ResolveSettings`, and `breaker.Settings`.
- Restored registry and composite to use the public breaker helpers.
- Deleted `internal/breakercore`.
- Preserved `pkg/breaker` as stdlib-only.
- Confirmed no `unsafe`, `go:linkname`, or `breakercore` references remain.

Ruling:
- Goal 3 true hide is deferred because `breaker.Option` is `func(*options) error`, `options` is unexported, and `pkg/breaker` must remain stdlib-only.

Verification:
- `task sanity` passed.
