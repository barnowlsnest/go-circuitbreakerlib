# High findings — design

Three production code fixes. No docs-only work in this pass.

## Intent

Make the library safer under custom callbacks, observe real HTTP/gRPC failure modes, and stop leaking sibling-package plumbing through `pkg/breaker`'s public API.

## Goals

1. Custom `Policy.ShouldTrip` / composite `TripFunc` cannot deadlock the breaker by re-entering it.
2. HTTP response-body errors and gRPC stream I/O errors can trip the breaker (library-owned wrappers).
3. `FillEvents`, `ValidateNoPolicies`, `ValidateNoReopenEarly`, `ResolveSettings`, and `Settings` are not part of the public `breaker` API.

## Non-goals

- Composite recovery semantics / operator essays
- `WindowSpec` or `adapter.Option` godoc-only polish
- Sharded windows, metrics, or API redesign beyond the three goals
- Changing default trip thresholds or Open duration behavior

## Anti-goals

- Holding the breaker mutex across user callbacks
- Double-counting one logical call in the window
- Exporting a new “advanced” public surface that replaces the old plumbing

---

## 1. Trip / policy after unlock

### Why

Today `Record` → `evaluate` → `tripped` → `ShouldTrip` runs under `mu`. Composite’s `tripPolicy.ShouldTrip` runs user `TripFunc` there. Re-entry into the same parent deadlocks.

### Behavior

- Snapshot what the policy needs **under the lock** (counts; for HalfOpen closing, whatever freeze input the policy already uses).
- **Unlock**, call `ShouldTrip` (with existing panic recover), collect trip decision + panics.
- **Relock**, re-check epoch / state still match the evaluate path that started the check.
- If still valid and trip is true, perform `openWith` under the lock as today.
- Fire hook panics and state-change events **after** the final unlock (unchanged ordering relative to hooks).

### Composite

**Decided:** do **not** store `tripAggregate` inside `ShouldTrip`. Freeze only after the parent has **actually** entered Open (e.g. `parentObserver` / post-`openWith` path). If an unlocked trip returns true but relock abandons the transition, no freeze is left behind.

`ShouldTrip` / `TripFunc` stay decision-only (plus existing panic recover). Built-in pure policies unchanged aside from the unlock window.

### Spec

Update `docs/spec.md` where it says `ShouldTrip` runs under the breaker lock / “locked section never calls out” — policies are no longer under the lock; Events hooks remain after unlock.

### Tests

- Custom `Policy` that calls `Allow`/`Record` on the same breaker from `ShouldTrip` must complete (no deadlock) under a short test timeout.
- Composite `TripFunc` that calls parent `Allow`/`Pick` must complete the same way.
- Existing trip/freeze/HalfOpen closing tests still pass.

**Builder call:** exact relock validation (which state/epoch fields) follows current `evaluate` branches; keep the critical section as small as the existing transition code.

---

## 2. HTTP body + gRPC stream observation

### Why

`RoundTrip` records at headers; stream interceptor records at stream creation. Body/stream failures never reach the breaker.

### HTTP

- On transport error or classifier failure from headers/err: `Record` immediately (today’s timing).
- On classifier **success** with a non-nil `Body`: do **not** `Record` yet. Wrap `Body` so the **single** `Record` happens when the body ends:
  - terminal read error (not EOF) → classify as failure (same cancel/deadline rules as today via classifier inputs)
  - clean EOF + `Close`, or `Close` after partial success → success
- Elapsed spans headers + body until that terminal point.
- Wrapped body must be safe if the caller never reads and only `Close`s (standard client duty). If the caller abandons the body without `Close`, HalfOpen relies on existing `ProbeTimeout` slot reclaim — do not add a goroutine/timer.
- Panic in caller after headers but before body finish: keep today’s defer failure path for the RoundTrip frame; body wrapper records at most once (idempotent).

**Builder call:** mirror `trackedConn`’s “record once” discipline; prefer a small `trackedBody` type in `pkg/adapter`. Default on — this is the product behavior change, not an opt-in flag, unless tests prove a forced opt-out is required for a specific compatibility case (prefer no flag).

### gRPC stream

- Stream establishment error: `Record` immediately (today).
- Establishment success: return a wrapped `ClientStream`; **single** `Record` on first terminal event:
  - any `SendMsg` / `RecvMsg` / `Header` / `Trailer` error → classify with existing `GRPCClassifier` / cancel rules and `Record`
  - clean end → `Record` Success once: server-stream when `RecvMsg` returns `io.EOF`; client- or bidi-stream after `CloseSend` when the finishing `RecvMsg` returns `io.EOF` (or the call’s normal success finish). Use `StreamDesc` only if needed to distinguish; prefer “first terminal wins” state machine over per-RPC special cases.
- No owned goroutine. Abandoned streams without finish: `ProbeTimeout` reclaim.
- Unary interceptor unchanged.

**Decided (builder call, partner OK’d overall):** Header/Trailer errors are terminal failures; success is clean stream finish, not establishment alone.

### Spec / examples

Update the adapter sections that currently document “headers only” / “establishment only”. Adjust examples/tests accordingly.

### Tests

- HTTP: body read error after 200 → Failure and trips when policy says so.
- HTTP: 200 + full body + Close → one Success.
- gRPC: stream establish OK, later Recv error → Failure.
- gRPC: stream completes cleanly → one Success.
- No double `Record` under Close-after-error.

---

## 3. Hide breaker plumbing from normal users

### Why

`FillEvents`, `ValidateNoPolicies`, `ValidateNoReopenEarly`, `ResolveSettings`, and `Settings` exist only for `registry` / `composite`.

### Approach

Move them to a module-level internal package, e.g. `internal/breakercore` (name up to builder), that:

- exposes the same behaviors to `pkg/registry` and `pkg/composite`
- is **not** importable by external modules

`pkg/breaker` keeps only what first-party breaker users need. Internal helpers may call unexported `breaker` details via functions living in `breaker` that are passed through internal, **or** by putting option factories in `breaker` as unexported and wrapping them in `internal` — builder picks the smallest move that keeps `go test` / fieldalignment happy.

Do **not** leave deprecated public aliases.

### Tests

- `registry` / `composite` still construct correctly with shared events and trip-owned parent policy.
- `pkg/breaker` API tests do not reference the moved symbols (none do today).

---

## Constraints

- Go 1.27; `task sanity` green after each slice.
- Short godoc only on new exported adapter types if any; ask before rewriting existing godoc beyond what’s required by behavior change.
- Spec is authority: update the few lines that become false.
- Stacked PRs preferred (breaker trip unlock → adapters → internal move), each passing sanity.

## Calls made (check while reading)

- Freeze aggregate only when Open really happens; not inside unlocked `ShouldTrip`.
- gRPC: first terminal event records once; Header/Trailer errors count as terminal; success = clean finish, not establish-only.
- HTTP body tracking on by default; no opt-out flag unless tests force one.
- Docs-only High items (composite recovery essay, WindowSpec, adapter.Option reshape) are out of scope.

## Left to the builder

- Exact relock predicates and panic-batching in `evaluate`
- `trackedBody` / stream wrapper method set
- Internal package name and move shape
- Whether HTTP nil/`http.NoBody` skips wrapping
