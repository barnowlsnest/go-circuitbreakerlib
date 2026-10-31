# High Findings Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unlock trip/policy callbacks, observe HTTP body and gRPC stream failures, and hide breaker plumbing used only by registry/composite.

**Architecture:** Refactor `Record`/`evaluate` so `ShouldTrip` runs between unlock and a validated relock; move composite freeze into `parentObserver` on real Open. Wrap successful HTTP bodies and gRPC client streams so each logical call gets one deferred `Record`. Relocate `FillEvents` / `Validate*` / `ResolveSettings` / `Settings` to `internal/breakercore`.

**Tech Stack:** Go 1.27, `pkg/breaker` + `pkg/composite` + `pkg/adapter` + `pkg/registry`, testify suites, `task sanity`.

**Spec:** `docs/superpowers/specs/2026-10-31-high-findings-design.md`

## Global Constraints

- Go version floor: **1.27** (`go.mod`)
- After each task: **`task sanity`** green
- Exported godoc: **one short sentence**; ask before rewriting unrelated existing godoc
- Spec authority: update only lines made false by these changes (`docs/spec.md`)
- No deprecated public aliases for moved breaker helpers
- No new HTTP/gRPC opt-in flags; body/stream tracking is default
- No owned goroutines/timers for abandoned body/stream (use existing `ProbeTimeout`)
- Stacked delivery: one commit series per task below (breaker → adapters → internal)

## Review Focus

- Concurrent `Record` while unlocked `ShouldTrip` runs — relock must abandon stale trip; freeze must not stick without Open
- HTTP caller reads partial body then `Close` — exactly one Success `Record`
- HTTP `Close` after read error — exactly one Failure `Record` (no second Success on Close)
- gRPC `Header()` error before any Recv — Failure, not Success
- HalfOpen probe: abandoned wrapped body/stream without Close — slot freed only via existing probe timeout path

---

### File map

| Path | Role |
|------|------|
| `pkg/breaker/breaker.go` | Unlock/relock around policy evaluation |
| `pkg/breaker/policy.go` / `docs/spec.md` | Spec lines on lock / ShouldTrip |
| `pkg/composite/composite.go` | Pure `tripPolicy`; freeze on Open in `parentObserver` |
| `pkg/adapter/http.go` | Defer Record on success until body end |
| `pkg/adapter/http_body.go` | `trackedBody` (new) |
| `pkg/adapter/grpc.go` | Defer Record until stream terminal |
| `pkg/adapter/grpc_stream.go` | `trackedClientStream` (new) |
| `internal/breakercore/` | Moved FillEvents / Validate* / ResolveSettings / Settings |
| `pkg/registry/*`, `pkg/composite/*` | Import internal helpers |

---

### Task 1: Policy / TripFunc after unlock + composite freeze on Open

**Files:**
- Modify: `pkg/breaker/breaker.go` (`Record`, `evaluate`, `tripped`, `shouldTrip`)
- Modify: `pkg/composite/composite.go` (`tripPolicy`, `parentObserver`)
- Modify: `docs/spec.md` (ShouldTrip-under-lock / locked-section claims)
- Test: `pkg/breaker/breaker_test.go`, `pkg/composite/composite_test.go` (or dedicated new suite methods)

**Interfaces:**
- Consumes: existing `Policy.ShouldTrip(Counts) bool`, `TripFunc`, `parentObserver.OnStateChange`
- Produces: `ShouldTrip` never called while `Breaker.mu` held; `tripAggregate` written only when parent transitions to Open; cleared on Closed (existing)

- [ ] **Step 1: Write failing re-entry tests**

`BreakerSuite.TestPolicyShouldTripMayReenterAllow` — mock/custom `Policy` whose `ShouldTrip` calls `b.Allow()` on the same breaker; `Record` a Closed failure that would consult policies; assert completes within ~1s and does not hang.

`Composite` suite — `TripFunc` that calls `c.Allow()` or `c.Pick`; drive parent trip; assert completes within ~1s.

- [ ] **Step 2: Run tests — expect hang/fail (deadlock or timeout)**

Run: `go test -timeout 3s ./pkg/breaker/ -run TestPolicyShouldTripMayReenterAllow -count=1`  
Expected: FAIL (timeout or deadlock)

Run: matching composite test with `-timeout 3s`  
Expected: FAIL

- [ ] **Step 3: Implement unlock/relock policy check in `breaker.go`**

Approach: under lock, update window; for Closed (and HalfOpen path that consults policies), snapshot `counts` + `epoch` + `state`; unlock; run policy loop with existing panic recover; relock; if `epoch`/state no longer the evaluate candidate, abandon; else `openWith` as today. HalfOpen failure → Open stays fully under lock (no policy). Keep Events hooks after final unlock.

- [ ] **Step 4: Move freeze out of `tripPolicy.ShouldTrip`**

`ShouldTrip`: decision only (use frozen aggregate when parent already HalfOpen; else live `counts`).  
`parentObserver.OnStateChange`: if `ev.To == StateOpen`, store a copy of `c.parent.Counts()` into `tripAggregate`; if `To == StateClosed`, clear (existing).

- [ ] **Step 5: Update `docs/spec.md` lock/ShouldTrip sentences to match**

- [ ] **Step 6: Run focused + composite trip tests**

Run: `go test -race -timeout 60s ./pkg/breaker/ ./pkg/composite/ -count=1`  
Expected: PASS (including re-entry tests; existing freeze/HalfOpen closing tests)

- [ ] **Step 7: `task sanity`**

Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add pkg/breaker/breaker.go pkg/breaker/breaker_test.go \
  pkg/composite/composite.go pkg/composite/composite_test.go docs/spec.md
git commit -m "$(cat <<'EOF'
fix(breaker): evaluate policies outside the breaker lock

Prevents custom Policy/TripFunc re-entry deadlocks; freeze composite
aggregate only when the parent actually opens.
EOF
)"
```

---

### Task 2: HTTP body tracking

**Files:**
- Create: `pkg/adapter/http_body.go` (`trackedBody`)
- Modify: `pkg/adapter/http.go` (`RoundTrip`, godoc on `NewHTTPRoundTripper`)
- Modify: `docs/spec.md` adapter HTTP elapsed/body lines
- Test: `pkg/adapter/http_test.go`

**Interfaces:**
- Consumes: Task 1 unchanged breaker `Record`/`Allow`
- Produces: `trackedBody` wrapping `io.ReadCloser`; records at most once; `RoundTrip` records immediately on transport/header failure; on header success skips wrap for `nil` or `http.NoBody` and records immediately, otherwise returns wrapped body and does not record until body terminal

- [ ] **Step 1: Write failing body tests**

`HTTPSuite.TestBodyReadErrorRecordsFailure` — next returns 200 + body that errors on Read; after `Read`/`Close`, counts show Failure (and can trip with a tight policy if useful).

`HTTPSuite.TestBodySuccessRecordsOnce` — 200 + readable body; after full read + `Close`, `Total==1`, `Successes==1`.

`HTTPSuite.TestBodyCloseAfterErrorRecordsOnce` — read error then `Close`; still one Failure.

Keep/adjust existing tests that use `http.NoBody` (immediate Success still expected).

- [ ] **Step 2: Run — expect FAIL (Success recorded at headers / no Failure on body error)**

Run: `go test ./pkg/adapter/ -run 'TestBody' -count=1`  
Expected: FAIL

- [ ] **Step 3: Implement `trackedBody` + `RoundTrip` deferral**

Record-once via `atomic` or `sync.Once`-style flag. Terminal: non-EOF read error → Failure via `classifyHTTP`; EOF and/or `Close` without prior error → Success. Elapsed from RoundTrip start to terminal. Panic path on RoundTrip frame unchanged when `done` never set before panic; if body already handed out, RoundTrip must not also Record (hand-off clears RoundTrip’s obligation).

- [ ] **Step 4: Spec + godoc one-liners for new behavior**

- [ ] **Step 5: Run adapter HTTP tests + sanity**

Run: `go test -race ./pkg/adapter/ -count=1` then `task sanity`  
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add pkg/adapter/http.go pkg/adapter/http_body.go pkg/adapter/http_test.go docs/spec.md
git commit -m "$(cat <<'EOF'
fix(adapter): record HTTP outcomes after the response body finishes

Body read errors can trip the breaker; headers-only success no longer
finalizes the call when a real body is present.
EOF
)"
```

---

### Task 3: gRPC stream tracking

**Files:**
- Create: `pkg/adapter/grpc_stream.go` (`trackedClientStream`)
- Modify: `pkg/adapter/grpc.go` (`stream` interceptor, godoc)
- Modify: `docs/spec.md` stream-establishment-only lines
- Test: `pkg/adapter/grpc_test.go` (replace `TestStreamRecordsEstablishmentOnly`)

**Interfaces:**
- Consumes: existing `classifyGRPC`, `admit`
- Produces: wrapped `grpc.ClientStream` that `Record`s once on first terminal: SendMsg/RecvMsg/Header/Trailer error → classified Failure/Ignored; clean finish (`RecvMsg` == `io.EOF` after normal completion, including after `CloseSend` for client/bidi) → Success. Establishment error still Records immediately; establishment success does not.

- [ ] **Step 1: Rewrite stream tests**

Replace establishment-only assertion: after successful establish + `Recv` Internal error → `Failures==1` (not Success).

Add clean server-stream success → one Success after Recv EOF.

Add `Header` error path → Failure.

- [ ] **Step 2: Run — expect FAIL on old Success-on-establish behavior**

Run: `go test ./pkg/adapter/ -run 'TestStream' -count=1`  
Expected: FAIL until wrapper lands

- [ ] **Step 3: Implement `trackedClientStream`**

First terminal wins; subsequent methods no-op for recording. No goroutine.

- [ ] **Step 4: Spec/godoc update**

- [ ] **Step 5: `go test -race ./pkg/adapter/` + `task sanity`**

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add pkg/adapter/grpc.go pkg/adapter/grpc_stream.go pkg/adapter/grpc_test.go docs/spec.md
git commit -m "$(cat <<'EOF'
fix(adapter): record gRPC stream outcomes on first terminal I/O

Stream establishment success is no longer treated as the final call
outcome; later Recv/Send/Header/Trailer errors can trip the breaker.
EOF
)"
```

---

### Task 4: Hide breaker plumbing in `internal/breakercore`

**Files:**
- Create: `internal/breakercore/breakercore.go` (or split if fieldalignment/`lll` prefers)
- Modify: `pkg/breaker/option.go` — remove public `FillEvents`, `ValidateNoPolicies`, `ValidateNoReopenEarly`, `ResolveSettings`, `Settings`; keep unexported building blocks as needed
- Modify: `pkg/registry/registry.go`, `pkg/registry/option.go` comments/imports
- Modify: `pkg/composite/composite.go`, `pkg/composite/child.go`, `pkg/composite/option.go`
- Test: existing registry/composite construction tests (no new public breaker API tests)

**Interfaces:**
- Consumes: breaker options / defaults
- Produces: `breakercore.FillEvents(ev breaker.Events) breaker.Option`, `ValidateNoPolicies`, `ValidateNoReopenEarly`, `ResolveSettings(...) (Settings, error)`, `Settings` with same fields as today’s public struct

- [ ] **Step 1: Add internal package + switch registry/composite imports**

Remove symbols from `pkg/breaker` public API in the same change (no aliases).

- [ ] **Step 2: `go test ./pkg/breaker/ ./pkg/registry/ ./pkg/composite/` — PASS**

Confirm external-style compile: `FillEvents` is not addressable as `breaker.FillEvents` (grep + build).

- [ ] **Step 3: `task sanity`**

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add internal/breakercore pkg/breaker/option.go \
  pkg/registry pkg/composite docs/spec.md
git commit -m "$(cat <<'EOF'
refactor: move breaker sibling plumbing behind internal/breakercore

Stops FillEvents/Validate*/ResolveSettings from cluttering the public
breaker API while registry and composite keep the same behavior.
EOF
)"
```

---

## Self-review (plan author)

| Spec goal | Task |
|-----------|------|
| Policy/Trip after unlock + freeze on real Open | Task 1 |
| HTTP body observation | Task 2 |
| gRPC stream observation | Task 3 |
| Hide plumbing | Task 4 |
| Review Focus cases | Task 1 (stale trip/freeze), Task 2 (partial/Close-after-error), Task 3 (Header error), Task 2–3 (probe abandon via existing timeout — covered by not adding timers; optional explicit probe-timeout test only if existing coverage missing) |
