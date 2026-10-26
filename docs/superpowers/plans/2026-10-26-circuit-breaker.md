# Circuit Breaker Library Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `github.com/barnowlsnest/go-circuitbreakerlib`: a zero-dependency core breaker, logging hooks, keyed registry, nestable composite, and HTTP/gRPC/TCP adapters, delivered as a stack of GitHub PRs.

**Architecture:** `pkg/breaker` holds the whole state machine (packed atomic state+epoch, permits, sliding windows, policies, half-open slots, two generic hooks for composites). Everything else is thin: `pkg/hooks` logs through go-logslib with `x/time/rate` limiters, `pkg/registry` keys breakers with sampled eviction, `pkg/composite` nests breakers behind `Lease`, and `pkg/adapter` wraps the three protocols.

**Tech Stack:** Go 1.27, stdlib, `golang.org/x/time/rate`, `github.com/barnowlsnest/go-logslib/v2` (≥ v2.4.0), `github.com/barnowlsnest/go-datalib/v5/pkg/tree`, `google.golang.org/grpc`. Tests: `testify/suite`, `testify/mock`, `go.uber.org/goleak`. Tooling: Task, golangci-lint v2, `gh`.

**Spec:** `docs/superpowers/specs/2026-10-26-circuit-breaker-design.md`

## Global Constraints

- Module path `github.com/barnowlsnest/go-circuitbreakerlib`, `go 1.27`, single `go.mod`.
- `pkg/breaker` imports stdlib only. `pkg/hooks` is the only importer of go-logslib and `x/time/rate`. `pkg/adapter` is the only importer of grpc. `pkg/composite` is the only importer of go-datalib.
- `.golangci.yaml` is `go-asynctasklib/.golangci.yaml` verbatim with the `goimports` local prefix changed to this module and `govet: { enable: [fieldalignment] }` added under `linters.settings`. lll 140, funlen 100/50, gocyclo 15, dupl 100, goconst min-occurrences 2.
- Struct field order is whatever `fieldalignment` dictates; `golangci-lint run --fix` applies it. Never hand-order fields. One field per line, no size comments.
- Dependencies are added by the PR that first imports them (go-logslib in PR 3, go-datalib in PR 5, grpc in PR 7); `go mod tidy` runs first in `task sanity`, so nothing is pre-pinned.
- Every `*_test.go` that contains tests uses `testify/suite`. Exceptions, which hold no suite: `main_test.go` (goleak `TestMain`), `fuzz_test.go`, `bench_test.go`, `export_test.go`, `example_test.go`.
- Doubles: interface doubles are `testify/mock` mocks named `Mock<Interface>` with `AssertExpectations` in `TearDownTest`; hand-written helpers (clock, counters) are named `test...`. Table-driven subtests with `expected` fields; named consts, no magic numbers; no `time.Sleep` for state transitions; injected clocks everywhere.
- `TestMain` with `goleak.VerifyTestMain(m)` in `breaker`, `hooks`, `registry`, `composite`, `adapter`.
- Godoc on every exported identifier. Every package has `example_test.go` with `Example*` functions; README code is copied from them.
- `task sanity` green before every commit and before every PR. `task go-cover` enforces 80% total coverage and runs in CI.
- Commits: conventional prefixes, ending with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. PR bodies end with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.
- Each PR is opened with `gh pr create --base <previous branch>` as soon as its branch is green. Each PR body links the PR below it.

## Review Focus

1. **A `Record` after `Reset()`** with a permit from before the reset must be discarded (epoch moved). Test in Task 5.
2. **`HalfOpenMaxCalls` larger than concurrent probe traffic** with one leaked slot must still reopen at `ProbeTimeout`. Test in Task 7.
3. **A `SlidingTimeWindow` idle for longer than `Size`** must report `Counts.Total == 0` from `Counts()` without a `Record`. Test in Task 3.
4. **`Composite.Pick` when every child is a nested composite whose leaves are all Open** must release every intermediate permit and return `ErrNoHealthyChildren` with in-flight 0 on every level. Test in Task 12.
5. **HTTP adapter when `next.RoundTrip` returns both a non-nil response and an error** (allowed by the `RoundTripper` contract on some transports) must classify as Failure and not leak the body. Test in Task 14.

---

## PR 0 — go-logslib `Logger.Level()`

### Task 1: Add `Level()` to go-logslib and tag v2.4.0

**Files:**
- Modify: `/Users/dshlychkou/barnowlsnest/go-logslib/pkg/logger/logger.go` (after `New`)
- Test: `/Users/dshlychkou/barnowlsnest/go-logslib/pkg/logger/logger_test.go`
- Modify: `/Users/dshlychkou/barnowlsnest/go-logslib/README.md` (Configuration → Log Levels), `CLAUDE.md` (latest tag line)

**Interfaces:**
- Produces: `func (l *Logger) Level() Level` returning `l.config.Level`.

- [ ] **Step 1:** In go-logslib, `git checkout -b feat/logger-level main`.
- [ ] **Step 2:** Write the failing test `TestLoggerLevel` (table: Debug, Info, Warn, Error; `expected` equals the configured level).
- [ ] **Step 3:** Run `go test ./pkg/logger -run TestLoggerLevel`. Expected: FAIL, undefined method.
- [ ] **Step 4:** Implement `Level()` with godoc. Add one README line under Log Levels: `log.Level()` returns the configured level.
- [ ] **Step 5:** `task sanity`. Expected: green, benchmarks unchanged.
- [ ] **Step 6:** Commit `feat: expose Logger.Level()`. Push. `gh pr create --base main --title "feat: expose Logger.Level()"`.
- [ ] **Step 7:** After merge: `git tag v2.4.0 && git push origin v2.4.0`. Verify with `GOPROXY=direct go list -m github.com/barnowlsnest/go-logslib/v2@v2.4.0` (the public proxy can lag by minutes).

---

## PR 1 — Scaffold (`feat/scaffold`)

### Task 2: Repository scaffold

**Files:**
- Create: `Taskfile.yaml`, `.golangci.yaml`, `.github/workflows/build.yml`, `.github/workflows/golangci-lint.yml`, `CLAUDE.md`, `README.md` (skeleton), `pkg/breaker/doc.go`
- Modify: `go.mod` (add `require` lines after `go get`), `.gitignore` (add `.idea/`)
- Delete: nothing (`main.go` does not exist)

**Interfaces:**
- Produces: `task sanity`, `task go-test`, `task go-build`, `task go-lint`, `task go-bench` (`go test -bench=. -benchmem -run=^$ ./...`), `task go-cover` (`go test -coverprofile=coverage.out ./... && go tool cover -func=coverage.out | tail -1` piped through an awk check that fails below 80.0).

- [ ] **Step 1:** `git checkout -b feat/scaffold main`.
- [ ] **Step 2:** Copy `Taskfile.yaml` from go-asynctasklib; set `MODULE_PATH`; add `go-bench` and `go-cover` tasks; `go-test` runs `go test -v -race -cover ./...`; `sanity` runs `go-cover` after `go-test`.
- [ ] **Step 3:** Copy `.golangci.yaml` from go-asynctasklib; change local prefix; add under `linters.settings`: `govet: { enable: [fieldalignment] }`.
- [ ] **Step 4:** Copy both workflows from go-asynctasklib; add a `Coverage` step running `task go-cover` to `build.yml`.
- [ ] **Step 5:** No dependencies are added here. `testify` and `goleak` arrive with the first tests in PR 2a.
- [ ] **Step 6:** Write `pkg/breaker/doc.go` with the package overview (three states, permits, windows, policies, hooks) so `go build ./...` has a package.
- [ ] **Step 7:** Write `CLAUDE.md` in go-asynctasklib's structure: project, layout table (the five packages), commands, CI, conventions (lint limits, errors, contexts, tests, field order), dependencies, stacked-PR workflow.
- [ ] **Step 8:** Write README skeleton: title, badges (same four as go-logslib), one-paragraph description, install line, "Status: v0, API may change", section headers for Quick start / Core / Hooks / Registry / Composite / Adapters / Benchmarks (empty).
- [ ] **Step 9:** `task sanity`. Expected: green (no tests yet is acceptable for this PR only).
- [ ] **Step 10:** Commit `chore: scaffold module, tooling, CI`. Push. `gh pr create --base main --title "chore: scaffold"`.

---

## PR 2a — Core types (`feat/breaker-types`)

Branch from `feat/scaffold`. Tasks 3 and 4. Files in `pkg/breaker`: `outcome.go`, `window.go`, `window_count.go`, `window_time.go`, `policy.go`, `backoff.go`, `errors.go`, `events.go`, `config.go`, `snapshot.go`. Tests mirror file names; shared mocks in `mocks_test.go`; `main_test.go` for goleak. PR opened at the end of Task 4 with `gh pr create --base feat/scaffold`.

## PR 2b — Core state machine (`feat/breaker-core`)

Branch from `feat/breaker-types`. Tasks 5, 6, 7. Files: `state.go`, `permit.go`, `breaker.go`, `execute.go`, `fuzz_test.go`, `bench_test.go`, `example_test.go`.

### Task 3: Sliding windows

**Files:**
- Create: `pkg/breaker/outcome.go`, `window.go`, `window_count.go`, `window_time.go`, `main_test.go`, `window_test.go`

**Interfaces:**
- Produces:
  ```go
  type Outcome uint8 // OutcomeSuccess, OutcomeFailure, OutcomeIgnored
  type Counts struct{ Total, Successes, Failures, Slow, ConsecutiveFailures, ConsecutiveSuccesses uint32 } // one per line in code
  // WindowSpec is what Config.Window holds; the two exported structs implement it.
  type WindowSpec interface {
      validate() error
      newWindow() window
  }
  // window is the unexported runtime instance.
  type window interface {
      record(now time.Time, outcome Outcome, slow bool) // outcome is never OutcomeIgnored
      counts(now time.Time) Counts                      // read-only; excludes expired data
      reset()
  }
  type SlidingCountWindow struct{ Size int }
  type SlidingTimeWindow struct{ Size time.Duration; Buckets int }
  ```
  Consecutive counters live in the window implementation and are included in `Counts`.

- [ ] **Step 1:** `git checkout -b feat/breaker-types feat/scaffold`. Add `main_test.go` with goleak `TestMain` (`go get go.uber.org/goleak github.com/stretchr/testify`).
- [ ] **Step 2:** Write `WindowSuite` tests (table-driven over both window kinds where the behaviour is shared):
  - `TestCountWindowEvictsOldest`: Size 3; record F,F,S,S → `Failures==1, Total==3`.
  - `TestCountWindowConsecutive`: F,F,S → `ConsecutiveFailures==0, ConsecutiveSuccesses==1`; F,F → `ConsecutiveFailures==2`.
  - `TestTimeWindowExpiresBuckets`: Size 10s, Buckets 10; record F at t0; `counts(t0+11s).Total==0` **without** a record (Review Focus 3).
  - `TestTimeWindowPartialExpiry`: F at t0, F at t0+5s; `counts(t0+10s+1ms)` → `Failures==1`.
  - `TestWindowSlowCounted`: record S with slow=true → `Slow==1, Successes==1`.
  - `TestWindowReset`: after reset all zero.
  - `TestWindowValidate`: Size 0 count window → error; Buckets 0 → defaults to 10 (no error); Size<0 → error.
- [ ] **Step 3:** Run `go test ./pkg/breaker -run WindowSuite`. Expected: FAIL, undefined.
- [ ] **Step 4:** Implement. Count window: ring `[]uint8` of outcome kinds + running totals. Time window: `[]bucket{total,failures,slow uint32}` plus `start time.Time` and `head int`; `advance(now)` zeroes buckets older than `Size`; `counts(now)` sums buckets whose span ended within `Size` of `now` without mutating.
- [ ] **Step 5:** Run suite. Expected: PASS. `task sanity`.
- [ ] **Step 6:** Commit `feat(breaker): sliding count and time windows`.

### Task 4: Policies, backoff, errors, events, config

**Files:**
- Create: `pkg/breaker/policy.go`, `backoff.go`, `errors.go`, `events.go`, `config.go`, `mocks_test.go`, `policy_test.go`, `backoff_test.go`, `config_test.go`

**Interfaces:**
- Produces, exactly as in the spec sections Policies, Backoff, Errors, Events, Config:
  `Policy` interface; `FailureRate`, `ConsecutiveFailures`, `SlowCallRate`, `FailureCount` structs; `Backoff` interface; `ConstantBackoff(d) Backoff`; `ExponentialBackoff{Base, Max, Jitter}`; sentinels `ErrInvalidConfig`, `ErrEmptyName`, `ErrOpen`, `ErrTooManyProbes`, `ErrNilFunc`; `RejectedError`; `Reason` consts; `Events`, `NoopEvents`, `NewNoopEvents`, `StateChangeEvent`, `CallEvent`, `RejectEvent`; `Config` (with `Window WindowSpec` and `ReopenEarly func() bool`); `Snapshot`; `Classifier`; `DefaultClassifier`.
  Internal: `func (c Config) withDefaults() (Config, error)`; defaults: window `SlidingTimeWindow{10s,10}`, policies `FailureRate{0.5,20}, ConsecutiveFailures{5}`, `OpenDuration` 30s, `ProbeTimeout` 60s, `HalfOpenMaxCalls` 1, `HalfOpenSuccesses` = `HalfOpenMaxCalls`, `Classifier` = `DefaultClassifier`, `Events` = `NoopEvents`, `Clock` = `time.Now`, `Backoff` = `ConstantBackoff(OpenDuration)`.
- `mocks_test.go`: `MockEvents`, `MockPolicy`, `MockBackoff` (testify/mock).

- [ ] **Step 1:** Write tests:
  - `PolicySuite`: table per policy with `Counts` input and `expected bool`; include `MinCalls` boundary (Total == MinCalls trips, MinCalls-1 does not) and `Name()` strings `"failure-rate"`, `"consecutive-failures"`, `"slow-call-rate"`, `"failure-count"`.
  - `BackoffSuite`: `ConstantBackoff(30s).Delay(5)==30s`; exponential Base 1s Max 8s: attempts 0..4 → 1s,2s,4s,8s,8s; with Jitter the result is in `[0, cap]`; negative attempt → 0.
  - `ConfigSuite`: empty name → `errors.Is(err, ErrInvalidConfig) && errors.Is(err, ErrEmptyName)` and message is `"invalid breaker configuration\nname is empty"` (errors.Join); `FailureRate{Threshold: 1.5}` → `ErrInvalidConfig`; defaults applied as listed; `HalfOpenMaxCalls: 1, HalfOpenSuccesses: 3` is **valid** (concurrency and count are independent).
  - `ClassifierSuite` for `DefaultClassifier`: cancelled ctx → Ignored; deadline-exceeded ctx with `context.DeadlineExceeded` err → Failure; live ctx nil err → Success; live ctx any err → Failure.
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement. Jitter uses `math/rand/v2`. `RejectedError.Error()` returns `"circuit breaker <name> is <state>: <err>"`.
- [ ] **Step 4:** Run. PASS. `task sanity`.
- [ ] **Step 5:** Add `example_test.go` with `ExampleConfig` (construct a config, print defaults applied). Commit `feat(breaker): policies, backoff, errors, events, config`. Push. `gh pr create --base feat/scaffold --title "feat(breaker): core types"`.

### Task 5: State machine — Closed/Open/HalfOpen with permits

**Files:**
- Create: `pkg/breaker/state.go`, `permit.go`, `breaker.go`, `snapshot.go`, `breaker_test.go`

**Interfaces:**
- Produces:
  ```go
  type State uint32 // StateClosed, StateOpen, StateHalfOpen; String()
  type Permit struct{ epoch uint64; slot uint32 }; func (p Permit) IsProbe() bool
  type Breaker struct{...}
  func New(cfg Config) (*Breaker, error)
  func (b *Breaker) Allow() (Permit, error)
  func (b *Breaker) Record(p Permit, outcome Outcome, elapsed time.Duration)
  func (b *Breaker) Name() string; State() State; Counts() Counts; Snapshot() Snapshot; HookPanics() uint64
  func (b *Breaker) ForceOpen(); ForceClose(); Reset()
  ```
  Internal layout: `packed atomic.Uint64` (`epoch<<2 | state`, epoch starts at 1), `reopenAtNanos atomic.Int64` (relative to `base time.Time` captured in `New`), `forced atomic.Bool`, `inFlight atomic.Int32`, `probeAdmitted []int64` (len `HalfOpenMaxCalls`; stores relative nanos **+1** so that 0 means free even when a probe is admitted at the base instant, which every injected-clock test does), `mu sync.Mutex`, `window Window`, `attempt int`, `openRejected, halfOpenRejected *RejectedError` (pre-allocated), `hookPanics atomic.Uint64`.
  Transition helper: `func (b *Breaker) transition(to State, reason Reason, now time.Time) StateChangeEvent` called under `mu`; fires `OnStateChange` after unlock.

- [ ] **Step 1:** `git checkout -b feat/breaker-core feat/breaker-types`. Write `BreakerSuite` with an injected `testClock` (`now time.Time; advance(d)`) and `MockEvents` with `.On("OnStateChange", mock.MatchedBy(...))` assertions on `From/To/Reason/Attempt`:
  - `TestNewDefaults`: `State()==StateClosed`, `Snapshot().Epoch==1`.
  - `TestClosedAllowReturnsPermit`: `Allow()` nil error, `!p.IsProbe()`.
  - `TestTripsOnConsecutiveFailures`: policies `ConsecutiveFailures{3}`; 3× Allow+Record(Failure) → Open; event `Reason=="consecutive-failures"`, `Attempt==0`; 4th `Allow` → `errors.Is(err, ErrOpen)`, `errors.As(&RejectedError)`, and the two `*RejectedError` pointers from two rejections are identical (pre-allocated).
  - `TestOpenToHalfOpenAfterBackoff`: OpenDuration 30s; at +29s `Allow` rejected; at +30s `Allow` returns `p.IsProbe()==true`, state HalfOpen, event `Reason==ReasonTimeout`.
  - `TestBackoffCalledOncePerOpen`: `MockBackoff.On("Delay", 0).Return(30s).Once()`; 100 rejected `Allow`s; `AssertNumberOfCalls(…,"Delay",1)`.
  - `TestHalfOpenProbeLimit`: `HalfOpenMaxCalls=1`; second `Allow` → `ErrTooManyProbes`, same pre-allocated pointer each time.
  - `TestHalfOpenSuccessCloses`: `HalfOpenSuccesses=2, HalfOpenMaxCalls=2`; two probes Success → Closed, `Reason==ReasonHalfOpenSuccess`, `Snapshot().Attempt==0`, `Counts().Total==0` (window cleared), `InFlight==0`.
  - `TestHalfOpenFailureReopens`: probe Failure → Open, `Attempt==1`, `Reason==ReasonHalfOpenFailure`; `MockBackoff` called with 1.
  - `TestStalePermitDiscarded`: permit from Closed; trip; advance to HalfOpen; `Record(stale, Failure)` → still HalfOpen, `InFlight` unchanged, `MockEvents` `AssertNotCalled("OnCall")`.
  - `TestRecordAfterResetDiscarded` (Review Focus 1): permit; `Reset()`; `Record(p, Failure, 0)` → `Counts().Failures==0`.
  - `TestIgnoredReleasesProbeSlot`: HalfOpen; probe permit; `Record(p, Ignored)` → `InFlight==0`, state HalfOpen, no `OnCall`.
  - `TestTwoSlotRelease`: `HalfOpenMaxCalls=2, HalfOpenSuccesses=1`; p1 Ignored, p2 Success → Closed, both slots free (`Snapshot().InFlight==0`).
  - `TestForceOpenPins`: `ForceOpen()` → Open, `Forced`; advance 1h; `Allow` still rejected; `ForceClose()` → Closed, 100 failures do not trip; `Reset()` → unpinned and trips again.
  - `TestReopenEarly`: `ReopenEarly func() bool` returns true → next `Allow` while Open gives a probe, `Reason==ReasonEarlyHalfOpen`; not consulted when forced (closure counts calls).
  - `TestReopenEarlyPanicIsFalse`: panicking `ReopenEarly` → stays Open, `OnHookPanic` called once.
  - `TestClosingRecordPolicyTrips`: `SlowCallRate{0.5,1}`, `SlowCallDuration=10ms`, `HalfOpenSuccesses=1`; probe Success with `elapsed=20ms` → Open directly, `Reason=="slow-call-rate"`, `Attempt==1`, no Closed event.
  - `TestPolicyPanicCountsFailure`: `MockPolicy` panics → outcome recorded as Failure, `OnHookPanic` called, caller does not panic.
  - `TestHookPanicSwallowed`: `MockEvents.OnCall` panics → `HookPanics()==1`.
  - `TestLazyStateView`: Open; advance past `reopenAt`; `State()==StateHalfOpen` but `Snapshot().Epoch` unchanged and no event; then `Allow` performs the real transition.
  - `TestOpenToHalfOpenCAS`: 64 goroutines call `Allow` after backoff; exactly one gets `IsProbe()`; the rest get `ErrTooManyProbes`; exactly one state-change event.
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement `Allow`:
  1. `w := packed.Load()`; `st := State(w & 3)`; `ep := w >> 2`.
  2. Closed: return `Permit{ep,0}, nil`.
  3. Open: if `!forced` and `ReopenEarly != nil` and the recovered `ReopenEarly()` call returns true → lock, re-check state, `transition(HalfOpen, ReasonEarlyHalfOpen)`, take slot 0, unlock, fire, return probe. Else if `now >= reopenAt` → same with `ReasonTimeout`. Else `OnReject`, return `openRejected`. No lock is taken on the rejection path.
  4. HalfOpen: lock; expire any `probeAdmitted[i]` older than `ProbeTimeout` → `transition(Open, ReasonProbeTimeout)` with `attempt++`, clear slots, unlock, fire, return `openRejected`; else find a free slot, set it to now, `inFlight++`, return `Permit{ep, i+1}`; none free → `halfOpenRejected`.
  Implement `Record`: fast path (epoch mismatch, or `Ignored && slot==0`) returns; lock; HalfOpen + probe: free slot, `inFlight--`; Ignored → unlock, return. Window record. Then by state:
  - Closed: run policies (each under recover); first trip → `attempt=0`, `reopenAt = now + Backoff.Delay(0)`, `transition(Open, Reason(policy.Name()))`.
  - HalfOpen, Failure → `attempt++`, `reopenAt = now + Delay(attempt)`, clear slots, `transition(Open, ReasonHalfOpenFailure)`.
  - HalfOpen, Success and successes reached `HalfOpenSuccesses` → run policies once on the current (pre-clear) counts; if one trips → `attempt++`, `transition(Open, Reason(policy.Name()))`; else `attempt=0`, window reset, `transition(Closed, ReasonHalfOpenSuccess)`.
  Unlock; fire `OnCall` then `OnStateChange`.
- [ ] **Step 4:** Run. PASS. `task sanity`.
- [ ] **Step 5:** Commit `feat(breaker): state machine with permits and half-open slots`.

### Task 6: `Execute` and `Do`

**Files:**
- Create: `pkg/breaker/execute.go`, `execute_test.go`

**Interfaces:**
- Produces: `func (b *Breaker) Execute(ctx, fn func(ctx) error) error`; `func Do[T any](ctx, b, fn func(ctx) (T, error)) (T, error)`.

- [ ] **Step 1:** Tests: cancelled ctx → returns `ctx.Err()`, `fn` not called, nothing recorded; nil fn → `ErrNilFunc`; Open → `*RejectedError`, fn not called; fn error passes through unchanged (`errors.Is` on a custom sentinel); fn panic → recorded Failure (`Counts().Failures==1`) and the panic propagates (`assert.Panics`); elapsed is measured (clock advanced inside fn → `CallEvent.Elapsed` matches); `Do` returns the value.
- [ ] **Step 2:** FAIL → implement with `defer` record and `recover`/re-panic → PASS → `task sanity`.
- [ ] **Step 3:** Add `ExampleBreaker_Execute` and `ExampleDo` to `example_test.go`. Commit `feat(breaker): Execute and Do`.

### Task 7: Fuzz test and benchmarks

**Files:**
- Create: `pkg/breaker/fuzz_test.go`, `bench_test.go`

- [ ] **Step 1:** `FuzzStateMachine(f *testing.F)`: input bytes decode to ops {Allow, RecordCurrent(outcome), RecordStale(outcome), Advance(d), ForceOpen, ForceClose, Reset}; after each op assert: `InFlight >= 0 && InFlight <= HalfOpenMaxCalls`; `attempt` non-decreasing between Closed entries; state-change events count equals observed transitions (recording `MockEvents` replaced by a simple counter type here); Closed ⇒ `InFlight == 0`; a permit admitted in Closed is never accepted after a trip. Also a goroutine variant: 8 goroutines interleaving `Allow` with one goroutine tripping; assert no stale acceptance.
- [ ] **Step 2:** Leaked-slot test (Review Focus 2): `HalfOpenMaxCalls=3`; slot A admitted at t0 never recorded; every 1s two probes admitted and recorded Success with `HalfOpenSuccesses=1000`; at t0+60s+1ms `Allow` → Open, `Reason==ReasonProbeTimeout`.
- [ ] **Step 3:** Benchmarks per the spec table: `BenchmarkAllowRecordClosedCount`, `...ClosedTime`, `BenchmarkAllowOpen`, `BenchmarkExecuteParallel8`, `BenchmarkExecuteParallel32`. The parallel ones use an explicit fan-out, not `RunParallel`: `benchParallel(b, n int)` starts exactly `n` goroutines via `sync.WaitGroup`, each running `b.N/n` iterations, with `b.ResetTimer()` after setup. Record ns/op, allocs, and `runtime.GOMAXPROCS(0)` in the PR body. Gate: 0 allocs on the Closed and Open paths; throughput at 32 ≥ 80% of throughput at 8.
- [ ] **Step 4:** `go test -fuzz=FuzzStateMachine -fuzztime=30s ./pkg/breaker` passes; `task go-bench` numbers recorded. `task sanity`.
- [ ] **Step 5:** Commit `test(breaker): fuzz invariants and benchmarks`. Push. `gh pr create --base feat/breaker-types --title "feat(breaker): core state machine"` with benchmark table in the body.

---

## PR 3 — Hooks (`feat/hooks`)

### Task 8: `LogEvents` and `MultiEvents`

**Files:**
- Create: `pkg/hooks/doc.go`, `log.go`, `multi.go`, `option.go`, `main_test.go`, `log_test.go`, `multi_test.go`, `bench_test.go`

**Interfaces:**
- Consumes: `breaker.Events` and event structs (Task 4).
- Produces: `NewLogEvents(log *logger.Logger, opts ...Option) (*LogEvents, error)`; `WithRejectEvery`, `WithFailureEvery`, `WithMinLevel`, `WithClock`; `(*LogEvents).Forget(name)`; `NewMultiEvents(evs ...breaker.Events) breaker.Events`; field-name consts `FieldBreaker="breaker"` … `FieldSuppressed="suppressed"`; `ErrNilLogger`.
  Internal per-breaker entry: `struct{ reject, failure *rate.Limiter; rejectSuppressed, failureSuppressed atomic.Uint64 }` in a `sync.Map`.

- [ ] **Step 1:** `git checkout -b feat/hooks feat/breaker-core`. `go get github.com/barnowlsnest/go-logslib/v2@v2.4.0 golang.org/x/time`. Tests with a JSON `logger.New` on a `bytes.Buffer`, `WithClock(testClock)`. go-logslib emits levels upper-case (`"level":"ERROR"`, `"INFO"`, `"WARN"`, `"DEBUG"`):
  - `TestNilLoggerErrors`.
  - `TestStateChangeIntoOpenIsError`: line has `"level":"ERROR"`, fields `breaker,from,to,reason,attempt,epoch,total,failures`.
  - `TestOtherTransitionsAreInfo`.
  - `TestFailureIsWarnWithErrorAndElapsed`.
  - `TestSuccessIsDebugAndFilteredByLevel`: logger at Info → zero bytes written; at Debug → one line.
  - `TestRejectRateLimited`: 10k `OnReject` at one instant → exactly 1 line; advance 1s; one more → line contains `"suppressed":9999`.
  - `TestFailureRateLimitedSeparately`: rejection limiter does not consume failure budget.
  - `TestForgetDropsState`: after `Forget`, next reject emits with `suppressed:0`.
  - `TestMultiFanOutAndRecover`: two `MockEvents`, first panics, second still receives.
- [ ] **Step 2:** FAIL → implement (`AllowN(clock(),1)`, `Swap(0)` on emit, level check before building fields using `log.Level()` unless `WithMinLevel`) → PASS.
- [ ] **Step 3:** Benchmarks: `BenchmarkAllowRecordLogInfo` (0 allocs required), `BenchmarkAllowRecordLogDebugTempFile` (sink `os.CreateTemp`), `BenchmarkAllowRecordLogWarnAllFailures`. Record in PR body.
- [ ] **Step 4:** `example_test.go` with `ExampleNewLogEvents`. `task sanity`. Commit `feat(hooks): LogEvents and MultiEvents`. Push. `gh pr create --base feat/breaker-core`.

---

## PR 4 — Registry (`feat/registry`)

### Task 9: `Registry`

**Files:**
- Create: `pkg/registry/doc.go`, `registry.go`, `option.go`, `errors.go`, `main_test.go`, `registry_test.go`, `bench_test.go`

**Interfaces:**
- Produces: exactly the spec's `pkg/registry` API plus `WithRand(r *rand.Rand) Option` (`math/rand/v2`; default a package-level `rand.New(rand.NewPCG(...))` seeded from `time.Now`); `ErrNilFactory`. Internal `entry{ b *breaker.Breaker; lastAccess atomic.Int64; index int }`.

- [ ] **Step 1:** `git checkout -b feat/registry feat/hooks`. Tests:
  - `TestGetCreatesOnce`: 100 goroutines `Get("a")` → same pointer; factory called once (mock counter).
  - `TestFactoryErrorNotMemoized`: factory returns bad config → `Get` errors each time, `Len()==0`.
  - `TestEventsInjectedWhenNil`.
  - `TestEvictPrefersClosed`: `MaxEntries=3`, `EvictSamples=3`; `a` forced Open, `b`,`c` Closed with older access; `Get("d")` evicts `b` or `c`, never `a`; `OnEvict` called with that key.
  - `TestEvictOpenOnlyIfAllSamplesOpen`.
  - `TestSampledEvictionUsesRand`: `MaxEntries=10, EvictSamples=3`, `WithRand` seeded; 10 Closed entries with distinct `lastAccess`; `Get("k")` evicts exactly the oldest of the three indices the seeded generator yields (compute expected by running the same generator in the test).
  - `TestRemoveIsSwapRemove`: remove middle of three, remaining indices consistent (`Range` yields two).
  - `TestAccessGranularity`: two `Get`s within 1s store once (observe via `Snapshot`-free internal accessor in `export_test.go`).
- [ ] **Step 2:** FAIL → implement → PASS. Benchmark `BenchmarkGetHit` 0 allocs.
- [ ] **Step 3:** `example_test.go` with `ExampleRegistry`. `task sanity`. Commit `feat(registry): keyed breakers with sampled eviction`. Push. `gh pr create --base feat/hooks`.

---

## PR 5 — Composite (`feat/composite`)

Files in `pkg/composite`: `doc.go`, `composite.go`, `child.go`, `lease.go`, `trip.go`, `scan.go`, `hierarchy.go`, `errors.go`, tests mirror, `main_test.go`, `bench_test.go`.

### Task 10: Trip functions

- [ ] **Step 1:** `git checkout -b feat/composite feat/registry`. `go get github.com/barnowlsnest/go-datalib/v5`. `TripSuite` table tests for `OpenChildren`, `OpenChildrenRatio` (ratio with 0 children is false), `AggregateFailureRate`, `AllOf`, `AnyOf`; invalid args (ratio > 1, ratio < 0, min < 0, threshold > 1) **panic at construction** (`assert.Panics`), the same contract as `regexp.MustCompile`: misconfiguration fails loudly at startup, never silently at request time. Godoc states it.
- [ ] **Step 2:** Implement in `trip.go`. Commit `feat(composite): trip functions`.

### Task 11: Children, scan cache, and parent breaker

**Files:** `child.go`, `scan.go`, `composite.go`, `errors.go`

**Interfaces:**
- Produces: `Child` interface; `Config`; `New(cfg Config) (*Composite, error)`; `AddChild`, `AttachChild`, `RemoveChild`, `Child`, `Children`; `Name`, `State`, `Allow`, `Record` (so `*Composite` is a `Child`); `ErrInvalidConfig` (re-exported alias to `breaker.ErrInvalidConfig`), `ErrDuplicateChild`, `ErrNoHealthyChildren`, `ErrTooDeep`, `ErrNilChild`.
  Internal: `children atomic.Pointer[[]Child]`, `scan struct{ open, total atomic.Int32; at atomic.Int64 }`, `tripAggregate atomic.Pointer[breaker.Counts]` (set by the adapter policy when it trips, read lock-free by `ReopenEarly`, cleared by an internal `breaker.Events` observer that `New` chains in front of `cfg.Events` on the **parent only** and that watches for `To == StateClosed`), `rr atomic.Uint32` round-robin index, `parent atomic.Pointer[Composite]` (set by `AttachChild`; non-nil → `ErrAlreadyAttached`). Errors add `ErrAlreadyAttached`.

- [ ] **Step 1:** Tests (`testClock` shared by parent and children via `ChildTemplate`):
  - `TestParentPoliciesRejected`: `Parent.Policies` non-empty → `ErrInvalidConfig`.
  - `TestAddDuplicate`, `TestAttachNil`, `TestRemoveInvalidatesScan`.
  - `TestAttachTwiceRejected`: attach the same sub-composite to two parents → second returns `ErrAlreadyAttached`.
  - `TestTripAggregateClearedOnClose`: after parent closes, internal pointer is nil (via `export_test.go`); user `MockEvents` still received the Closed event.
  - `TestOneOfThreeOpenDoesNotTrip` (default Trip).
  - `TestTwoOfThreeOpenLowRateDoesNotTrip`.
  - `TestTripsOnRatioAndRate`: 2 of 3 children Open and 50 parent records at 60% failure → parent Open.
  - `TestReopenEarlyWhenChildrenRecover`: `Trip=OpenChildren(2)`; parent Open; advance children past their `OpenDuration` (lazy HalfOpen) but **not** past parent's; `Pick` → parent HalfOpen with `ReasonEarlyHalfOpen`.
  - `TestFrozenAggregateKeepsParentOpen`: default Trip; parent Open at t0; advance 11s (parent live counts empty); children still Open → `Pick` still rejected (no early reopen).
  - `TestClosingRecheckReopens`: `Trip=OpenChildren(2)`; parent HalfOpen; probe Success through healthy child while 2 children still Open → parent `HalfOpen→Open`, `Attempt==1`, no Closed event.
  - `TestScanThrottled`: `ScanInterval=10ms`; child flips Open; within 10ms the cached open count is unchanged; after advance it updates.
- [ ] **Step 2:** FAIL → implement → PASS. Commit `feat(composite): children, scan cache, parent breaker`.

### Task 12: `Lease`, `Pick`, `Record`, `Execute`

**Files:** `lease.go`, `composite.go` (methods)

**Interfaces:**
- Produces: `const MaxDepth = 4`; `type Lease struct{ Leaf *breaker.Breaker; path [MaxDepth]hop; depth uint8 }`; `(Lease).ID()`, `(Lease).Path()`; `Pick() (Lease, error)`; `Record(l Lease, outcome, elapsed)`; `Execute(ctx, fn func(ctx, leaf string) error) error`.

- [ ] **Step 1:** Tests:
  - `TestPickRoundRobinSkipsOpen`: children a,b,c; b Open → picks alternate a,c.
  - `TestPickRejectedWhenParentOpen`: `*RejectedError{Name: parent}`.
  - `TestNoHealthyChildren`: all Open → `errors.Is(err, ErrNoHealthyChildren) && errors.Is(err, ErrOpen)`; parent `Snapshot().InFlight==0`.
  - `TestRecordHitsLeafAndParent`: counts on both.
  - `TestNestedPickReleasesIntermediatePermit` (Review Focus 4): root → two sub-composites, each with two leaves all Open; `Pick` → `ErrNoHealthyChildren`; every composite `InFlight==0`.
  - `TestNestedRecordWalksPath`: depth 3, counts on every level.
  - `TestTooDeep`: attaching a composite that would exceed `MaxDepth` → `ErrTooDeep`.
  - `TestExecuteRecordsViaDefer` (panic path).
  - `TestPerHopEpochIndependent`: leaf tripped between Pick and Record → leaf discards, parent still counts.
- [ ] **Step 2:** FAIL → implement → PASS. Benchmark `BenchmarkPickRecord10Children` 0 allocs. `example_test.go` with `ExampleComposite`. Commit `feat(composite): Lease, Pick, Record, Execute`.

### Task 13: `FromHierarchy`

**Files:** `hierarchy.go`, `hierarchy_test.go`

- [ ] **Step 1:** Tests: model `{#root:[payments], payments:[eu,us], eu:[h1,h2], us:[h3]}` → root composite with 2 composite children and 3 leaves; `compositeCfg` and `leafCfg` receive the exact `path` slices; cycle model → error from `tree.Hierarchy` wrapped with `ErrInvalidConfig`; depth 5 model → `ErrTooDeep`.
- [ ] **Step 2:** FAIL → implement (walk with `ChildrenIter`, build bottom-up, local `uint64` counter for node IDs) → PASS. Add `ExampleFromHierarchy`. `task sanity`.
- [ ] **Step 3:** Commit `feat(composite): FromHierarchy`. Push. `gh pr create --base feat/registry`.

---

## PR 6 — HTTP adapter (`feat/adapter-http`)

### Task 14: Shared options and HTTP round tripper

**Files:**
- Create: `pkg/adapter/doc.go`, `option.go`, `errors.go`, `http.go`, `main_test.go`, `option_test.go`, `http_test.go`, `mocks_test.go`

**Interfaces:**
- Produces: `Option` (func over an internal `settings` struct with `proto` tag), `WithBreaker`, `WithRegistry`, `ErrInvalidOption`, `ErrNoBreaker`, `ErrBothBreakerAndRegistry`, `ErrRegistry`; HTTP API exactly per spec; internal `resolve(settings, key string) (*breaker.Breaker, error)`.

- [ ] **Step 1:** `git checkout -b feat/adapter-http feat/composite`. Tests with `httptest.NewServer` and a `MockRoundTripper`:
  - `TestOptionsExactlyOneSource`: none → `ErrNoBreaker`; both → `ErrBothBreakerAndRegistry`; gRPC option passed to HTTP constructor → `ErrInvalidOption`.
  - `TestNilNextUsesDefaultTransport`.
  - `TestDefaultHTTPClassifier` table: 200→Success, 404→Success, 429→Success, 408→Failure, 500→Failure, 503→Failure, transport err→Failure, cancelled ctx→Ignored, deadline ctx + err→Failure.
  - `TestRejectedWhenOpen`: `*RejectedError` returned, mock `RoundTrip` not called.
  - `TestRegistryKeyedByHost`: two hosts → two breakers.
  - `TestRegistryErrorFailsRequest`: `errors.Is(err, ErrRegistry)`.
  - `TestResponseAndErrorBothSet` (Review Focus 5): mock returns `(&http.Response{Body: closer}, err)` → Failure recorded, body closed (closer mock `AssertCalled("Close")`).
  - `TestElapsedExcludesBody`: slow body reader; `CallEvent.Elapsed` small.
- [ ] **Step 2:** FAIL → implement → PASS. `task sanity`.
- [ ] **Step 3:** `example_test.go` with `ExampleNewHTTPRoundTripper`. Commit `feat(adapter): shared options and HTTP round tripper`. Push. `gh pr create --base feat/composite`.

---

## PR 7 — gRPC adapter (`feat/adapter-grpc`)

### Task 15: gRPC interceptors

**Files:** `pkg/adapter/grpc.go`, `grpc_test.go`. No protoc and no hand-written `ServiceDesc` (the default proto codec would reject hand-written messages): tests use `google.golang.org/grpc/interop/grpc_testing`, which ships compiled `TestService` stubs with `UnaryCall` and `StreamingOutputCall`, served over `grpc/test/bufconn` by a small in-test server type that embeds `grpc_testing.UnimplementedTestServiceServer` and returns whatever status each test configures.

**Interfaces:**
- Produces: API per spec, plus `type grpcRejectedError struct{ *breaker.RejectedError; code codes.Code }` implementing `GRPCStatus() *status.Status` and `Unwrap() error`.

- [ ] **Step 1:** `git checkout -b feat/adapter-grpc feat/adapter-http`. `go get google.golang.org/grpc`. Tests over bufconn:
  - `TestDefaultGRPCClassifier` table over every `codes.Code` plus cancelled ctx.
  - `TestUnaryRejectedStatus`: Open → `status.Code(err)==codes.Unavailable` and `errors.Is(err, breaker.ErrOpen)`.
  - `TestRejectCodeOption`: `WithGRPCRejectCode(codes.FailedPrecondition)`.
  - `TestUnaryRecordsOutcome`: server returns `Internal` → Failure.
  - `TestStreamRecordsEstablishmentOnly`: stream opens → Success even if later `RecvMsg` errors.
  - `TestKeyDefaultsToTarget`.
- [ ] **Step 2:** FAIL → implement → PASS. `task sanity`.
- [ ] **Step 3:** Add `ExampleNewGRPCUnaryInterceptor`. Commit `feat(adapter): gRPC client interceptors`. Push. `gh pr create --base feat/adapter-http`.

---

## PR 8 — TCP adapter (`feat/adapter-tcp`)

### Task 16: TCP dialer and tracked conn

**Files:** `pkg/adapter/tcp.go`, `tcp_conn.go`, `tcp_test.go`

**Interfaces:**
- Produces: API per spec; `type trackedConn struct{ net.Conn; b *breaker.Breaker; classify TCPClassifier }` with `Unwrap() net.Conn`, conditional `ReadFrom`/`WriteTo`/`CloseRead`/`CloseWrite`/`SyscallConn`; `type connRejectedError struct{ *breaker.RejectedError }` implementing `net.Error` (`Timeout() false`, `Temporary() false`).

- [ ] **Step 1:** `git checkout -b feat/adapter-tcp feat/adapter-grpc`. Two groups of tests. Real sockets (`net.Listen("tcp","127.0.0.1:0")`) only for dial and wrapping, which are not platform-sensitive:
  - `TestDialRecorded`: refused port → Failure; success → Success.
  - `TestDialRejectedWhenOpen`: `MockContextDialer` not called.
  - `TestTrackIOOffByDefault`: returned conn is the raw `*net.TCPConn`.
  - `TestUnwrapAndOptionalInterfaces`: wrapped `*net.TCPConn` exposes `Unwrap`, `CloseWrite`, `SyscallConn`.

  The I/O classification matrix uses a `MockConn` (testify mock implementing `net.Conn`) returned by `MockContextDialer`, so error ordering never depends on the OS:
  - `TestTrackIOReadErrorIsFailure`: `Read` returns `syscall.ECONNRESET` → Failure.
  - `TestTrackIOEOFNotRecorded`: `Read` returns `io.EOF` → `Counts().Total==0`.
  - `TestTrackIOClosedNotRecorded`: `net.ErrClosed`.
  - `TestTrackIODeadlineIgnored`: `Read` returns `os.ErrDeadlineExceeded` → Ignored.
  - `TestTrackIOFailsFastWhenOpen`: `Write` returns an error satisfying `net.Error` (`Timeout()==false`) and `errors.Is(err, breaker.ErrOpen)`; `MockConn.Write` not called.
  - `TestTrackIOProbeSuccessCloses`: breaker HalfOpen; one successful `Write` → Closed.
  - `TestTrackIONonProbeSuccessIgnored`: Closed; 1000 successful writes → `Counts().Total==0`.
- [ ] **Step 2:** FAIL → implement → PASS. `task sanity`.
- [ ] **Step 3:** Add `ExampleNewTCPDialer`. Commit `feat(adapter): TCP dialer with optional I/O tracking`. Push. `gh pr create --base feat/adapter-grpc`.

---

## PR 9 — Docs (`feat/docs`)

### Task 17: README, package docs, benchmark report

**Files:** `README.md`, `pkg/*/doc.go`, `docs/benchmarks.md`. README code blocks are copied verbatim from the `Example*` functions already in each package's `example_test.go` (compiled and run by `go test`, shown on pkg.go.dev); no README extraction tooling.

- [ ] **Step 1:** `git checkout -b feat/docs feat/adapter-tcp`. Write README sections: Quick start (HTTP), Core config reference table (every `Config` field, default, meaning), Permits and `Execute`, Policies, Windows (sliding, default time window), Half-open and `ProbeTimeout ≥ request timeout` rule, Hooks (`Events` example for a hypothetical metrics client, `LogEvents` with `BufferSize > 0` note and `Forget` wired to `WithOnEvict`), Registry, Composite (A → B with 3 instances of C; parent recovery bounded by children's `OpenDuration`), `FromHierarchy`, Adapters (HTTP, gRPC with retry-policy note and `WithGRPCRejectCode`, TCP with TrackIO semantics), Benchmarks (table from `docs/benchmarks.md`), Versioning (v0 statement).
- [ ] **Step 2:** Diff each README block against its `Example*` source by eye; `go test ./... -run Example` passes.
- [ ] **Step 3:** `task go-bench > docs/benchmarks.md` (formatted table, machine noted).
- [ ] **Step 4:** `task sanity`. Commit `docs: README, package docs, benchmarks`. Push. `gh pr create --base feat/adapter-tcp`.

---

## Merge procedure

- [ ] Merge PR 1 → `main`. `gh pr edit <PR2a> --base main`. Rebase `feat/breaker-types` on `main`, force-push. Repeat up the stack after every merge (2a, 2b, 3, 4, 5, 6, 7, 8, 9).
- [ ] After PR 9 merges: `git tag v0.1.0 && git push origin v0.1.0`.
