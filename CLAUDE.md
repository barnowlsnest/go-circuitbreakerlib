# CLAUDE.md

Guidance for Claude Code when working in this repository.

## Project

`go-circuitbreakerlib` — a circuit breaker library for Go services that call
remote dependencies over HTTP, gRPC, or TCP. It is a **library, not an
application**: there is no `main`, no root package, and no binary to run.

- Module path: `github.com/barnowlsnest/go-circuitbreakerlib`.
- Go version: **1.27** (`go.mod`). CI installs it via `go-version-file: go.mod`.
- Design: `docs/spec.md`. The spec is the authority when code and comments
  disagree.

## Layout

All code lives under `pkg/`. Dependencies flow one way — a package never
imports a package listed below it.

| Package         | Purpose                                                                                      |
|-----------------|----------------------------------------------------------------------------------------------|
| `pkg/breaker`   | Core state machine: `Breaker`, `Permit`, windows, policies, backoff, `Events`. Stdlib only.  |
| `pkg/hooks`     | `LogEvents` (go-logslib + x/time/rate) and `MultiEvents`.                                    |
| `pkg/registry`  | Keyed breakers with sampled eviction.                                                        |
| `pkg/composite` | Parent/child breakers, `Lease`, `FromHierarchy` (go-datalib `tree`).                         |
| `pkg/adapter`   | One file per protocol: `http.go`, `grpc.go`, `tcp.go`; shared `option.go`.                   |

## Commands

The project is driven by [Task](https://taskfile.dev) (`Taskfile.yaml`):

```bash
task sanity      # tidy + clean + fmt + vet + lint + test + coverage gate  ← run this
task go-test     # go test -v -race -coverprofile=coverage.out ./...
task go-cover    # fail below 80% total coverage (reads coverage.out)
task go-bench    # go test -bench=. -benchmem -run='^$' ./...
task go-build    # go build ./...
task go-lint     # golangci-lint run --fix
```

**Run `task sanity` after every implementation step, not just at the end.**

## CI

- `.github/workflows/build.yml` — `task go-build`, `task go-test`, `task go-cover`
- `.github/workflows/golangci-lint.yml` — golangci-lint v2.14

Both resolve the Go toolchain from `go.mod` with `GOTOOLCHAIN=local`.

## Conventions

### Lint (`.golangci.yaml`, `version: "2"`)

Copied from `go-asynctasklib`, plus `govet` `fieldalignment`. Limits that
bite most often:

- `lll` line length **140**
- `funlen` 100 lines / 50 statements
- `gocyclo` min-complexity **15**
- `dupl` threshold 100
- `goconst` fires at **2** occurrences of a repeated literal
- `gocritic` with every tag enabled
- `goimports` local prefix is the module path — module-local imports go in
  their own final group

`fieldalignment` dictates struct field order and `--fix` applies it. Never
hand-order fields against it. One field per line; no size comments.

`_test.go` files are exempt from `dupl`, `funlen`, `goconst`, `gocyclo`, `gosec`.

### Errors

Sentinels live in each package's `errors.go`. Config validation joins
`ErrInvalidConfig` with a specific sentinel via `errors.Join`, so
`errors.Is` matches both. Rejections are `*breaker.RejectedError`, which
unwraps to `ErrOpen` or `ErrTooManyProbes`. Wrap rather than reformat.

### Contexts

Every method taking a `ctx` honors it. Classifiers receive the caller's
`ctx`; only `context.Canceled` is ignored, deadlines are failures.

### Concurrency

No breaker owns a goroutine or timer. State and epoch are one packed
atomic. A goroutine never holds two breaker locks; hooks fire after
unlock. `Allow` in Closed and the Open rejection path are lock-free;
HalfOpen admission takes the mutex.

### Comments

Don't add explanatory comments to fixes or tests. Godoc on exported
identifiers stays; ask before changing it.

### Tests

- `testify/suite` for every test file except `main_test.go`, `fuzz_test.go`,
  `bench_test.go`, `export_test.go`, `example_test.go`.
- Interface doubles are `testify/mock` mocks named `Mock<Interface>` with
  `AssertExpectations` in `TearDownTest`. Hand-written helpers (clocks,
  counters) are named `test...`.
- Table-driven subtests; `expected` (not `want`) for expectations.
- Injected clocks everywhere; never `time.Sleep` for a state transition.
- Every package runs under **goleak** via `TestMain` (`main_test.go`).
- No magic numbers — hoist them into named `const`s.
- Assert returned values; never discard them with `_`.
- README code is copied from `Example*` functions in `example_test.go`.

### Go version features

`go.mod` is on 1.27, so `wg.Go(func(){...})` is available and preferred.

## Delivery

Work is delivered as stacked GitHub PRs managed with `gh stack`, one per
package layer. Open a PR as soon as its branch passes `task sanity`. Never
chain PR bases by hand.

## Dependencies

Each package owns its imports: `pkg/breaker` stdlib only; `pkg/hooks`
go-logslib and `x/time/rate`; `pkg/composite` go-datalib `tree`;
`pkg/adapter` grpc. Test-only: `stretchr/testify`, `go.uber.org/goleak`.
Adding a new dependency is a deliberate decision — prefer the standard
library.
