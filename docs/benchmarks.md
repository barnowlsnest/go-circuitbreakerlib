# Benchmarks

`task go-bench` on an Apple M1 Max, macOS 26.5, Go 1.27, `GOMAXPROCS=10`.
Every number is `ns/op` with `-benchmem`; allocations are per operation.

| Package     | Benchmark                                            | ns/op | B/op | allocs/op | Target                     |
|-------------|------------------------------------------------------|------:|-----:|----------:|----------------------------|
| `breaker`   | `Allow` + `Record`, Closed, count window             | 119.9 |    0 |         0 | ≤ 200, 0 allocs            |
| `breaker`   | `Allow` + `Record`, Closed, time window (10 buckets) | 120.7 |    0 |         0 | ≤ 300, 0 allocs            |
| `breaker`   | `Allow`, Open (rejection)                            |  40.6 |    0 |         0 | ≤ 100, 0 allocs            |
| `breaker`   | `Execute`, 8 goroutines                              | 284.1 |    0 |         0 | ≥ 5M ops/s (see below)     |
| `breaker`   | `Execute`, 32 goroutines                             | 289.5 |    0 |         0 | ≥ 80% of 8 goroutines      |
| `hooks`     | `Allow` + `Record`, `LogEvents`, logger at Info      | 113.6 |    0 |         0 | 0 allocs                   |
| `hooks`     | `Allow` + `Record`, `LogEvents`, Debug, temp file    | 600.8 |   80 |         4 | reported (go-logslib)      |
| `hooks`     | `Execute`, `LogEvents` at Warn, 100% failures        | 301.1 |    0 |         0 | no allocs beyond go-logslib |
| `registry`  | `Get`, hit                                           |  40.2 |    0 |         0 | ≤ 100, 0 allocs            |
| `registry`  | `Get`, hit, parallel                                 |  22.7 |    0 |         0 |                            |
| `registry`  | `Get`, create + evict, 1024 entries bounded          | 769.7 |  920 |        12 |                            |
| `composite` | `Pick` + `RecordLease`, 10 healthy children          | 335.2 |    0 |         0 | ≤ 400, 0 allocs            |
| `composite` | same, parallel                                       | 406.8 |    0 |         0 |                            |

## The parallel `Execute` gate

The design set two gates for keeping one mutex per breaker instead of a
sharded window: 5M ops/s with 8 goroutines, and 32 goroutines keeping 80%
of that. Measured: 3.5M ops/s at 8 goroutines and 3.5M ops/s at 32. The
first gate is missed, the second is met with room to spare, and 32
goroutines are not slower than 8, so the mutex is not the limit. The
remaining cost is two clock reads and a `defer` in `Execute`; the raw
`Allow` + `Record` pair runs at 8.3M ops/s on one goroutine. The sharded
window was not built.
