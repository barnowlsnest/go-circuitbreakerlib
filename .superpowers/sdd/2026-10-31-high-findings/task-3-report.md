# Task 3 Fix Report

## Review Fix 1

- Finding: successful client-streaming RPCs using `CloseAndRecv` returned a response with nil error but never recorded a breaker outcome.
- Fix: `trackedClientStream` now carries `StreamDesc.ServerStreams`; `RecvMsg(nil error)` records Success once only for non-server-streaming RPCs, while server-streaming and bidi streams still wait for EOF or an error.
- Regression: added `TestStreamRecordsClientStreamingCloseAndRecv` using `grpc_testing.StreamingInputCall`.
- Trailer check: `grpc.ClientStream.Trailer()` has no error return in grpc-go; terminal trailer status errors surface through `RecvMsg` / `CloseAndRecv` and are recorded there.
- Verification:
  - `go test ./pkg/adapter/ -run 'TestGRPCSuite/TestStreamRecordsClientStreamingCloseAndRecv' -count=1` failed before the fix with `Counts.Total == 0`.
  - `go test ./pkg/adapter/ -run 'TestGRPCSuite/TestStream' -count=1` passed after the fix.
  - `go test -race ./pkg/adapter/` passed.
  - `task sanity` passed.
