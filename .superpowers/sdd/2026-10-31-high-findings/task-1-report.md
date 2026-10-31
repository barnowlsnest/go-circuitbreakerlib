# Task 1 Report: Policy / TripFunc after unlock + composite freeze on Open

Status: DONE_WITH_CONCERNS

## Commit

- `1bebae5 fix(breaker): evaluate policies outside the breaker lock`

## Summary

- Added RED re-entry tests for `Policy.ShouldTrip` and composite `TripFunc`; both failed by one-second timeout before the implementation.
- Changed `Breaker.Record` evaluation so policy checks run after a counts snapshot and unlock, then relock and validate epoch/state before opening or closing.
- Kept HalfOpen failure reopening under the lock.
- Moved composite `tripAggregate` freezing out of `tripPolicy.ShouldTrip` and into `parentObserver.OnStateChange` when the parent actually enters Open; Closed still clears it.
- Updated `docs/spec.md` lines that claimed policies run under the breaker lock or freeze happens inside the policy decision.

## Verification

- RED breaker: `go test -timeout 3s ./pkg/breaker/ -run TestBreakerSuite/TestPolicyShouldTripMayReenterAllow -count=1` failed with `Record did not complete after Policy.ShouldTrip re-entered Allow`.
- RED composite: `go test -timeout 3s ./pkg/composite/ -run TestCompositeSuite/TestTripFuncMayReenterPick -count=1` failed with `Record did not complete after TripFunc re-entered Pick`.
- Focused green: `go test -race -timeout 60s ./pkg/breaker/ ./pkg/composite/ -count=1` passed.
- Full green: `task sanity` passed.

## Self-Review

- Relock validation checks the original epoch and candidate state before applying delayed policy decisions.
- Policy panics are still collected and reported after the final unlock.
- Composite freezing now happens only from the parent Open state-change observer, so abandoned unlocked trip decisions do not leave stale freeze data.

## Concerns

- The requested Closed re-entry shape needed the callback to settle the returned permit with `Record`; `Allow` alone is lock-free in Closed and would not reproduce the original deadlock.

## Fix Report: Review Findings

Status: DONE

### Fixes

- Critical: Added `TestHalfOpenFailureKeepsTripAggregate`; verified it failed before the fix with frozen aggregate `Total=1` and immediate reopen. Fixed `parentObserver.OnStateChange` to freeze only for `tripPolicyName` Open transitions and only when no freeze exists, preserving the original aggregate across HalfOpen failure/probe-timeout reopens.
- Important: Rewrote `Policy` godoc to one short sentence without lock-timing claims.
- Minor: Updated `hookPanic` comment to remove the stale under-lock claim.

### Verification

- RED: `go test -timeout 10s ./pkg/composite/ -run TestCompositeSuite/TestHalfOpenFailureKeepsTripAggregate -count=1` failed before implementation as expected.
- GREEN: `go test -timeout 10s ./pkg/composite/ -run TestCompositeSuite/TestHalfOpenFailureKeepsTripAggregate -count=1` passed.
- Focused: `go test -race -timeout 60s ./pkg/breaker/ ./pkg/composite/ -count=1` passed.
- Full: `task sanity` passed.
