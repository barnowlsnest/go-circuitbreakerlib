package breaker

import (
	"context"
)

// Execute runs fn through the breaker. A canceled ctx returns ctx.Err()
// without touching the breaker; a rejection returns *RejectedError without
// calling fn; otherwise fn's error is returned unchanged after its outcome,
// decided by Config.Classifier, is recorded. Recording happens in a defer,
// so a panicking fn records a failure before the panic propagates.
func (b *Breaker) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	if fn == nil {
		return ErrNilFunc
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p, err := b.Allow()
	if err != nil {
		return err
	}
	start := b.cfg.Clock()
	done := false
	var callErr error
	defer func() {
		elapsed := b.cfg.Clock().Sub(start)
		if !done {
			b.Record(p, OutcomeFailure, elapsed)
			return
		}
		b.Record(p, b.classify(ctx, callErr), elapsed)
	}()
	callErr = fn(ctx)
	done = true
	return callErr
}

// Do is Execute for functions that return a value.
func Do[T any](ctx context.Context, b *Breaker, fn func(ctx context.Context) (T, error)) (T, error) {
	var out T
	if fn == nil {
		return out, ErrNilFunc
	}
	err := b.Execute(ctx, func(ctx context.Context) error {
		var err error
		out, err = fn(ctx)
		return err
	})
	return out, err
}

func (b *Breaker) classify(ctx context.Context, err error) (o Outcome) {
	defer func() {
		if r := recover(); r != nil {
			o = OutcomeFailure
			b.fireHookPanic(hookClassifier, r)
		}
	}()
	return b.cfg.Classifier(ctx, err)
}
