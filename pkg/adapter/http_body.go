package adapter

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

type trackedBody struct {
	body       io.ReadCloser
	breaker    *breaker.Breaker
	resp       *http.Response
	classifier HTTPClassifier
	ctx        context.Context
	start      time.Time
	permit     breaker.Permit
	recorded   atomic.Bool
}

func (b *trackedBody) Read(p []byte) (n int, err error) {
	done := false
	defer func() {
		if !done {
			b.record(breaker.OutcomeFailure, err)
		}
	}()
	n, err = b.body.Read(p)
	done = true
	switch {
	case err == nil:
		return n, nil
	case errors.Is(err, io.EOF):
		b.record(b.classify(nil), nil)
	default:
		b.record(b.classify(err), err)
	}
	return n, err
}

func (b *trackedBody) Close() error {
	done := false
	defer func() {
		if !done {
			b.record(breaker.OutcomeFailure, nil)
		}
	}()
	err := b.body.Close()
	done = true
	b.record(b.classify(err), err)
	return err
}

func (b *trackedBody) classify(err error) breaker.Outcome {
	return classifyHTTP(b.breaker, b.classifier, b.ctx, b.resp, err)
}

func (b *trackedBody) record(outcome breaker.Outcome, err error) {
	if !b.recorded.CompareAndSwap(false, true) {
		return
	}
	b.breaker.Record(b.permit, outcome, b.breaker.Now().Sub(b.start), err)
}
