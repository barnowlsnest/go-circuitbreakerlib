package adapter

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

type trackedClientStream struct {
	grpc.ClientStream
	breaker       *breaker.Breaker
	classifier    GRPCClassifier
	ctx           context.Context
	start         time.Time
	permit        breaker.Permit
	recorded      atomic.Bool
	serverStreams bool
}

func (s *trackedClientStream) Header() (metadata.MD, error) {
	done := false
	var md metadata.MD
	var err error
	defer func() {
		if !done {
			s.record(breaker.OutcomeFailure, err)
		}
	}()
	md, err = s.ClientStream.Header()
	done = true
	if err != nil {
		s.record(s.classify(err), err)
	}
	return md, err
}

func (s *trackedClientStream) CloseSend() error {
	done := false
	var err error
	defer func() {
		if !done {
			s.record(breaker.OutcomeFailure, err)
		}
	}()
	err = s.ClientStream.CloseSend()
	done = true
	if err != nil {
		s.record(s.classify(err), err)
	}
	return err
}

func (s *trackedClientStream) SendMsg(m any) error {
	done := false
	var err error
	defer func() {
		if !done {
			s.record(breaker.OutcomeFailure, err)
		}
	}()
	err = s.ClientStream.SendMsg(m)
	done = true
	if err != nil {
		if errors.Is(err, io.EOF) {
			return err
		}
		s.record(s.classify(err), err)
	}
	return err
}

func (s *trackedClientStream) RecvMsg(m any) error {
	done := false
	var err error
	defer func() {
		if !done {
			s.record(breaker.OutcomeFailure, err)
		}
	}()
	err = s.ClientStream.RecvMsg(m)
	done = true
	if err == nil {
		if !s.serverStreams {
			s.record(s.classify(nil), nil)
		}
		return nil
	}
	if errors.Is(err, io.EOF) {
		s.record(s.classify(nil), nil)
		return err
	}
	if err != nil {
		s.record(s.classify(err), err)
	}
	return err
}

func (s *trackedClientStream) classify(err error) breaker.Outcome {
	return classifyGRPC(s.breaker, s.classifier, s.ctx, err)
}

func (s *trackedClientStream) record(outcome breaker.Outcome, err error) {
	if !s.recorded.CompareAndSwap(false, true) {
		return
	}
	s.breaker.Record(s.permit, outcome, s.breaker.Now().Sub(s.start), err)
}
