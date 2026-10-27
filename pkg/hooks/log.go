package hooks

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"
	"golang.org/x/time/rate"

	"github.com/barnowlsnest/go-circuitbreakerlib/pkg/breaker"
)

// Field names used on every line LogEvents writes.
const (
	FieldBreaker    = "breaker"
	FieldFrom       = "from"
	FieldTo         = "to"
	FieldReason     = "reason"
	FieldAttempt    = "attempt"
	FieldEpoch      = "epoch"
	FieldTotal      = "total"
	FieldFailures   = "failures"
	FieldSlow       = "slow"
	FieldOutcome    = "outcome"
	FieldElapsed    = "elapsed"
	FieldError      = "error"
	FieldState      = "state"
	FieldHook       = "hook"
	FieldSuppressed = "suppressed"
)

// Messages used on every line LogEvents writes.
const (
	msgStateChange = "circuit breaker state changed"
	msgFailure     = "circuit breaker call failed"
	msgSlow        = "circuit breaker call slow"
	msgSuccess     = "circuit breaker call succeeded"
	msgReject      = "circuit breaker rejected call"
	msgHookPanic   = "circuit breaker hook panicked"
)

const limiterBurst = 1

// LogEvents is a breaker.Events that logs through go-logslib.
//
// Levels are fixed: a transition into Open is Error, any other transition is
// Info, a failure or slow call is Warn, a rejection is Warn, a success is
// Debug, and a hook panic is Error. Events below the logger's level return
// before building any field.
//
// Rejections and failures are rate-limited per breaker name: at most one
// line per class per period, carrying the number of lines suppressed since
// the previous one. State changes and hook panics are never suppressed.
//
// go-logslib writes on the calling goroutine, so the logger should use a
// buffer and a non-blocking sink when the breaker sits on a hot path.
type LogEvents struct {
	log          *logger.Logger
	clock        func() time.Time
	entries      sync.Map
	rejectEvery  time.Duration
	failureEvery time.Duration
	minLevel     logger.Level
}

// entry holds one breaker's limiter state.
type entry struct {
	reject            *rate.Limiter
	failure           *rate.Limiter
	rejectSuppressed  atomic.Uint64
	failureSuppressed atomic.Uint64
}

// NewLogEvents returns a LogEvents writing to log. log must not be nil.
func NewLogEvents(log *logger.Logger, opts ...Option) (*LogEvents, error) {
	if log == nil {
		return nil, ErrNilLogger
	}
	o := options{
		clock:        time.Now,
		rejectEvery:  DefaultRejectEvery,
		failureEvery: DefaultFailureEvery,
	}
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	if !o.minLevelSet {
		o.minLevel = log.Level()
	}
	return &LogEvents{
		log:          log,
		clock:        o.clock,
		rejectEvery:  o.rejectEvery,
		failureEvery: o.failureEvery,
		minLevel:     o.minLevel,
	}, nil
}

// Forget drops the limiter state kept for name. Wire it to
// registry.WithOnEvict so evicted breakers do not leak entries.
func (l *LogEvents) Forget(name string) {
	l.entries.Delete(name)
}

func (l *LogEvents) enabled(level logger.Level) bool {
	return level >= l.minLevel
}

func (l *LogEvents) entry(name string) *entry {
	if e, ok := l.entries.Load(name); ok {
		return e.(*entry)
	}
	e, _ := l.entries.LoadOrStore(name, &entry{
		reject:  rate.NewLimiter(rate.Every(l.rejectEvery), limiterBurst),
		failure: rate.NewLimiter(rate.Every(l.failureEvery), limiterBurst),
	})
	return e.(*entry)
}

// OnStateChange implements breaker.Events.
func (l *LogEvents) OnStateChange(ev breaker.StateChangeEvent) {
	level := logger.InfoLevel
	if ev.To == breaker.StateOpen {
		level = logger.ErrorLevel
	}
	if !l.enabled(level) {
		return
	}
	fields := [...]logger.Field{
		logger.StringField(FieldBreaker, ev.Name),
		logger.StringField(FieldFrom, ev.From.String()),
		logger.StringField(FieldTo, ev.To.String()),
		logger.StringField(FieldReason, string(ev.Reason)),
		logger.IntField(FieldAttempt, ev.Attempt),
		logger.Uint64Field(FieldEpoch, ev.Epoch),
		logger.Uint32Field(FieldTotal, ev.Counts.Total),
		logger.Uint32Field(FieldFailures, ev.Counts.Failures),
	}
	if level == logger.ErrorLevel {
		l.log.Error(msgStateChange, fields[:]...)
		return
	}
	l.log.Info(msgStateChange, fields[:]...)
}

// OnCall implements breaker.Events.
func (l *LogEvents) OnCall(ev breaker.CallEvent) {
	if ev.Outcome == breaker.OutcomeSuccess && !ev.Slow {
		if !l.enabled(logger.DebugLevel) {
			return
		}
		l.log.Debug(msgSuccess,
			logger.StringField(FieldBreaker, ev.Name),
			logger.StringField(FieldState, ev.State.String()),
			logger.StringField(FieldOutcome, ev.Outcome.String()),
			logger.DurationField(FieldElapsed, ev.Elapsed),
		)
		return
	}
	if !l.enabled(logger.WarnLevel) {
		return
	}
	e := l.entry(ev.Name)
	if !e.failure.AllowN(l.clock(), 1) {
		e.failureSuppressed.Add(1)
		return
	}
	msg := msgFailure
	if ev.Slow {
		msg = msgSlow
	}
	errText := ""
	if ev.Err != nil {
		errText = ev.Err.Error()
	}
	fields := [...]logger.Field{
		logger.StringField(FieldBreaker, ev.Name),
		logger.StringField(FieldState, ev.State.String()),
		logger.StringField(FieldOutcome, ev.Outcome.String()),
		logger.BoolField(FieldSlow, ev.Slow),
		logger.DurationField(FieldElapsed, ev.Elapsed),
		logger.Uint64Field(FieldSuppressed, e.failureSuppressed.Swap(0)),
		logger.StringField(FieldError, errText),
	}
	if ev.Err == nil {
		l.log.Warn(msg, fields[:len(fields)-1]...)
		return
	}
	l.log.Warn(msg, fields[:]...)
}

// OnReject implements breaker.Events.
func (l *LogEvents) OnReject(ev breaker.RejectEvent) {
	if !l.enabled(logger.WarnLevel) {
		return
	}
	e := l.entry(ev.Name)
	if !e.reject.AllowN(l.clock(), 1) {
		e.rejectSuppressed.Add(1)
		return
	}
	errText := ""
	if ev.Err != nil {
		errText = ev.Err.Error()
	}
	l.log.Warn(msgReject,
		logger.StringField(FieldBreaker, ev.Name),
		logger.StringField(FieldState, ev.State.String()),
		logger.StringField(FieldError, errText),
		logger.Uint64Field(FieldSuppressed, e.rejectSuppressed.Swap(0)),
	)
}

// OnHookPanic implements breaker.Events.
func (l *LogEvents) OnHookPanic(name string, recovered any) {
	if !l.enabled(logger.ErrorLevel) {
		return
	}
	text := ""
	switch r := recovered.(type) {
	case error:
		text = r.Error()
	case string:
		text = r
	default:
		text = "non-error panic value"
	}
	l.log.Error(msgHookPanic,
		logger.StringField(FieldHook, name),
		logger.StringField(FieldError, text),
	)
}
