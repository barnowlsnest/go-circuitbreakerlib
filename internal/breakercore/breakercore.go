package breakercore

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const unknownName = "unknown"

var (
	// ErrInvalidConfig marks an unusable breaker configuration.
	ErrInvalidConfig = errors.New("invalid breaker configuration")
	// ErrInvalidOption marks an unusable breaker option.
	ErrInvalidOption = errors.New("breaker: invalid option")
	// ErrEmptyName marks an empty breaker name.
	ErrEmptyName = errors.New("name is empty")
	// ErrNegativeDuration marks a negative duration.
	ErrNegativeDuration = errors.New("duration must not be negative")
	// ErrNilPolicy marks a nil policy.
	ErrNilPolicy = errors.New("policy is nil")
	// ErrEmptyPolicies marks an empty policy list.
	ErrEmptyPolicies = errors.New("policies must not be empty")
)

// State is the breaker's position in its state cycle.
type State uint32

const (
	// StateClosed admits every call and records outcomes.
	StateClosed State = iota
	// StateOpen rejects every call until the backoff elapses.
	StateOpen
	// StateHalfOpen admits a bounded number of probe calls.
	StateHalfOpen
)

// String returns the lower-case state name.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return unknownName
	}
}

// Reason explains a state change.
type Reason string

const (
	// ReasonTimeout is Open to HalfOpen after the backoff elapsed.
	ReasonTimeout Reason = "timeout"
	// ReasonEarlyHalfOpen is Open to HalfOpen before the backoff elapsed.
	ReasonEarlyHalfOpen Reason = "early-half-open"
	// ReasonHalfOpenSuccess is HalfOpen to Closed after probe successes.
	ReasonHalfOpenSuccess Reason = "half-open-success"
	// ReasonHalfOpenFailure is HalfOpen to Open after a probe failure.
	ReasonHalfOpenFailure Reason = "half-open-failure"
	// ReasonProbeTimeout is HalfOpen to Open after a stale probe.
	ReasonProbeTimeout Reason = "probe-timeout"
	// ReasonForced is a forced transition.
	ReasonForced Reason = "forced"
	// ReasonReset is a reset transition.
	ReasonReset Reason = "reset"
)

// Outcome is the classification of one call.
type Outcome uint8

const (
	// OutcomeSuccess counts as a success.
	OutcomeSuccess Outcome = iota
	// OutcomeFailure counts as a failure.
	OutcomeFailure
	// OutcomeIgnored is not counted.
	OutcomeIgnored
)

// String returns the outcome name.
func (o Outcome) String() string {
	switch o {
	case OutcomeSuccess:
		return "success"
	case OutcomeFailure:
		return "failure"
	case OutcomeIgnored:
		return "ignored"
	default:
		return unknownName
	}
}

// Counts is a snapshot of a sliding window.
type Counts struct {
	Total                uint32
	Successes            uint32
	Failures             uint32
	Slow                 uint32
	ConsecutiveFailures  uint32
	ConsecutiveSuccesses uint32
}

// Classifier turns a call result into an Outcome.
type Classifier func(ctx context.Context, err error) Outcome

// DefaultClassifier maps nil to success and most errors to failure.
func DefaultClassifier(ctx context.Context, err error) Outcome {
	if errors.Is(ctx.Err(), context.Canceled) {
		return OutcomeIgnored
	}
	if err == nil {
		return OutcomeSuccess
	}
	return OutcomeFailure
}

// Backoff decides how long a breaker stays Open.
type Backoff interface {
	Delay(attempt int) time.Duration
}

// Policy decides whether a breaker should trip.
type Policy interface {
	ShouldTrip(c Counts) bool
	Name() string
}

// Events observes a breaker.
type Events interface {
	OnStateChange(ev StateChangeEvent)
	OnCall(ev CallEvent)
	OnReject(ev RejectEvent)
	OnHookPanic(name string, recovered any)
}

// StateChangeEvent describes one transition.
type StateChangeEvent struct {
	At      time.Time
	Name    string
	Reason  Reason
	Counts  Counts
	Attempt int
	Epoch   uint64
	From    State
	To      State
}

// CallEvent describes one recorded outcome.
type CallEvent struct {
	At      time.Time
	Err     error
	Name    string
	Elapsed time.Duration
	State   State
	Outcome Outcome
	Slow    bool
}

// RejectEvent describes one rejected Allow.
type RejectEvent struct {
	At    time.Time
	Err   error
	Name  string
	State State
}

// NoopEvents discards every event.
type NoopEvents struct{}

// NewNoopEvents returns a no-op observer.
func NewNoopEvents() *NoopEvents { return &NoopEvents{} }

// OnStateChange implements Events.
func (*NoopEvents) OnStateChange(StateChangeEvent) {}

// OnCall implements Events.
func (*NoopEvents) OnCall(CallEvent) {}

// OnReject implements Events.
func (*NoopEvents) OnReject(RejectEvent) {}

// OnHookPanic implements Events.
func (*NoopEvents) OnHookPanic(string, any) {}

// Option configures breaker options.
type Option func(*options) error

type options struct {
	events            Events
	window            any
	backoff           Backoff
	clock             func() time.Time
	classifier        Classifier
	reopenEarly       func() bool
	name              string
	policies          []Policy
	slowCallDuration  time.Duration
	openDuration      time.Duration
	probeTimeout      time.Duration
	halfOpenMaxCalls  uint32
	halfOpenSuccesses uint32
	policiesSet       bool
	reopenEarlySet    bool
}

// Config is the applied breaker options before breaker validation.
type Config struct {
	Events            Events
	Window            any
	Backoff           Backoff
	Clock             func() time.Time
	Classifier        Classifier
	ReopenEarly       func() bool
	Name              string
	Policies          []Policy
	SlowCallDuration  time.Duration
	OpenDuration      time.Duration
	ProbeTimeout      time.Duration
	HalfOpenMaxCalls  uint32
	HalfOpenSuccesses uint32
}

// Settings is the resolved breaker metadata used by sibling packages.
type Settings struct {
	Clock      func() time.Time
	Classifier Classifier
	Events     Events
	Name       string
}

// WithName sets the breaker name.
func WithName(name string) Option {
	return func(o *options) error {
		if name == "" {
			return errors.Join(ErrInvalidConfig, ErrEmptyName)
		}
		o.name = name
		return nil
	}
}

// Invalid returns an option that always fails.
func Invalid(err error) Option {
	return func(*options) error {
		return err
	}
}

// WithPolicies replaces the trip policies.
func WithPolicies(policies ...Policy) Option {
	return func(o *options) error {
		if len(policies) == 0 {
			return errors.Join(ErrInvalidConfig, ErrEmptyPolicies)
		}
		for i, p := range policies {
			if p == nil {
				return fmt.Errorf("%w: %w at %d", ErrInvalidConfig, ErrNilPolicy, i)
			}
		}
		o.policies = policies
		o.policiesSet = true
		return nil
	}
}

// WithWindow sets the sliding window.
func WithWindow(w any) Option {
	return func(o *options) error {
		if w == nil {
			return fmt.Errorf("%w: nil window", ErrInvalidOption)
		}
		o.window = w
		return nil
	}
}

// WithBackoff sets the open-state delay.
func WithBackoff(b Backoff) Option {
	return func(o *options) error {
		if b == nil {
			return fmt.Errorf("%w: nil backoff", ErrInvalidOption)
		}
		o.backoff = b
		return nil
	}
}

// WithEvents sets the event sink.
func WithEvents(ev Events) Option {
	return func(o *options) error {
		if ev == nil {
			return fmt.Errorf("%w: nil events", ErrInvalidOption)
		}
		o.events = ev
		return nil
	}
}

// FillEvents fills an unset breaker event sink.
func FillEvents(ev Events) Option {
	return func(o *options) error {
		if ev == nil {
			return fmt.Errorf("%w: nil events", ErrInvalidOption)
		}
		if o.events == nil {
			o.events = ev
		}
		return nil
	}
}

// WithClassifier sets how call results become outcomes.
func WithClassifier(fn Classifier) Option {
	return func(o *options) error {
		if fn == nil {
			return fmt.Errorf("%w: nil classifier", ErrInvalidOption)
		}
		o.classifier = fn
		return nil
	}
}

// WithClock sets the time source.
func WithClock(fn func() time.Time) Option {
	return func(o *options) error {
		if fn == nil {
			return fmt.Errorf("%w: nil clock", ErrInvalidOption)
		}
		o.clock = fn
		return nil
	}
}

// WithReopenEarly sets the early-reopen predicate.
func WithReopenEarly(fn func() bool) Option {
	return func(o *options) error {
		o.reopenEarly = fn
		o.reopenEarlySet = true
		return nil
	}
}

// WithSlowCallDuration sets the slow-call threshold.
func WithSlowCallDuration(d time.Duration) Option {
	return func(o *options) error {
		if d < 0 {
			return errors.Join(ErrInvalidConfig, ErrNegativeDuration)
		}
		o.slowCallDuration = d
		return nil
	}
}

// WithOpenDuration sets the default open delay.
func WithOpenDuration(d time.Duration) Option {
	return func(o *options) error {
		if d < 0 {
			return errors.Join(ErrInvalidConfig, ErrNegativeDuration)
		}
		o.openDuration = d
		return nil
	}
}

// WithProbeTimeout sets the probe timeout.
func WithProbeTimeout(d time.Duration) Option {
	return func(o *options) error {
		if d < 0 {
			return errors.Join(ErrInvalidConfig, ErrNegativeDuration)
		}
		o.probeTimeout = d
		return nil
	}
}

// WithHalfOpenMaxCalls sets the HalfOpen probe limit.
func WithHalfOpenMaxCalls(n uint32) Option {
	return func(o *options) error {
		o.halfOpenMaxCalls = n
		return nil
	}
}

// WithHalfOpenSuccesses sets the HalfOpen close threshold.
func WithHalfOpenSuccesses(n uint32) Option {
	return func(o *options) error {
		o.halfOpenSuccesses = n
		return nil
	}
}

// ApplyOptions applies options without defaults.
func ApplyOptions(opts ...Option) (Config, error) {
	var o options
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return Config{}, err
		}
	}
	return (&o).toConfig(), nil
}

// ValidateNoPolicies rejects parent policy overrides.
func ValidateNoPolicies(opts ...Option) error {
	o, err := applyOptions(opts...)
	if err != nil {
		return err
	}
	if o.policiesSet {
		return fmt.Errorf("%w: policies must be empty, Trip is the parent policy", ErrInvalidConfig)
	}
	return nil
}

// ValidateNoReopenEarly rejects parent reopen overrides.
func ValidateNoReopenEarly(opts ...Option) error {
	o, err := applyOptions(opts...)
	if err != nil {
		return err
	}
	if o.reopenEarlySet {
		return fmt.Errorf("%w: ReopenEarly is reserved by the composite", ErrInvalidConfig)
	}
	return nil
}

// ResolveSettings resolves breaker metadata without construction.
func ResolveSettings(opts ...Option) (Settings, error) {
	o, err := applyOptions(opts...)
	if err != nil {
		return Settings{}, err
	}
	cfg := (&o).toConfig()
	cfg.applySettingsDefaults()
	if err := (&cfg).validateSettings(); err != nil {
		return Settings{}, err
	}
	return Settings{
		Clock:      cfg.Clock,
		Classifier: cfg.Classifier,
		Events:     cfg.Events,
		Name:       cfg.Name,
	}, nil
}

func applyOptions(opts ...Option) (options, error) {
	var o options
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return o, err
		}
	}
	return o, nil
}

func (o *options) toConfig() Config {
	return Config{
		Events:            o.events,
		Window:            o.window,
		Backoff:           o.backoff,
		Clock:             o.clock,
		Classifier:        o.classifier,
		ReopenEarly:       o.reopenEarly,
		Name:              o.name,
		Policies:          o.policies,
		SlowCallDuration:  o.slowCallDuration,
		OpenDuration:      o.openDuration,
		ProbeTimeout:      o.probeTimeout,
		HalfOpenMaxCalls:  o.halfOpenMaxCalls,
		HalfOpenSuccesses: o.halfOpenSuccesses,
	}
}

func (c *Config) applySettingsDefaults() {
	if c.Classifier == nil {
		c.Classifier = DefaultClassifier
	}
	if c.Events == nil {
		c.Events = NewNoopEvents()
	}
	if c.Clock == nil {
		c.Clock = time.Now
	}
}

func (c *Config) validateSettings() error {
	if c.Name == "" {
		return errors.Join(ErrInvalidConfig, ErrEmptyName)
	}
	if c.SlowCallDuration < 0 || c.OpenDuration < 0 || c.ProbeTimeout < 0 {
		return errors.Join(ErrInvalidConfig, ErrNegativeDuration)
	}
	return nil
}
