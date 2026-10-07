package streamadapter

import (
	"context"
	"errors"
	"fmt"
	"sync"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
)

// Terminal lifecycle state spellings a one-shot backend may report in
// RunStatus.LifecycleState. The AG-UI terminal vocabulary is three events, so
// the adapter normalizes the backend's strings onto them: "completed" is the
// entity spelling, "succeeded" the plan spelling, "canceled" the common variant.
const (
	lifecycleSucceeded = "succeeded"
	lifecycleCompleted = "completed"
	lifecycleFailed    = "failed"
	lifecycleCanceled  = "canceled"
)

// RunLifecycle mirrors a backend-owned run's observable lifecycle onto the
// data plane. One-shot backends (HTTP / gRPC / temporal_external) own no local
// byte stream — their token stream lives in the remote runtime and arrives via
// the event-ingest endpoint. What the control plane can observe is the run's
// lifecycle transitions, so this thin adapter publishes those as AG-UI
// milestones on the run channel: the integration contract's map + publish
// steps for a backend that owns no byte stream. Usage for such runs rides the
// ingest endpoint, not the milestone.
//
// Milestones are facts first: when a recorder is attached, each one is made
// durable before it is announced, and a persist failure is returned so the
// enclosing activity fails and the platform retries — the run's scope is kept
// so the retry re-attempts the same terminal. The bus publish stays fail-open
// (a nil publisher degrades the mirror to a no-op); the milestone is already
// durable. A run's scope is retained until a terminal milestone is durable,
// so the milestone is recorded at most once; abandoned runs hold their scope
// for the process lifetime, mirroring the run backend index.
type RunLifecycle struct {
	pub    stream.Publisher
	facts  *MilestoneRecorder
	logger logger.Interface

	mu     sync.Mutex
	scopes map[string]*runScope
}

// runScope is one started run's data-plane identity. The scope is dropped once
// a terminal milestone is durable, which also serves as the dedup marker: a
// later terminal Status has no scope to look up and is a no-op.
type runScope struct {
	handle   *stream.Handle
	threadID string
	runID    string
}

// NewRunLifecycle creates a lifecycle adapter. The logger may be nil;
// bus-publish failures are then dropped silently.
func NewRunLifecycle(pub stream.Publisher, l logger.Interface) *RunLifecycle {
	return &RunLifecycle{
		pub:    pub,
		logger: l,
		scopes: make(map[string]*runScope),
	}
}

// WithFacts attaches the milestone recorder that makes each lifecycle
// milestone durable before it is announced on the bus.
func (l *RunLifecycle) WithFacts(recorder *MilestoneRecorder) *RunLifecycle {
	l.facts = recorder

	return l
}

// PublishStarted opens a run's timeline after Start succeeds. It records the
// run's data-plane scope for later terminal milestones, publishes RUN_STARTED,
// and — when the remote already reports a terminal state at start — the
// terminal milestone too. A returned error means a milestone could not be made
// durable; the caller must fail so the platform retries the start.
func (l *RunLifecycle) PublishStarted(ctx context.Context, spec *agentos.RunSpec, status *agentos.RunStatus) error {
	if spec == nil || spec.RunID == "" {
		return nil
	}

	scope := &runScope{
		handle:   HandleForRun(spec.AccountID, spec.RunID),
		threadID: spec.ThreadID,
		runID:    spec.RunID,
	}

	l.mu.Lock()
	l.scopes[spec.RunID] = scope
	l.mu.Unlock()

	started := stream.NewRunStarted(spec.ThreadID, spec.RunID)

	if err := l.record(ctx, scope, started); err != nil {
		return err
	}

	l.announce(ctx, scope, started)

	return l.emitTerminal(ctx, scope, status)
}

// PublishStatus mirrors a Status observation. It publishes the terminal
// milestone exactly once per run; non-terminal states and repeats are no-ops.
// The error semantics match PublishStarted.
func (l *RunLifecycle) PublishStatus(ctx context.Context, runID string, status *agentos.RunStatus) error {
	scope := l.lookup(runID)
	if scope == nil {
		return nil
	}

	return l.emitTerminal(ctx, scope, status)
}

// emitTerminal makes the AG-UI terminal milestone matching a terminal
// lifecycle state durable, then releases the run's scope and announces it on
// the bus. The scope is the at-most-once latch: the caller that removes it
// owns the terminal. A persist failure puts the scope back so a retry
// re-attempts the same milestone instead of losing it — a release-then-publish
// order would drop a terminal silently whenever the store hiccuped.
func (l *RunLifecycle) emitTerminal(ctx context.Context, scope *runScope, status *agentos.RunStatus) error {
	if status == nil {
		return nil
	}

	ev := terminalEvent(scope, status)
	if ev == nil {
		return nil
	}

	l.mu.Lock()

	_, owned := l.scopes[scope.runID]
	if owned {
		delete(l.scopes, scope.runID)
	}

	l.mu.Unlock()

	if !owned {
		return nil
	}

	if err := l.record(ctx, scope, ev); err != nil {
		l.restore(scope)

		return err
	}

	l.announce(ctx, scope, ev)

	return nil
}

// restore puts a scope back after a failed terminal record, unless a newer
// scope for the same run appeared meanwhile.
func (l *RunLifecycle) restore(scope *runScope) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, exists := l.scopes[scope.runID]; !exists {
		l.scopes[scope.runID] = scope
	}
}

func (l *RunLifecycle) lookup(runID string) *runScope {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.scopes[runID]
}

// record makes one milestone durable when a recorder is attached; without one
// the adapter is a pure mirror and nothing can fail.
func (l *RunLifecycle) record(ctx context.Context, scope *runScope, ev *stream.Event) error {
	if l.facts == nil {
		return nil
	}

	return l.facts.Record(ctx, scope.handle, ev)
}

// announce sends one event to the bus, fail-open on transport errors.
func (l *RunLifecycle) announce(ctx context.Context, scope *runScope, ev *stream.Event) {
	if l.pub == nil {
		return
	}

	if err := l.pub.Publish(ctx, scope.handle, ev); err != nil {
		l.warnf("streamadapter: publish %s: %v (fail-open)", ev.Type, err)
	}
}

// terminalEvent maps a terminal lifecycle state onto its AG-UI milestone.
// Anything non-terminal maps to nil: the run stays open on the timeline.
func terminalEvent(scope *runScope, status *agentos.RunStatus) *stream.Event {
	switch status.LifecycleState {
	case lifecycleCompleted, lifecycleSucceeded:
		return stream.NewRunFinished(scope.threadID, scope.runID)
	case lifecycleFailed:
		return stream.NewRunError(scope.threadID, scope.runID, runFailure(status))
	case lifecycleCanceled:
		return stream.NewRunCanceled(scope.threadID, scope.runID)
	default:
		return nil
	}
}

// errRunFailed is the sentinel for a remote run that failed without a reason.
var errRunFailed = errors.New("run failed")

// runFailure builds the RUN_ERROR payload from the remote's reason, falling
// back to the generic sentinel when the remote reported none.
func runFailure(status *agentos.RunStatus) error {
	if status.Reason != "" {
		return fmt.Errorf("%w: %s", errRunFailed, status.Reason)
	}

	return errRunFailed
}

func (l *RunLifecycle) warnf(format string, args ...any) {
	if l.logger != nil {
		l.logger.Warn(format, args...)
	}
}
