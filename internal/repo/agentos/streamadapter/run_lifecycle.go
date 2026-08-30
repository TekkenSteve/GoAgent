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
// Publishing is fail-open by design — a bus hiccup must never break the run,
// mirroring PublishWriter. A nil publisher degrades the adapter to a no-op. A
// run's scope is retained until a terminal milestone is published, so the
// milestone is sent at most once; abandoned runs hold their scope for the
// process lifetime, mirroring the run backend index.
type RunLifecycle struct {
	pub    stream.Publisher
	logger logger.Interface

	// ensure is an optional projection-attach hook fired once per run before
	// its first publish, mirroring PublishWriter.WithEnsure. It takes the run's
	// id and handle so the caller can attach per-run (runprojection's
	// EnsureSubscribed needs both).
	ensure func(context.Context, string, *stream.Handle) error

	mu     sync.Mutex
	scopes map[string]*runScope
}

// runScope is one started run's data-plane identity plus its per-run ensure
// latch. The scope is dropped once a terminal milestone is published, which
// also serves as the dedup marker: a later terminal Status has no scope to
// look up and is a no-op.
type runScope struct {
	handle     *stream.Handle
	threadID   string
	runID      string
	ensure     func(context.Context, string, *stream.Handle) error
	ensureOnce sync.Once
}

// NewRunLifecycle creates a lifecycle adapter. The logger may be nil;
// publishing failures are then dropped silently.
func NewRunLifecycle(pub stream.Publisher, l logger.Interface) *RunLifecycle {
	return &RunLifecycle{
		pub:    pub,
		logger: l,
		scopes: make(map[string]*runScope),
	}
}

// WithEnsure installs an optional projection-attach hook fired once per run
// before its first publish, mirroring PublishWriter.WithEnsure. A failing hook
// is logged and the next publish retries.
func (l *RunLifecycle) WithEnsure(fn func(context.Context, string, *stream.Handle) error) *RunLifecycle {
	l.ensure = fn

	return l
}

// PublishStarted opens a run's timeline after Start succeeds. It records the
// run's data-plane scope for later terminal milestones, publishes RUN_STARTED,
// and — when the remote already reports a terminal state at start — the
// terminal milestone too.
func (l *RunLifecycle) PublishStarted(ctx context.Context, spec *agentos.RunSpec, status *agentos.RunStatus) {
	if spec == nil || spec.RunID == "" {
		return
	}

	scope := &runScope{
		handle:   HandleForRun(spec.AccountID, spec.RunID),
		threadID: spec.ThreadID,
		runID:    spec.RunID,
		ensure:   l.ensure,
	}

	l.mu.Lock()
	l.scopes[spec.RunID] = scope
	l.mu.Unlock()

	l.publish(ctx, scope, stream.NewRunStarted(spec.ThreadID, spec.RunID))
	l.publishTerminal(ctx, scope, status)
}

// PublishStatus mirrors a Status observation. It publishes the terminal
// milestone exactly once per run; non-terminal states and repeats are no-ops.
func (l *RunLifecycle) PublishStatus(ctx context.Context, runID string, status *agentos.RunStatus) {
	scope := l.lookup(runID)
	if scope == nil {
		return
	}

	l.publishTerminal(ctx, scope, status)
}

// publishTerminal publishes the AG-UI terminal milestone matching a terminal
// lifecycle state, then releases the run's scope so the milestone is sent at
// most once. The check-and-release is atomic under l.mu; only the caller that
// wins it publishes.
func (l *RunLifecycle) publishTerminal(ctx context.Context, scope *runScope, status *agentos.RunStatus) {
	if status == nil {
		return
	}

	ev := terminalEvent(scope, status)
	if ev == nil {
		return
	}

	l.mu.Lock()

	_, published := l.scopes[scope.runID]
	if published {
		delete(l.scopes, scope.runID)
	}
	l.mu.Unlock()

	if !published {
		return
	}

	l.publish(ctx, scope, ev)
}

func (l *RunLifecycle) lookup(runID string) *runScope {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.scopes[runID]
}

// publish sends one event to the bus, fail-open on transport errors. Before
// the first publish of the run it fires the optional projection-attach hook.
func (l *RunLifecycle) publish(ctx context.Context, scope *runScope, ev *stream.Event) {
	if l.pub == nil {
		return
	}

	scope.ensureOnce.Do(func() {
		if scope.ensure != nil {
			if err := scope.ensure(ctx, scope.runID, scope.handle); err != nil {
				l.warnf("streamadapter: ensure projection subscribe: %v (fail-open)", err)
			}
		}
	})

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
