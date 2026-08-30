// Package runprojection is the control plane's read role on the data plane:
// it consumes the same per-run channel the frontend subscribes to, reduces the
// stream to its durable milestones (agentos/stream.ProjectToCore), and persists
// them to Postgres. Token bytes stay on the bus; only milestones and references
// reach the durable projection.
package runprojection

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
)

var (
	errRunProjectionSubscriberRequired = errors.New("agentos run projection: subscriber is required")
	errRunProjectionStoreRequired      = errors.New("agentos run projection: store is required")
)

const (
	// ensureSubscribeTimeout bounds the subscribe call on the activity publish
	// path. The centrifugo client's own connect default is 5s, long enough to
	// stall an LLM streaming heartbeat; the projector is fail-open, so a short
	// bound just means the next activity's first publish retries the attach.
	ensureSubscribeTimeout = 2 * time.Second

	// appendRetryDelay is the single backoff between two append attempts.
	appendRetryDelay = 200 * time.Millisecond

	// appendTimeout bounds one durable append so a hung database stalls the
	// drain instead of silently dropping the rest of the run's timeline.
	appendTimeout = 5 * time.Second

	// defaultReapInterval is how often the watcher scans for stale projections.
	defaultReapInterval = time.Minute

	// defaultProjectionIdleTimeout is how long a projection may sit without a
	// drain event before the reaper closes it as stale.
	defaultProjectionIdleTimeout = 10 * time.Minute
)

// Controller is what the streaming activities hold: an idempotent attach to a
// run's projection subscription. The first publish of each activity fires it
// once; re-attaches (process restart, earlier subscribe failure) re-subscribe
// with the persisted cursor and replay the retained window.
type Controller interface {
	EnsureSubscribed(ctx context.Context, runID string, handle *stream.Handle) error
}

// RunEventStore is the durable milestone sink the projector writes.
type RunEventStore interface {
	LastRunEventSequence(ctx context.Context, runID string) (int64, error)
	AppendRunEvent(ctx context.Context, ev *agentoscore.Event) error
}

// Config wires a run-event projector.
type Config struct {
	// Subscriber reads the data-plane channel (production: centrifugo; tests: memstream).
	Subscriber stream.Subscriber
	// Store persists projected milestones.
	Store RunEventStore
	// Logger is optional; nil drops projection diagnostics.
	Logger logger.Interface
	// ReapInterval controls how often the watcher scans for stale projections.
	// Zero applies defaultReapInterval.
	ReapInterval time.Duration
	// ProjectionIdleTimeout is how long a projection may sit without a drain
	// event before it is reaped. Zero applies defaultProjectionIdleTimeout.
	ProjectionIdleTimeout time.Duration
}

// runProjection is one run's live subscription and the handle it reads.
type runProjection struct {
	handle *stream.Handle
	sub    *stream.Subscription

	// lastActivity is the UnixNano timestamp of the projection's last drain
	// event, written lock-free from register and drain, read by reap.
	lastActivity atomic.Int64
}

// RunEventProjector attaches to run channels on demand and drains their
// milestones into the durable store. Shutdown mirrors the plan metrics loop:
// cancel, then wait for every drain to release its subscription.
type RunEventProjector struct {
	sub          stream.Subscriber
	store        RunEventStore
	logger       logger.Interface
	reapInterval time.Duration
	idleTimeout  time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once

	mu     sync.Mutex
	closed bool
	subs   map[string]*runProjection
	wg     sync.WaitGroup
}

var _ Controller = (*RunEventProjector)(nil)

// New creates a projector and starts its shutdown watcher.
func New(parent context.Context, cfg Config) (*RunEventProjector, error) {
	if cfg.Subscriber == nil {
		return nil, errRunProjectionSubscriberRequired
	}

	if cfg.Store == nil {
		return nil, errRunProjectionStoreRequired
	}

	reapInterval := cfg.ReapInterval
	if reapInterval <= 0 {
		reapInterval = defaultReapInterval
	}

	idleTimeout := cfg.ProjectionIdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = defaultProjectionIdleTimeout
	}

	ctx, cancel := context.WithCancel(parent)
	p := &RunEventProjector{
		sub:          cfg.Subscriber,
		store:        cfg.Store,
		logger:       cfg.Logger,
		reapInterval: reapInterval,
		idleTimeout:  idleTimeout,
		ctx:          ctx,
		cancel:       cancel,
		done:         make(chan struct{}),
		subs:         make(map[string]*runProjection),
	}

	go p.run(ctx)

	return p, nil
}

// EnsureSubscribed attaches the projector to a run's channel unless one is
// already active, resuming from the highest persisted sequence. It is
// fail-open: a subscriber or store error is returned (the caller logs and the
// next publish retries), never blocking the run.
func (p *RunEventProjector) EnsureSubscribed(ctx context.Context, runID string, handle *stream.Handle) error {
	p.mu.Lock()
	subscribed := p.closed

	if !subscribed {
		_, subscribed = p.subs[runID]
	}

	p.mu.Unlock()

	if subscribed {
		return nil
	}

	cursor, err := p.store.LastRunEventSequence(ctx, runID)
	if err != nil {
		return fmt.Errorf("runprojection - ensure %s - cursor: %w", runID, err)
	}

	subscribeCtx, cancel := context.WithTimeout(ctx, ensureSubscribeTimeout)
	sub, err := p.sub.Subscribe(subscribeCtx, handle, cursor)

	cancel()

	if err != nil {
		return fmt.Errorf("runprojection - subscribe %s: %w", runID, err)
	}

	rp, ok := p.register(runID, handle, sub)
	if !ok {
		sub.Close()

		return nil
	}

	go p.drain(runID, rp)

	return nil
}

// register records a fresh subscription under the lock. It returns the
// registered projection and false when the projector is shutting down or a
// subscription already won the race, in which case the caller must close the
// freshly created subscription. The WaitGroup Add happens here — under the same
// lock Close's watcher sets closed before it waits — so Add can never race
// Wait, and the caller's go p.drain is guaranteed to pair with the Add.
func (p *RunEventProjector) register(runID string, handle *stream.Handle, sub *stream.Subscription) (*runProjection, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, false
	}

	if _, ok := p.subs[runID]; ok {
		return nil, false
	}

	rp := &runProjection{handle: handle, sub: sub}
	rp.lastActivity.Store(time.Now().UnixNano())
	p.subs[runID] = rp
	p.wg.Add(1)

	return rp, true
}

// drain reduces one run's channel stream to milestones and persists them. It
// self-terminates on a terminal milestone (run completed/failed): unregister,
// close the subscription, release the WaitGroup.
func (p *RunEventProjector) drain(runID string, rp *runProjection) {
	defer p.wg.Done()

	for stored := range rp.sub.C {
		rp.lastActivity.Store(time.Now().UnixNano())

		core, ok := stream.ProjectToCore(rp.handle, &stored)
		if !ok {
			continue
		}

		core.Sequence = stored.Sequence
		core.EventID = runEventID(runID, stored.Sequence)

		if err := p.appendRunEvent(p.ctx, core); err != nil {
			p.logf("runprojection: persist %s %s: %v (fail-open)", runID, core.EventType, err)
		}

		if isRunTerminal(core.EventType) {
			p.unregister(runID, rp)
			rp.sub.Close()

			return
		}
	}
}

// appendRunEvent persists one milestone with a single retry after a short
// backoff. A failed append is reported so the drain can log-and-continue; the
// persisted cursor plus the bus history window make any gap recoverable on the
// next re-attach.
func (p *RunEventProjector) appendRunEvent(ctx context.Context, ev *agentoscore.Event) error {
	ctx, cancel := context.WithTimeout(ctx, appendTimeout)
	defer cancel()

	if err := p.store.AppendRunEvent(ctx, ev); err == nil {
		return nil
	}

	timer := time.NewTimer(appendRetryDelay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}

	return p.store.AppendRunEvent(ctx, ev)
}

// unregister removes a run's projection unless a newer subscription for the
// same run replaced it (the terminal drain of an older subscription must not
// evict a re-attached one).
func (p *RunEventProjector) unregister(runID string, rp *runProjection) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.subs[runID] == rp {
		delete(p.subs, runID)
	}
}

// run watches for shutdown, periodically reaps stale projections, and on
// shutdown closes every live subscription and waits for all drains to exit.
func (p *RunEventProjector) run(ctx context.Context) {
	defer close(p.done)

	ticker := time.NewTicker(p.reapInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.closeAllSubs()

			return
		case <-ticker.C:
			p.reap()
		}
	}
}

// closeAllSubs closes every live projection subscription so their drains
// unwind, then waits for the drains to exit. Called once from the watcher on
// shutdown, after closed is set so no new subscription can register.
func (p *RunEventProjector) closeAllSubs() {
	p.mu.Lock()
	p.closed = true

	subs := make([]*runProjection, 0, len(p.subs))
	for _, rp := range p.subs {
		subs = append(subs, rp)
	}

	p.mu.Unlock()

	for _, rp := range subs {
		rp.sub.Close()
	}

	p.wg.Wait()
}

// reap closes projections idle past the configured timeout. A run that exits
// without a terminal milestone (an activity error, a hard workflow
// termination) would otherwise pin its drain goroutine forever; the next
// publish re-attaches it via EnsureSubscribed, resuming from the persisted
// cursor, so reaping is fail-open.
func (p *RunEventProjector) reap() {
	cutoff := time.Now().Add(-p.idleTimeout).UnixNano()

	p.mu.Lock()
	stale := make(map[string]*runProjection)

	for runID, rp := range p.subs {
		if rp.lastActivity.Load() < cutoff {
			delete(p.subs, runID)
			stale[runID] = rp
		}
	}

	p.mu.Unlock()

	for runID, rp := range stale {
		p.logf("runprojection: reap stale projection %s", runID)
		rp.sub.Close()
	}
}

// Close stops the projector and waits for every drain to release its
// subscription. Safe to call more than once.
func (p *RunEventProjector) Close() {
	p.once.Do(p.shutdown)
}

// shutdown performs the one-time stop: cancel the watcher and wait for every
// drain to release its subscription. It lives in its own method (rather than
// inline in the once.Do closure) so the cancel call is on the receiver body,
// which the context-propagation analyzer can trace back to the WithCancel in
// New.
func (p *RunEventProjector) shutdown() {
	p.cancel()

	<-p.done
}

func (p *RunEventProjector) logf(format string, args ...any) {
	if p.logger != nil {
		p.logger.Warn(format, args...)
	}
}

// runEventID derives the stable durable id of a projected milestone from its
// run scope and bus offset, mirroring the plan event id shape.
func runEventID(runID string, sequence int64) string {
	return fmt.Sprintf("%s:%d", runID, sequence)
}

// isRunTerminal reports whether a projected milestone ends the run's timeline.
// Canceled is terminal too: a run that exits via the agent-command cancel
// signal publishes a canceled milestone, so its projection self-closes.
func isRunTerminal(typ agentoscore.EventType) bool {
	return typ == agentoscore.EventRunCompleted || typ == agentoscore.EventRunFailed || typ == agentoscore.EventRunCanceled
}
