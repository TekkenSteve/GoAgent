package streamadapter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
)

var errFactStoreRequired = errors.New("streamadapter: fact store is required")

// factCursorTimeout bounds the one cursor read a run's first milestone pays.
// It runs on the milestone path, so a hung database must stall the fact write
// visibly rather than block the writer indefinitely; the append that follows
// carries the real timeout semantics.
const factCursorTimeout = 2 * time.Second

// factRetryDelay is the single backoff between two append attempts, mirroring
// the bus-riding projector this recorder replaces: one retry absorbs a
// transient hiccup for writers with no platform retry of their own (the dsh
// consume loop), while Temporal-wrapped writers get the surfaced error on top.
const factRetryDelay = 200 * time.Millisecond

// RunEventStore is the durable milestone sink the recorder appends to. It is
// satisfied by the run-event repository, whose AppendRunEvent persists the
// event and its fact-log publication intent in one transaction.
type RunEventStore interface {
	// LastRunEventSequence returns the highest persisted sequence for a run.
	LastRunEventSequence(ctx context.Context, runID string) (int64, error)
	// AppendRunEvent persists one milestone idempotently.
	AppendRunEvent(ctx context.Context, ev *agentoscore.Event) error
}

// MilestoneRecorder makes a milestone durable at the writer, before it is
// announced on the bus: the fact-first half of the data-plane ruling that
// keeps the fact path independent of the live channel.
//
// The recorder allocates each run's durable sequence itself — a dense
// per-run ordinal, lazily bootstrapped from the store's cursor — because the
// bus offset cannot be the fact's identity: a fact that never touches the bus
// still needs one, and a bus that assigns a different offset on replay must
// not be able to change it.
//
// One recorder serves a whole process, and a run's milestones are written by
// one writer at a time (Temporal serializes a run's activities), so the
// in-memory cursor cannot race itself. Concurrent writers for the same run
// would collide on the (run_id, sequence) key and drop one milestone with a
// logged error — an invariant violation to surface, not a case to mask.
//
// A persist failure is returned, never swallowed: the enclosing activity
// fails and Temporal retries it, which is the designed failure mode for a
// Postgres outage on the fact path. The bus publish that follows a
// successful record stays fail-open.
type MilestoneRecorder struct {
	store  RunEventStore
	logger logger.Interface

	mu      sync.Mutex
	cursors map[string]int64
}

// NewMilestoneRecorder creates a recorder over the durable run-event store.
// The logger may be nil; record failures are then returned silently to the
// caller, which reports them through its own path.
func NewMilestoneRecorder(store RunEventStore, l logger.Interface) (*MilestoneRecorder, error) {
	if store == nil {
		return nil, errFactStoreRequired
	}

	return &MilestoneRecorder{
		store:   store,
		logger:  l,
		cursors: make(map[string]int64),
	}, nil
}

// Record reduces one milestone event to its durable form and appends it with
// its fact-log publication intent. Non-milestones are a no-op (the reduction
// vocabulary lives in stream.ProjectToCore, the single classification point).
// One failed append is retried once after a short backoff — the same policy
// the bus-riding projector applied.
//
// The returned error means the fact is not durable; the caller must fail the
// run rather than announce the milestone on the bus.
func (r *MilestoneRecorder) Record(ctx context.Context, handle *stream.Handle, ev *stream.Event) error {
	if ev == nil {
		return nil
	}

	core, ok := stream.ProjectToCore(handle, &stream.StoredEvent{Event: *ev})
	if !ok {
		return nil
	}

	core.Sequence = r.next(ev.RunID)

	if err := r.append(ctx, core); err != nil {
		return fmt.Errorf("streamadapter - record %s %s: %w", ev.RunID, ev.Type, err)
	}

	return nil
}

// append persists one milestone with a single retry after a short backoff,
// bounded by the caller's context so a canceled run stops waiting.
func (r *MilestoneRecorder) append(ctx context.Context, core *agentoscore.Event) error {
	if err := r.store.AppendRunEvent(ctx, core); err == nil {
		return nil
	}

	timer := time.NewTimer(factRetryDelay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}

	return r.store.AppendRunEvent(ctx, core)
}

// next allocates the run's next durable sequence. The first milestone of a
// run bootstraps from the store's cursor, so a restarted process resumes
// where the durable timeline ends; every later milestone increments the
// in-memory cursor under the recorder's lock.
func (r *MilestoneRecorder) next(runID string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	cursor, ok := r.cursors[runID]
	if !ok {
		cursor = r.bootstrap(runID)
	}

	cursor++

	r.cursors[runID] = cursor

	return cursor
}

// bootstrap reads the run's durable cursor, assumed zero for a run this
// process has not seen when the read cannot be made — the append that
// follows rejects a colliding sequence and the collision is logged loudly
// rather than masked. It runs under the recorder's lock, so one bootstrap
// happens per run per process.
func (r *MilestoneRecorder) bootstrap(runID string) int64 {
	ctx, cancel := context.WithTimeout(context.Background(), factCursorTimeout)
	defer cancel()

	cursor, err := r.store.LastRunEventSequence(ctx, runID)
	if err != nil {
		r.warnf("streamadapter: read %s fact cursor: %v (assuming 0)", runID, err)

		return 0
	}

	return cursor
}

func (r *MilestoneRecorder) warnf(format string, args ...any) {
	if r.logger != nil {
		r.logger.Warn(format, args...)
	}
}
