package persistent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/repo/persistent/sqlcgen"
	"github.com/jackc/pgx/v5"
)

const (
	// defaultRunEventsLimit and maxRunEventsLimit bound history reads so one
	// request can never page an entire timeline at once.
	defaultRunEventsLimit = 100
	maxRunEventsLimit     = 500
)

// AgentOSRunEventRepo persists the projected AgentOS run milestone timeline —
// the durable half of the data plane's projection consumption.
//
// The SQL lives in queries/run_outbox.sql and sqlc generates the parameter
// and row bindings: a column the migrations do not define, or a scan whose
// types do not match, is a generate-time failure rather than a runtime one.
type AgentOSRunEventRepo struct {
	*postgres.Postgres

	queries *sqlcgen.Queries

	// eventOutbox records each newly stored event's publication intent in the
	// same transaction, so the run timeline reaches the fact log through the
	// backbone drainer instead of a second write path. It is enabled exactly
	// when the deployment runs a drainer.
	eventOutbox bool
}

// RunEventRepoOption configures an AgentOSRunEventRepo.
type RunEventRepoOption func(*AgentOSRunEventRepo)

// WithRunEventOutbox enqueues every newly stored run event for publication to
// the run.timeline domain of the fact log. A deployment without a backbone
// drainer leaves it off: rows nobody drains would only pile up.
func WithRunEventOutbox() RunEventRepoOption {
	return func(r *AgentOSRunEventRepo) {
		r.eventOutbox = true
	}
}

// NewAgentOSRunEventRepo creates a Postgres-backed run event repository.
func NewAgentOSRunEventRepo(pg *postgres.Postgres, opts ...RunEventRepoOption) *AgentOSRunEventRepo {
	repo := &AgentOSRunEventRepo{Postgres: pg, queries: sqlcgen.New(pg.Pool)}

	for _, opt := range opts {
		opt(repo)
	}

	return repo
}

// LastRunEventSequence returns the highest persisted sequence for a run, or 0
// when nothing has been projected yet.
func (r *AgentOSRunEventRepo) LastRunEventSequence(ctx context.Context, runID string) (int64, error) {
	sequence, err := r.queries.LastRunEventSequence(ctx, runID)
	if err != nil {
		return 0, fmt.Errorf("AgentOSRunEventRepo - LastRunEventSequence - query: %w", err)
	}

	return sequence, nil
}

// AppendRunEvent persists one projected milestone idempotently. Re-append of an
// already-stored (run_id, sequence) is a no-op, not an error, so replay after a
// reconnect never duplicates a milestone.
//
// When the outbox is enabled, the event row and its publication intent commit
// in one transaction: a run milestone that is durable is also queued for the
// fact log, or neither — which is what makes the log a derived delivery of
// Postgres rather than a second write path. Only a newly stored event is
// queued; a replayed append enqueues nothing, so an upgrade cannot flood the
// log with history it already carries.
func (r *AgentOSRunEventRepo) AppendRunEvent(ctx context.Context, ev *agentoscore.Event) (err error) {
	if err := normalizeRunEvent(ev); err != nil {
		return err
	}

	payloadJSON, err := json.Marshal(ev.Payload)
	if err != nil {
		return fmt.Errorf("AgentOSRunEventRepo - AppendRunEvent - marshal payload: %w", err)
	}

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("AgentOSRunEventRepo - AppendRunEvent - begin: %w", err)
	}

	defer func() {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("AgentOSRunEventRepo - AppendRunEvent - rollback: %w", rollbackErr))
		}
	}()

	if err := r.storeRunEvent(ctx, r.queries.WithTx(tx), ev, payloadJSON); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("AgentOSRunEventRepo - AppendRunEvent - commit: %w", err)
	}

	return nil
}

// storeRunEvent writes the event and, when this repo carries the outbox, its
// publication intent — both inside the caller's transaction, so a milestone
// that is durable is also queued for the fact log, or neither.
//
// A zero-rows insert means the event is already stored — a replayed append —
// and returns without queueing: the fact was queued when it was first stored,
// or predates the outbox and was published by the path that stored it, so an
// upgrade cannot flood the log with history it already carries.
func (r *AgentOSRunEventRepo) storeRunEvent(ctx context.Context, tx *sqlcgen.Queries, ev *agentoscore.Event, payloadJSON []byte) error {
	stored, err := tx.InsertRunEvent(ctx, sqlcgen.InsertRunEventParams{
		RunID:      ev.RunID,
		Sequence:   ev.Sequence,
		EventID:    ev.EventID,
		EventType:  string(ev.EventType),
		ThreadID:   ev.ThreadID,
		ProcessID:  ev.ProcessID,
		Source:     ev.Source,
		OccurredAt: ev.Timestamp,
		Payload:    payloadJSON,
	})
	if err != nil {
		return fmt.Errorf("AgentOSRunEventRepo - AppendRunEvent - insert: %w", err)
	}

	if stored == 0 {
		return nil
	}

	if !r.eventOutbox {
		return nil
	}

	if err := tx.EnqueueRunEventOutbox(ctx, sqlcgen.EnqueueRunEventOutboxParams{
		RunID:    ev.RunID,
		Sequence: ev.Sequence,
	}); err != nil {
		return fmt.Errorf("AgentOSRunEventRepo - AppendRunEvent - enqueue outbox: %w", err)
	}

	return nil
}

// ListRunEvents returns a run's projected milestones in sequence order,
// resuming after the given cursor. A limit of 0 applies the default; a limit
// beyond the maximum is clamped. Unknown runs return an empty slice.
func (r *AgentOSRunEventRepo) ListRunEvents(ctx context.Context, runID string, after int64, limit int) ([]agentoscore.Event, error) {
	if limit <= 0 {
		limit = defaultRunEventsLimit
	} else if limit > maxRunEventsLimit {
		limit = maxRunEventsLimit
	}

	rows, err := r.queries.ListRunEvents(ctx, sqlcgen.ListRunEventsParams{
		RunID:         runID,
		AfterSequence: after,
		RowLimit:      int32(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("AgentOSRunEventRepo - ListRunEvents - query: %w", err)
	}

	events := make([]agentoscore.Event, 0, limit)

	for i := range rows {
		ev, err := runEventFromListRow(runID, &rows[i])
		if err != nil {
			return nil, err
		}

		events = append(events, ev)
	}

	return events, nil
}

// runEventFromListRow turns one generated history row into the domain event.
func runEventFromListRow(runID string, row *sqlcgen.ListRunEventsRow) (agentoscore.Event, error) {
	ev := agentoscore.Event{
		RunID:     runID,
		EventID:   row.EventID,
		EventType: agentoscore.EventType(row.EventType),
		ThreadID:  row.ThreadID,
		ProcessID: row.ProcessID,
		Source:    row.Source,
		Timestamp: row.OccurredAt,
		Sequence:  row.Sequence,
	}

	if len(row.Payload) > 0 {
		if err := json.Unmarshal(row.Payload, &ev.Payload); err != nil {
			return agentoscore.Event{}, fmt.Errorf("AgentOSRunEventRepo - ListRunEvents - unmarshal payload: %w", err)
		}
	}

	return ev, nil
}

// normalizeRunEvent validates one milestone and fills the fields a producer
// may leave unset: the event id derives from the run and sequence, the
// timestamp from the clock, and the payload from nothing — an explicit empty
// object rather than a null the column's default would otherwise mask.
func normalizeRunEvent(ev *agentoscore.Event) error {
	if ev == nil {
		return fmt.Errorf("%w: run event is required", agentoscore.ErrInvalidRunEvent)
	}

	if ev.RunID == "" {
		return fmt.Errorf("%w: run id is required", agentoscore.ErrInvalidRunEvent)
	}

	if ev.EventType == "" {
		return fmt.Errorf("%w: event type is required", agentoscore.ErrInvalidRunEvent)
	}

	if ev.Sequence <= 0 {
		return fmt.Errorf("%w: sequence must be positive", agentoscore.ErrInvalidRunEvent)
	}

	if ev.EventID == "" {
		ev.EventID = fmt.Sprintf("%s:%d", ev.RunID, ev.Sequence)
	}

	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now().UTC()
	}

	if ev.Payload == nil {
		ev.Payload = map[string]any{}
	}

	return nil
}
