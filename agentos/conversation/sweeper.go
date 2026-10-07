package conversation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	"github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/jackc/pgx/v5"
)

// This file holds the runtime's sweeper: the loop that keeps a conversation
// thread from being locked by a run that no longer exists, and that applies the
// projections which had to wait for it.
//
// Both halves exist because a turn's RUN_STARTED is what takes the thread, and
// a thread with a running run refuses another. A run whose owner died holds
// that lock forever; a steer turn, admitted while its predecessor streams,
// waits for it. The sweeper abandons the first and starts the second.

const (
	// defaultRunLease is how long a running run may go without producing a
	// fact before the sweeper treats it as dead. It is generous on purpose: a
	// long tool call or a slow model produces nothing for minutes at a time,
	// and abandoning a live run would cut real work short.
	defaultRunLease = 30 * time.Minute
	// defaultLeaseSweepInterval is how often the sweeper looks. A lease is
	// minutes long, so seconds of granularity are plenty and the query scans a
	// partial index over running runs only.
	defaultLeaseSweepInterval = time.Minute
	// abandonedRunBatch bounds one sweep, so a backlog is worked off in
	// pieces instead of holding a transaction open over all of it.
	abandonedRunBatch = 32
)

// sweepOnce performs one pass: abandon runs whose lease expired, then apply the
// projections that were waiting for a free thread.
func (r *Runtime) sweepOnce(ctx context.Context) (abandoned, projected int, err error) {
	abandoned, err = r.abandonExpiredRuns(ctx)
	if err != nil {
		return abandoned, 0, err
	}

	projected, err = r.applyDeferredProjections(ctx)

	return abandoned, projected, err
}

// abandonExpiredRuns terminates running runs that have produced nothing within
// their lease, by appending the terminal fact they never delivered.
//
// The fact is a RUN_FINISHED with outcome "abandoned": the run's history ends
// with an explicit, auditable reason, and the thread's turn is released,
// because nothing else can release it.
func (r *Runtime) abandonExpiredRuns(ctx context.Context) (int, error) {
	return withConversationTx(ctx, r.pool, &pgx.TxOptions{}, func(tx pgx.Tx) (int, error) {
		expired, err := expiredRunningRuns(ctx, tx, r.runLease, abandonedRunBatch)
		if err != nil {
			return 0, err
		}

		abandoned := 0

		for i := range expired {
			if err := r.abandonRun(ctx, tx, &expired[i]); err != nil {
				return abandoned, err
			}

			abandoned++
		}

		return abandoned, nil
	})
}

// expiredRunningRuns selects running runs whose last activity predates the
// lease. SKIP LOCKED lets several workers sweep at once without waiting on one
// another: a run another worker took is simply not this worker's business.
func expiredRunningRuns(ctx context.Context, tx pgx.Tx, lease time.Duration, limit int) ([]agentos.ConversationRun, error) {
	rows, err := tx.Query(ctx, `
		SELECT run_id, thread_id, process_id, account_id, project_id
		FROM agentos_conversation_runs
		WHERE status = 'running' AND last_activity_at < $1
		ORDER BY last_activity_at
		LIMIT $2
		FOR UPDATE SKIP LOCKED`, time.Now().UTC().Add(-lease), limit)
	if err != nil {
		return nil, fmt.Errorf("agentos conversation: select expired runs: %w", wrapProjectionError(err))
	}
	defer rows.Close()

	var expired []agentos.ConversationRun

	for rows.Next() {
		var run agentos.ConversationRun
		if err := rows.Scan(&run.RunID, &run.ThreadID, &run.ProcessID, &run.AccountID, &run.ProjectID); err != nil {
			return nil, fmt.Errorf("agentos conversation: scan expired run: %w", err)
		}

		expired = append(expired, run)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("agentos conversation: iterate expired runs: %w", err)
	}

	return expired, nil
}

// abandonRun writes the terminal fact for one expired run through the same
// ingestion path every other fact takes, so ordering, the source cursor and the
// projection stay consistent with how the run's history was written.
func (r *Runtime) abandonRun(ctx context.Context, tx pgx.Tx, run *agentos.ConversationRun) error {
	_, lastSourceSequence, err := lockRun(ctx, tx, run.RunID)
	if err != nil {
		return err
	}

	// Re-checked under the row lock: a real terminal event may have landed
	// since the sweep selected this run.
	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM agentos_conversation_runs WHERE run_id = $1`, run.RunID).Scan(&status); err != nil {
		return fmt.Errorf("agentos conversation: reread run status: %w", err)
	}

	if status != agentos.ConversationRunRunning {
		return nil
	}

	event := &agentos.ExternalConversationEvent{
		ThreadID:       run.ThreadID,
		RunID:          run.RunID,
		ProcessID:      run.ProcessID,
		AccountID:      run.AccountID,
		ProjectID:      run.ProjectID,
		SourceEventID:  abandonedSourceEventID(run.RunID),
		SourceSequence: lastSourceSequence + 1,
		EventType:      agentos.ConversationEventRunFinished,
		OccurredAt:     time.Now().UTC(),
		Payload: map[string]any{
			"outcome":            agentos.ConversationOutcomeAbandoned,
			"termination_reason": "lease_expired",
		},
	}

	_, err = r.persistEvent(ctx, tx, event)

	return err
}

// abandonedSourceEventID is the deterministic identity of the abandonment fact.
// A second sweep, or a second worker, writes the same source event id and
// therefore appends nothing: ingestion is idempotent on it.
func abandonedSourceEventID(runID string) string {
	return "lease-expired:" + runID
}

// applyDeferredProjections applies projections that were parked while another
// run held the thread. It takes the earliest parked projection per thread, in
// the order the thread produced them: the thread's turn is a sequence, so
// starting a later turn before an earlier one would reorder the conversation.
func (r *Runtime) applyDeferredProjections(ctx context.Context) (int, error) {
	return withConversationTx(ctx, r.pool, &pgx.TxOptions{}, func(tx pgx.Tx) (int, error) {
		deferred, err := nextDeferredProjections(ctx, tx, abandonedRunBatch)
		if err != nil {
			return 0, err
		}

		applied := 0

		for _, entry := range deferred {
			done, err := r.applyDeferredProjection(ctx, tx, entry)
			if err != nil {
				return applied, err
			}

			if done {
				applied++
			}
		}

		return applied, nil
	})
}

// deferredProjection is one parked projection: the event to apply.
type deferredProjection struct {
	threadID  string
	sequence  int64
	runID     string
	eventType string
}

// nextDeferredProjections returns the earliest parked projection of each thread
// that currently has no running run — the state the parked RUN_STARTED was
// waiting for.
func nextDeferredProjections(ctx context.Context, tx pgx.Tx, limit int) ([]deferredProjection, error) {
	rows, err := tx.Query(ctx, `
		SELECT d.thread_id, d.sequence, d.run_id, d.event_type
		FROM agentos_conversation_deferred_projections d
		WHERE NOT EXISTS (
			SELECT 1 FROM agentos_conversation_runs r
			WHERE r.thread_id = d.thread_id AND r.status = 'running'
		)
		ORDER BY d.thread_id, d.sequence
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("agentos conversation: select deferred projections: %w", err)
	}
	defer rows.Close()

	var deferred []deferredProjection

	for rows.Next() {
		var entry deferredProjection
		if err := rows.Scan(&entry.threadID, &entry.sequence, &entry.runID, &entry.eventType); err != nil {
			return nil, fmt.Errorf("agentos conversation: scan deferred projection: %w", err)
		}

		deferred = append(deferred, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("agentos conversation: iterate deferred projections: %w", err)
	}

	return deferred, nil
}

// applyDeferredProjection applies one parked projection from the stored fact.
// The fact is already durable, so this never rewrites it: it replays the
// projection, and drops the queue row once the projection is in place.
func (r *Runtime) applyDeferredProjection(ctx context.Context, tx pgx.Tx, entry deferredProjection) (bool, error) {
	run, _, err := lockRun(ctx, tx, entry.runID)
	if err != nil {
		return false, err
	}

	event, found, err := storedEvent(ctx, tx, entry.threadID, entry.sequence)
	if err != nil || !found {
		return false, err
	}

	if err := applyEventProjection(ctx, tx, &run, event); err != nil {
		if errors.Is(err, ErrRunAlreadyActive) {
			// The thread was taken again between selection and projection
			// (a concurrent worker, or a real start). The queue keeps the row
			// and the next sweep tries again; the attempt counter is what makes
			// a projection that never drains visible.
			return false, recordDeferredAttempt(ctx, tx, entry)
		}

		return false, err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM agentos_conversation_deferred_projections WHERE thread_id = $1 AND sequence = $2`, entry.threadID, entry.sequence); err != nil {
		return false, fmt.Errorf("agentos conversation: clear deferred projection: %w", err)
	}

	return true, nil
}

func recordDeferredAttempt(ctx context.Context, tx pgx.Tx, entry deferredProjection) error {
	if _, err := tx.Exec(ctx, `
		UPDATE agentos_conversation_deferred_projections
		SET attempts = attempts + 1
		WHERE thread_id = $1 AND sequence = $2`, entry.threadID, entry.sequence); err != nil {
		return fmt.Errorf("agentos conversation: record deferred attempt: %w", err)
	}

	return nil
}

// storedEvent reads one stored fact back as the event a projection applies.
// The fact was written by ingestion, so it is read with the same shape the
// ingestion path stores.
func storedEvent(ctx context.Context, tx pgx.Tx, threadID string, sequence int64) (*agentos.ExternalConversationEvent, bool, error) {
	var (
		event       agentos.ExternalConversationEvent
		eventType   string
		payloadJSON []byte
	)

	row := tx.QueryRow(ctx, `
		SELECT run_id, process_id, source_event_id, source_sequence, event_type, occurred_at, payload
		FROM agentos_conversation_events
		WHERE thread_id = $1 AND sequence = $2`, threadID, sequence)

	if err := row.Scan(&event.RunID, &event.ProcessID, &event.SourceEventID, &event.SourceSequence, &eventType, &event.OccurredAt, &payloadJSON); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("agentos conversation: load deferred event: %w", err)
	}

	event.ThreadID = threadID
	event.EventType = core.EventType(eventType)

	if err := json.Unmarshal(payloadJSON, &event.Payload); err != nil {
		return nil, false, fmt.Errorf("agentos conversation: decode deferred payload: %w", err)
	}

	return &event, true, nil
}
