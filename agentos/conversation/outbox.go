// Package conversation implements a durable conversation runtime for AgentOS agents.
package conversation

import (
	"context"
	"errors"
	"fmt"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	"github.com/jackc/pgx/v5"
)

func (r *Runtime) wakeConversationOutbox() {
	if r == nil || r.stream == nil {
		return
	}

	select {
	case r.outboxWake <- struct{}{}:
	default:
	}
}

func (r *Runtime) runConversationOutbox(ctx context.Context) {
	ticker := time.NewTicker(r.outboxPollInterval)
	defer ticker.Stop()

	for {
		for {
			published, err := r.publishConversationOutboxBatch(ctx)
			if err != nil || published == 0 {
				break
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-r.outboxWake:
		case <-ticker.C:
		}
	}
}

func (r *Runtime) publishConversationOutboxBatch(ctx context.Context) (int, error) {
	options := pgx.TxOptions{}

	result, err := withConversationTx(ctx, r.pool, &options, func(tx pgx.Tx) (outboxPublishResult, error) {
		return r.publishConversationOutboxTx(ctx, tx)
	})
	if err != nil {
		return 0, err
	}

	return result.published, result.publishErr
}

type outboxPublishResult struct {
	published  int
	publishErr error
}

type outboxBatch struct {
	threadID  string
	accountID string
	projectID string
	events    []agentos.ConversationEvent
}

func (r *Runtime) publishConversationOutboxTx(ctx context.Context, tx pgx.Tx) (outboxPublishResult, error) {
	batch, err := loadOutboxBatch(ctx, tx)
	if err != nil || len(batch.events) == 0 {
		return outboxPublishResult{}, err
	}

	if publishErr := r.publishOutboxEvents(ctx, tx, &batch); publishErr != nil {
		return outboxPublishResult{publishErr: publishErr}, nil
	}

	lastSequence := batch.events[len(batch.events)-1].Sequence
	if _, err := tx.Exec(ctx, `DELETE FROM agentos_conversation_event_outbox WHERE thread_id = $1 AND sequence <= $2`, batch.threadID, lastSequence); err != nil {
		return outboxPublishResult{}, fmt.Errorf("agentos conversation: delete published outbox: %w", err)
	}

	return outboxPublishResult{published: len(batch.events)}, nil
}

func loadOutboxBatch(ctx context.Context, tx pgx.Tx) (outboxBatch, error) {
	var threadID string

	err := tx.QueryRow(ctx, `
		SELECT o.thread_id
		FROM agentos_conversation_event_outbox o
		WHERE o.sequence = (
			SELECT MIN(first.sequence) FROM agentos_conversation_event_outbox first
			WHERE first.thread_id = o.thread_id
		) AND o.available_at <= NOW()
		ORDER BY o.created_at, o.thread_id LIMIT 1`).Scan(&threadID)
	if errors.Is(err, pgx.ErrNoRows) {
		return outboxBatch{}, nil
	}

	if err != nil {
		return outboxBatch{}, fmt.Errorf("agentos conversation: select outbox thread: %w", err)
	}

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, threadID); err != nil {
		return outboxBatch{}, fmt.Errorf("agentos conversation: lock outbox thread: %w", err)
	}

	rows, err := tx.Query(ctx, `
		SELECT e.event_id, e.thread_id, e.run_id, e.process_id, e.sequence, e.source_event_id,
		       e.source_sequence, e.event_type, e.occurred_at, e.payload
		FROM agentos_conversation_event_outbox o
		JOIN agentos_conversation_events e USING (thread_id, sequence)
		WHERE o.thread_id = $1
		  AND (SELECT MIN(sequence) FROM agentos_conversation_event_outbox WHERE thread_id = $1) =
		      (SELECT MIN(sequence) FROM agentos_conversation_event_outbox WHERE thread_id = $1 AND available_at <= NOW())
		ORDER BY e.sequence LIMIT 100 FOR UPDATE OF o`, threadID)
	if err != nil {
		return outboxBatch{}, fmt.Errorf("agentos conversation: read outbox events: %w", err)
	}

	events, err := scanEvents(rows)
	rows.Close()

	if err != nil {
		return outboxBatch{}, err
	}

	if len(events) == 0 {
		return outboxBatch{}, nil
	}

	var accountID, projectID string
	if err := tx.QueryRow(ctx, `SELECT account_id, project_id FROM agentos_threads WHERE thread_id = $1`, threadID).Scan(&accountID, &projectID); err != nil {
		return outboxBatch{}, fmt.Errorf("agentos conversation: read outbox tenant: %w", err)
	}

	return outboxBatch{threadID: threadID, accountID: accountID, projectID: projectID, events: events}, nil
}

func (r *Runtime) publishOutboxEvents(ctx context.Context, tx pgx.Tx, batch *outboxBatch) error {
	for i := range batch.events {
		if err := r.stream.Publish(ctx, &batch.events[i], batch.accountID, batch.projectID); err != nil {
			_, updateErr := tx.Exec(ctx, `
				UPDATE agentos_conversation_event_outbox
				SET attempts = attempts + 1, last_error = $2, available_at = NOW() + INTERVAL '1 second'
				WHERE thread_id = $1`, batch.threadID, err.Error())
			if updateErr != nil {
				return errors.Join(err, fmt.Errorf("agentos conversation: record outbox failure: %w", updateErr))
			}

			return err
		}
	}

	return nil
}
