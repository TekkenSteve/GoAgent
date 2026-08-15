package persistent

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
)

const (
	// agentosRunEventLastSequenceSQL reads the highest persisted bus offset for
	// a run — the resume cursor for a projector re-attaching after a reconnect
	// or process restart. COALESCE makes an empty run report 0 (replay from the
	// start of the retained window).
	agentosRunEventLastSequenceSQL = `
SELECT COALESCE(MAX(sequence), 0)
FROM agentos_run_events
WHERE run_id = $1`

	// agentosRunEventInsertSQL persists one projected milestone. The bus offset
	// (StoredEvent.Sequence) IS the idempotency token: the same (run_id,
	// sequence) always carries the same event, so a replay conflict is success
	// rather than an error.
	agentosRunEventInsertSQL = `
INSERT INTO agentos_run_events (
    run_id,
    sequence,
    event_id,
    event_type,
    thread_id,
    process_id,
    source,
    occurred_at,
    payload
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT (run_id, sequence) DO NOTHING`

	// agentosRunEventListSQL reads a run's projected milestones in sequence
	// order, resuming after a cursor. This is the run's authoritative history:
	// the same durable timeline the projector wrote, minus the transient byte
	// deltas that only ever live in the bus history window.
	agentosRunEventListSQL = `
SELECT event_id, event_type, thread_id, process_id, source, occurred_at, payload, sequence
FROM agentos_run_events
WHERE run_id = $1 AND sequence > $2
ORDER BY sequence
LIMIT $3`

	// defaultRunEventsLimit and maxRunEventsLimit bound history reads so one
	// request can never page an entire timeline at once.
	defaultRunEventsLimit = 100
	maxRunEventsLimit     = 500
)

// AgentOSRunEventRepo persists the projected AgentOS run milestone timeline —
// the durable half of the data plane's projection consumption.
type AgentOSRunEventRepo struct {
	*postgres.Postgres
}

// NewAgentOSRunEventRepo creates a Postgres-backed run event repository.
func NewAgentOSRunEventRepo(pg *postgres.Postgres) *AgentOSRunEventRepo {
	return &AgentOSRunEventRepo{pg}
}

// LastRunEventSequence returns the highest persisted sequence for a run, or 0
// when nothing has been projected yet.
func (r *AgentOSRunEventRepo) LastRunEventSequence(ctx context.Context, runID string) (int64, error) {
	var sequence int64

	if err := r.Pool.QueryRow(ctx, agentosRunEventLastSequenceSQL, runID).Scan(&sequence); err != nil {
		return 0, fmt.Errorf("AgentOSRunEventRepo - LastRunEventSequence - query: %w", err)
	}

	return sequence, nil
}

// AppendRunEvent persists one projected milestone idempotently. Re-append of an
// already-stored (run_id, sequence) is a no-op, not an error, so replay after a
// reconnect never duplicates a milestone.
func (r *AgentOSRunEventRepo) AppendRunEvent(ctx context.Context, ev *agentoscore.Event) error {
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

	payloadJSON, err := json.Marshal(ev.Payload)
	if err != nil {
		return fmt.Errorf("AgentOSRunEventRepo - AppendRunEvent - marshal payload: %w", err)
	}

	_, err = r.Pool.Exec(ctx, agentosRunEventInsertSQL,
		ev.RunID,
		ev.Sequence,
		ev.EventID,
		string(ev.EventType),
		ev.ThreadID,
		ev.ProcessID,
		ev.Source,
		ev.Timestamp,
		payloadJSON,
	)
	if err != nil {
		return fmt.Errorf("AgentOSRunEventRepo - AppendRunEvent - insert: %w", err)
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

	rows, err := r.Pool.Query(ctx, agentosRunEventListSQL, runID, after, limit)
	if err != nil {
		return nil, fmt.Errorf("AgentOSRunEventRepo - ListRunEvents - query: %w", err)
	}
	defer rows.Close()

	events := make([]agentoscore.Event, 0, limit)

	for rows.Next() {
		var (
			ev          agentoscore.Event
			eventType   string
			payloadJSON []byte
		)

		if err := rows.Scan(
			&ev.EventID,
			&eventType,
			&ev.ThreadID,
			&ev.ProcessID,
			&ev.Source,
			&ev.Timestamp,
			&payloadJSON,
			&ev.Sequence,
		); err != nil {
			return nil, fmt.Errorf("AgentOSRunEventRepo - ListRunEvents - scan: %w", err)
		}

		ev.EventType = agentoscore.EventType(eventType)
		if len(payloadJSON) > 0 {
			if err := json.Unmarshal(payloadJSON, &ev.Payload); err != nil {
				return nil, fmt.Errorf("AgentOSRunEventRepo - ListRunEvents - unmarshal payload: %w", err)
			}
		}

		ev.RunID = runID
		events = append(events, ev)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("AgentOSRunEventRepo - ListRunEvents - rows: %w", err)
	}

	return events, nil
}
