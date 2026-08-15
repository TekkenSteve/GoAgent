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
