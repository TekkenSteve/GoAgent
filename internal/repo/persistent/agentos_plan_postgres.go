package persistent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"github.com/jackc/pgx/v5"
)

// AgentOSPlanRepo persists RunPlan aggregate state and durable plan events.
type AgentOSPlanRepo struct {
	*postgres.Postgres
}

// NewAgentOSPlanRepo creates a Postgres-backed RunPlan repository.
func NewAgentOSPlanRepo(pg *postgres.Postgres) *AgentOSPlanRepo {
	return &AgentOSPlanRepo{pg}
}

func (r *AgentOSPlanRepo) CreatePlan(ctx context.Context, spec agentos.RunPlanSpec, status agentos.RunPlanStatus) (agentos.RunPlanStatus, bool, error) {
	if status.PlanID == "" {
		status.PlanID = spec.PlanID
	}
	_, existing, exists, err := r.GetPlan(ctx, spec.PlanID)
	if err != nil {
		return agentos.RunPlanStatus{}, false, err
	}
	if exists {
		return existing, false, nil
	}
	if err := r.SavePlanState(ctx, agentosplan.PlanStateSnapshot{
		Spec:           spec,
		Status:         status,
		IdempotencyKey: spec.IdempotencyKey,
	}); err != nil {
		return agentos.RunPlanStatus{}, false, err
	}

	return status, true, nil
}

func (r *AgentOSPlanRepo) GetPlan(ctx context.Context, planID string) (agentos.RunPlanSpec, agentos.RunPlanStatus, bool, error) {
	snapshot, exists, err := r.LoadPlanState(ctx, planID)
	if err != nil || !exists {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, exists, err
	}

	return snapshot.Spec, snapshot.Status, true, nil
}

func (r *AgentOSPlanRepo) UpdatePlanStatus(ctx context.Context, status agentos.RunPlanStatus, idempotencyKey string) error {
	if status.PlanID == "" {
		return fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	snapshot, exists, err := r.LoadPlanState(ctx, status.PlanID)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, status.PlanID)
	}
	snapshot.Status = status
	snapshot.IdempotencyKey = idempotencyKey

	return r.SavePlanState(ctx, snapshot)
}

func (r *AgentOSPlanRepo) SavePlanState(ctx context.Context, snapshot agentosplan.PlanStateSnapshot) error {
	if snapshot.Spec.PlanID == "" {
		return fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	if snapshot.Status.PlanID == "" {
		snapshot.Status.PlanID = snapshot.Spec.PlanID
	}
	if snapshot.Status.LifecycleState == "" {
		snapshot.Status.LifecycleState = agentos.PlanLifecyclePending
	}
	if snapshot.Status.UpdatedAt.IsZero() {
		snapshot.Status.UpdatedAt = time.Now().UTC()
	}

	specJSON, err := json.Marshal(snapshot.Spec)
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - SavePlanState - marshal spec: %w", err)
	}
	statusJSON, err := json.Marshal(snapshot.Status)
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - SavePlanState - marshal status: %w", err)
	}

	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - SavePlanState - begin: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	idempotencyKey := snapshot.Spec.IdempotencyKey
	sql, args, err := r.Builder.
		Insert("plans").
		Columns(
			"plan_id",
			"thread_id",
			"account_id",
			"project_id",
			"idempotency_key",
			"lifecycle_state",
			"reason",
			"spec_json",
			"status_json",
			"requested_at",
		).
		Values(
			snapshot.Spec.PlanID,
			snapshot.Spec.ThreadID,
			snapshot.Spec.AccountID,
			snapshot.Spec.ProjectID,
			idempotencyKey,
			snapshot.Status.LifecycleState,
			snapshot.Status.Reason,
			specJSON,
			statusJSON,
			nullableTime(snapshot.Spec.RequestedAt),
		).
		Suffix(`
ON CONFLICT (plan_id) DO UPDATE SET
    thread_id = EXCLUDED.thread_id,
    account_id = EXCLUDED.account_id,
    project_id = EXCLUDED.project_id,
    lifecycle_state = EXCLUDED.lifecycle_state,
    reason = EXCLUDED.reason,
    spec_json = EXCLUDED.spec_json,
    status_json = EXCLUDED.status_json,
    requested_at = EXCLUDED.requested_at,
    updated_at = NOW()`).
		ToSql()
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - SavePlanState - plan builder: %w", err)
	}
	if _, err := tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("AgentOSPlanRepo - SavePlanState - plan exec: %w", err)
	}

	capabilityByNode := make(map[string]string, len(snapshot.Spec.Nodes))
	for _, node := range snapshot.Spec.Nodes {
		capabilityByNode[node.NodeID] = node.Capability
	}
	for _, node := range snapshot.Status.Nodes {
		if err := r.upsertPlanNode(ctx, tx, snapshot.Spec.PlanID, capabilityByNode[node.NodeID], node); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("AgentOSPlanRepo - SavePlanState - commit: %w", err)
	}

	return nil
}

func (r *AgentOSPlanRepo) upsertPlanNode(ctx context.Context, tx pgx.Tx, planID, capability string, node agentos.PlanNodeStatus) error {
	statusJSON, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - upsertPlanNode - marshal: %w", err)
	}

	sql, args, err := r.Builder.
		Insert("plan_nodes").
		Columns(
			"plan_id",
			"node_id",
			"run_id",
			"backend_kind",
			"backend_name",
			"capability",
			"lifecycle_state",
			"attempts",
			"reason",
			"status_json",
		).
		Values(
			planID,
			node.NodeID,
			node.RunID,
			string(node.Backend.Kind),
			node.Backend.Name,
			capability,
			node.LifecycleState,
			node.Attempts,
			node.Reason,
			statusJSON,
		).
		Suffix(`
ON CONFLICT (plan_id, node_id) DO UPDATE SET
    run_id = EXCLUDED.run_id,
    backend_kind = EXCLUDED.backend_kind,
    backend_name = EXCLUDED.backend_name,
    capability = EXCLUDED.capability,
    lifecycle_state = EXCLUDED.lifecycle_state,
    attempts = EXCLUDED.attempts,
    reason = EXCLUDED.reason,
    status_json = EXCLUDED.status_json,
    updated_at = NOW()`).
		ToSql()
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - upsertPlanNode - builder: %w", err)
	}
	if _, err := tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("AgentOSPlanRepo - upsertPlanNode - exec: %w", err)
	}

	return nil
}

func (r *AgentOSPlanRepo) LoadPlanState(ctx context.Context, planID string) (agentosplan.PlanStateSnapshot, bool, error) {
	if planID == "" {
		return agentosplan.PlanStateSnapshot{}, false, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}

	sql, args, err := r.Builder.
		Select("spec_json", "status_json", "idempotency_key").
		From("plans").
		Where(sq.Eq{"plan_id": planID}).
		ToSql()
	if err != nil {
		return agentosplan.PlanStateSnapshot{}, false, fmt.Errorf("AgentOSPlanRepo - LoadPlanState - builder: %w", err)
	}

	var specJSON []byte
	var statusJSON []byte
	var idempotencyKey string
	err = r.Pool.QueryRow(ctx, sql, args...).Scan(&specJSON, &statusJSON, &idempotencyKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentosplan.PlanStateSnapshot{}, false, nil
		}

		return agentosplan.PlanStateSnapshot{}, false, fmt.Errorf("AgentOSPlanRepo - LoadPlanState - query: %w", err)
	}

	var spec agentos.RunPlanSpec
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return agentosplan.PlanStateSnapshot{}, false, fmt.Errorf("AgentOSPlanRepo - LoadPlanState - decode spec: %w", err)
	}
	var status agentos.RunPlanStatus
	if err := json.Unmarshal(statusJSON, &status); err != nil {
		return agentosplan.PlanStateSnapshot{}, false, fmt.Errorf("AgentOSPlanRepo - LoadPlanState - decode status: %w", err)
	}

	return agentosplan.PlanStateSnapshot{
		Spec:           spec,
		Status:         status,
		IdempotencyKey: idempotencyKey,
	}, true, nil
}

func (r *AgentOSPlanRepo) AppendPlanEvent(ctx context.Context, event agentos.PlanEvent, idempotencyKey string) (agentos.PlanEvent, error) {
	if event.PlanID == "" {
		return agentos.PlanEvent{}, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidPlanEvent)
	}
	if idempotencyKey != "" {
		existing, exists, err := r.planEventByIdempotencyKey(ctx, event.PlanID, idempotencyKey)
		if err != nil || exists {
			return existing, err
		}
	}

	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - AppendPlanEvent - begin: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var sequence int64
	err = tx.QueryRow(ctx, `
UPDATE plans
SET event_sequence = event_sequence + 1
WHERE plan_id = $1
RETURNING event_sequence`, event.PlanID).Scan(&sequence)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentos.PlanEvent{}, fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, event.PlanID)
		}

		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - AppendPlanEvent - next sequence: %w", err)
	}

	if event.Sequence == 0 {
		event.Sequence = sequence
	}
	if event.EventID == "" {
		event.EventID = fmt.Sprintf("%s:%d", event.PlanID, event.Sequence)
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	if event.Payload == nil {
		event.Payload = map[string]any{}
	}

	payloadJSON, err := json.Marshal(event.Payload)
	if err != nil {
		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - AppendPlanEvent - marshal payload: %w", err)
	}
	eventJSON, err := agentos.MarshalPlanEvent(event)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	insertSQL := `
INSERT INTO plan_events (
    event_id,
    plan_id,
    node_id,
    run_id,
    event_type,
    sequence,
    idempotency_key,
    payload_json,
    event_json,
    timestamp
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
RETURNING event_json`
	if idempotencyKey != "" {
		insertSQL = `
INSERT INTO plan_events (
    event_id,
    plan_id,
    node_id,
    run_id,
    event_type,
    sequence,
    idempotency_key,
    payload_json,
    event_json,
    timestamp
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (plan_id, idempotency_key) WHERE idempotency_key <> ''
DO UPDATE SET event_json = plan_events.event_json
RETURNING event_json`
	}

	var storedJSON []byte
	err = tx.QueryRow(ctx, insertSQL,
		event.EventID,
		event.PlanID,
		event.NodeID,
		event.RunID,
		string(event.EventType),
		event.Sequence,
		idempotencyKey,
		payloadJSON,
		eventJSON,
		event.Timestamp,
	).Scan(&storedJSON)
	if err != nil {
		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - AppendPlanEvent - insert: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - AppendPlanEvent - commit: %w", err)
	}

	stored, err := agentos.UnmarshalPlanEvent(storedJSON)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	return stored, nil
}

func (r *AgentOSPlanRepo) planEventByIdempotencyKey(ctx context.Context, planID, idempotencyKey string) (agentos.PlanEvent, bool, error) {
	sql, args, err := r.Builder.
		Select("event_json").
		From("plan_events").
		Where(sq.Eq{"plan_id": planID, "idempotency_key": idempotencyKey}).
		ToSql()
	if err != nil {
		return agentos.PlanEvent{}, false, fmt.Errorf("AgentOSPlanRepo - planEventByIdempotencyKey - builder: %w", err)
	}

	var eventJSON []byte
	err = r.Pool.QueryRow(ctx, sql, args...).Scan(&eventJSON)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentos.PlanEvent{}, false, nil
		}

		return agentos.PlanEvent{}, false, fmt.Errorf("AgentOSPlanRepo - planEventByIdempotencyKey - query: %w", err)
	}

	event, err := agentos.UnmarshalPlanEvent(eventJSON)
	if err != nil {
		return agentos.PlanEvent{}, false, err
	}

	return event, true, nil
}

func (r *AgentOSPlanRepo) ListPlanEvents(ctx context.Context, scope agentos.PlanStreamScope, limit int) ([]agentos.PlanEvent, error) {
	if scope.PlanID == "" {
		return nil, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidStreamScope)
	}

	builder := r.Builder.
		Select("event_json").
		From("plan_events").
		Where(sq.Eq{"plan_id": scope.PlanID}).
		Where(sq.Gt{"sequence": scope.AfterSequence}).
		OrderBy("sequence ASC")
	if scope.NodeID != "" {
		builder = builder.Where(sq.Eq{"node_id": scope.NodeID})
	}
	if scope.RunID != "" {
		builder = builder.Where(sq.Eq{"run_id": scope.RunID})
	}
	if limit > 0 {
		builder = builder.Limit(uint64(limit))
	}

	sql, args, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("AgentOSPlanRepo - ListPlanEvents - builder: %w", err)
	}
	rows, err := r.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("AgentOSPlanRepo - ListPlanEvents - query: %w", err)
	}
	defer rows.Close()

	var events []agentos.PlanEvent
	for rows.Next() {
		var eventJSON []byte
		if err := rows.Scan(&eventJSON); err != nil {
			return nil, fmt.Errorf("AgentOSPlanRepo - ListPlanEvents - scan: %w", err)
		}
		event, err := agentos.UnmarshalPlanEvent(eventJSON)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("AgentOSPlanRepo - ListPlanEvents - rows: %w", err)
	}

	return events, nil
}

func (r *AgentOSPlanRepo) RecordAudit(ctx context.Context, record agentosplan.AuditRecord) (agentosplan.AuditRecord, bool, error) {
	if record.PlanID == "" {
		return agentosplan.AuditRecord{}, false, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	if record.Action == "" {
		return agentosplan.AuditRecord{}, false, fmt.Errorf("%w: audit action is required", agentos.ErrInvalidRunPlan)
	}
	if record.IdempotencyKey == "" {
		return agentosplan.AuditRecord{}, false, fmt.Errorf("%w: audit idempotency key is required", agentos.ErrInvalidRunPlan)
	}
	existing, exists, err := r.auditRecordByIdempotencyKey(ctx, record.IdempotencyKey)
	if err != nil || exists {
		return existing, false, err
	}
	if record.AuditID == "" {
		record.AuditID = auditIDFromIdempotencyKey(record.IdempotencyKey)
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	if record.Payload == nil {
		record.Payload = map[string]any{}
	}
	payloadJSON, err := json.Marshal(record.Payload)
	if err != nil {
		return agentosplan.AuditRecord{}, false, fmt.Errorf("AgentOSPlanRepo - RecordAudit - marshal payload: %w", err)
	}

	_, err = r.Pool.Exec(ctx, `
INSERT INTO audit_logs (
    audit_id,
    plan_id,
    run_id,
    node_id,
    actor_id,
    action,
    idempotency_key,
    payload_json,
    created_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		record.AuditID,
		record.PlanID,
		record.RunID,
		record.NodeID,
		record.ActorID,
		string(record.Action),
		record.IdempotencyKey,
		payloadJSON,
		record.CreatedAt,
	)
	if err != nil {
		return agentosplan.AuditRecord{}, false, fmt.Errorf("AgentOSPlanRepo - RecordAudit - insert: %w", err)
	}

	return record, true, nil
}

func (r *AgentOSPlanRepo) auditRecordByIdempotencyKey(ctx context.Context, idempotencyKey string) (agentosplan.AuditRecord, bool, error) {
	sql, args, err := r.Builder.
		Select("audit_id", "plan_id", "run_id", "node_id", "actor_id", "action", "idempotency_key", "payload_json", "created_at").
		From("audit_logs").
		Where(sq.Eq{"idempotency_key": idempotencyKey}).
		ToSql()
	if err != nil {
		return agentosplan.AuditRecord{}, false, fmt.Errorf("AgentOSPlanRepo - auditRecordByIdempotencyKey - builder: %w", err)
	}

	var record agentosplan.AuditRecord
	var action string
	var payloadJSON []byte
	err = r.Pool.QueryRow(ctx, sql, args...).Scan(
		&record.AuditID,
		&record.PlanID,
		&record.RunID,
		&record.NodeID,
		&record.ActorID,
		&action,
		&record.IdempotencyKey,
		&payloadJSON,
		&record.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentosplan.AuditRecord{}, false, nil
		}

		return agentosplan.AuditRecord{}, false, fmt.Errorf("AgentOSPlanRepo - auditRecordByIdempotencyKey - query: %w", err)
	}
	if len(payloadJSON) > 0 {
		if err := json.Unmarshal(payloadJSON, &record.Payload); err != nil {
			return agentosplan.AuditRecord{}, false, fmt.Errorf("AgentOSPlanRepo - auditRecordByIdempotencyKey - decode payload: %w", err)
		}
	}
	record.Action = agentosplan.AuditAction(action)

	return record, true, nil
}

func auditIDFromIdempotencyKey(idempotencyKey string) string {
	sum := sha256.Sum256([]byte(idempotencyKey))

	return "audit:" + hex.EncodeToString(sum[:])
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}

	return value
}

var (
	_ agentosplan.PlanIndex      = (*AgentOSPlanRepo)(nil)
	_ agentosplan.PlanStateStore = (*AgentOSPlanRepo)(nil)
	_ agentosplan.PlanEventStore = (*AgentOSPlanRepo)(nil)
	_ agentosplan.AuditStore     = (*AgentOSPlanRepo)(nil)
)
