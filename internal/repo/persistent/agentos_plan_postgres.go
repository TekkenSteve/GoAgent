package persistent

import (
	"context"
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
	if spec.PlanID == "" {
		return agentos.RunPlanStatus{}, false, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	if spec.IdempotencyKey == "" {
		return agentos.RunPlanStatus{}, false, fmt.Errorf("%w: plan idempotency key is required", agentos.ErrInvalidRunPlan)
	}
	if status.PlanID == "" {
		status.PlanID = spec.PlanID
	}
	existingSpec, existing, exists, err := r.planByIdempotencyKey(ctx, spec.AccountID, spec.ProjectID, spec.IdempotencyKey)
	if err != nil {
		return agentos.RunPlanStatus{}, false, err
	}
	if exists {
		if err := agentosplan.ValidatePlanStartIdempotency(existingSpec, spec); err != nil {
			return agentos.RunPlanStatus{}, false, err
		}

		return existing, false, nil
	}
	existingSpec, existing, exists, err = r.GetPlan(ctx, spec.PlanID)
	if err != nil {
		return agentos.RunPlanStatus{}, false, err
	}
	if exists {
		if err := agentosplan.ValidatePlanStartIdempotency(existingSpec, spec); err != nil {
			return agentos.RunPlanStatus{}, false, err
		}

		return existing, false, nil
	}
	if err := r.SavePlanState(ctx, agentosplan.PlanStateSnapshot{
		Spec:           spec,
		Status:         status,
		IdempotencyKey: spec.IdempotencyKey,
	}); err != nil {
		if isPostgresUniqueViolation(err) {
			existingSpec, existing, exists, lookupErr := r.planByIdempotencyKey(ctx, spec.AccountID, spec.ProjectID, spec.IdempotencyKey)
			if lookupErr != nil {
				return agentos.RunPlanStatus{}, false, lookupErr
			}
			if exists {
				if validateErr := agentosplan.ValidatePlanStartIdempotency(existingSpec, spec); validateErr != nil {
					return agentos.RunPlanStatus{}, false, validateErr
				}

				return existing, false, nil
			}
		}

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

func (r *AgentOSPlanRepo) GetPlanByRef(ctx context.Context, ref agentos.PlanRef) (agentos.RunPlanSpec, agentos.RunPlanStatus, bool, error) {
	if err := agentosplan.ValidatePlanRef(ref); err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, err
	}
	snapshot, exists, err := r.loadPlanStateByWhere(ctx, sq.Eq{
		"plan_id":    ref.PlanID,
		"account_id": ref.AccountID,
		"project_id": ref.ProjectID,
	}, "GetPlanByRef")
	if err != nil || !exists {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, exists, err
	}
	if err := agentosplan.ValidatePlanTenantAccess(ref, snapshot.Spec); err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, err
	}

	return snapshot.Spec, snapshot.Status, true, nil
}

func (r *AgentOSPlanRepo) ListPlanRefs(ctx context.Context, scope agentosplan.PlanRefScope) ([]agentos.PlanRef, error) {
	if scope.Limit < 0 {
		return nil, fmt.Errorf("%w: plan ref limit must be non-negative", agentos.ErrInvalidPlanScope)
	}
	for _, state := range scope.LifecycleStates {
		if state == "" {
			return nil, fmt.Errorf("%w: lifecycle state is required", agentos.ErrInvalidPlanScope)
		}
	}

	builder := r.Builder.
		Select("plan_id", "account_id", "project_id").
		From("plans").
		OrderBy("updated_at ASC", "plan_id ASC")
	if scope.AccountID != "" {
		builder = builder.Where(sq.Eq{"account_id": scope.AccountID})
	}
	if scope.ProjectID != "" {
		builder = builder.Where(sq.Eq{"project_id": scope.ProjectID})
	}
	if len(scope.LifecycleStates) > 0 {
		builder = builder.Where(sq.Eq{"lifecycle_state": scope.LifecycleStates})
	}
	if !scope.UpdatedAfter.IsZero() {
		builder = builder.Where(sq.Gt{"updated_at": scope.UpdatedAfter})
	}
	if scope.Limit > 0 {
		builder = builder.Limit(uint64(scope.Limit))
	}

	sql, args, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("AgentOSPlanRepo - ListPlanRefs - builder: %w", err)
	}
	rows, err := r.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("AgentOSPlanRepo - ListPlanRefs - query: %w", err)
	}
	defer rows.Close()

	var refs []agentos.PlanRef
	for rows.Next() {
		var ref agentos.PlanRef
		if err := rows.Scan(&ref.PlanID, &ref.AccountID, &ref.ProjectID); err != nil {
			return nil, fmt.Errorf("AgentOSPlanRepo - ListPlanRefs - scan: %w", err)
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("AgentOSPlanRepo - ListPlanRefs - rows: %w", err)
	}

	return refs, nil
}

func (r *AgentOSPlanRepo) SavePlanState(ctx context.Context, snapshot agentosplan.PlanStateSnapshot) error {
	if snapshot.Spec.PlanID == "" {
		return fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	if snapshot.Spec.IdempotencyKey == "" {
		return fmt.Errorf("%w: plan idempotency key is required", agentos.ErrInvalidRunPlan)
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
	state, err := agentosplan.NewStateFromStatus(snapshot.Spec, snapshot.Status)
	if err != nil {
		return err
	}
	snapshot.Status = state.Status

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

	existingSpec, exists, err := r.planSpecForUpdate(ctx, tx, snapshot.Spec.PlanID)
	if err != nil {
		return err
	}
	if exists {
		if err := agentosplan.ValidatePlanStateIdentity(existingSpec, snapshot.Spec); err != nil {
			return err
		}
	}

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
	if err := r.deleteStalePlanNodes(ctx, tx, snapshot.Spec.PlanID, snapshot.Status.Nodes); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("AgentOSPlanRepo - SavePlanState - commit: %w", err)
	}

	return nil
}

func (r *AgentOSPlanRepo) planSpecForUpdate(ctx context.Context, tx pgx.Tx, planID string) (agentos.RunPlanSpec, bool, error) {
	var specJSON []byte
	err := tx.QueryRow(ctx, `
SELECT spec_json
FROM plans
WHERE plan_id = $1
FOR UPDATE`, planID).Scan(&specJSON)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentos.RunPlanSpec{}, false, nil
		}

		return agentos.RunPlanSpec{}, false, fmt.Errorf("AgentOSPlanRepo - planSpecForUpdate - query: %w", err)
	}

	var spec agentos.RunPlanSpec
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return agentos.RunPlanSpec{}, false, fmt.Errorf("AgentOSPlanRepo - planSpecForUpdate - decode spec: %w", err)
	}

	return spec, true, nil
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

func (r *AgentOSPlanRepo) deleteStalePlanNodes(ctx context.Context, tx pgx.Tx, planID string, nodes []agentos.PlanNodeStatus) error {
	nodeIDs := make([]string, 0, len(nodes))
	for _, node := range nodes {
		nodeIDs = append(nodeIDs, node.NodeID)
	}
	if len(nodeIDs) == 0 {
		if _, err := tx.Exec(ctx, `DELETE FROM plan_nodes WHERE plan_id = $1`, planID); err != nil {
			return fmt.Errorf("AgentOSPlanRepo - deleteStalePlanNodes - delete all: %w", err)
		}

		return nil
	}

	if _, err := tx.Exec(ctx, `DELETE FROM plan_nodes WHERE plan_id = $1 AND NOT (node_id = ANY($2))`, planID, nodeIDs); err != nil {
		return fmt.Errorf("AgentOSPlanRepo - deleteStalePlanNodes - delete: %w", err)
	}

	return nil
}

func (r *AgentOSPlanRepo) LoadPlanState(ctx context.Context, planID string) (agentosplan.PlanStateSnapshot, bool, error) {
	if planID == "" {
		return agentosplan.PlanStateSnapshot{}, false, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}

	return r.loadPlanStateByWhere(ctx, sq.Eq{"plan_id": planID}, "LoadPlanState")
}

func (r *AgentOSPlanRepo) loadPlanStateByWhere(ctx context.Context, where sq.Eq, op string) (agentosplan.PlanStateSnapshot, bool, error) {
	sql, args, err := r.Builder.
		Select("spec_json", "status_json", "idempotency_key").
		From("plans").
		Where(where).
		ToSql()
	if err != nil {
		return agentosplan.PlanStateSnapshot{}, false, fmt.Errorf("AgentOSPlanRepo - %s - builder: %w", op, err)
	}

	var specJSON []byte
	var statusJSON []byte
	var idempotencyKey string
	err = r.Pool.QueryRow(ctx, sql, args...).Scan(&specJSON, &statusJSON, &idempotencyKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentosplan.PlanStateSnapshot{}, false, nil
		}

		return agentosplan.PlanStateSnapshot{}, false, fmt.Errorf("AgentOSPlanRepo - %s - query: %w", op, err)
	}

	var spec agentos.RunPlanSpec
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return agentosplan.PlanStateSnapshot{}, false, fmt.Errorf("AgentOSPlanRepo - %s - decode spec: %w", op, err)
	}
	var status agentos.RunPlanStatus
	if err := json.Unmarshal(statusJSON, &status); err != nil {
		return agentosplan.PlanStateSnapshot{}, false, fmt.Errorf("AgentOSPlanRepo - %s - decode status: %w", op, err)
	}
	nodes, err := r.loadPlanNodeStatuses(ctx, spec.PlanID)
	if err != nil {
		return agentosplan.PlanStateSnapshot{}, false, err
	}
	status.Nodes = nodes
	state, err := agentosplan.NewStateFromStatus(spec, status)
	if err != nil {
		return agentosplan.PlanStateSnapshot{}, false, err
	}
	status = state.Status

	return agentosplan.PlanStateSnapshot{
		Spec:           spec,
		Status:         status,
		IdempotencyKey: idempotencyKey,
	}, true, nil
}

func (r *AgentOSPlanRepo) loadPlanNodeStatuses(ctx context.Context, planID string) ([]agentos.PlanNodeStatus, error) {
	sql, args, err := r.Builder.
		Select("status_json").
		From("plan_nodes").
		Where(sq.Eq{"plan_id": planID}).
		OrderBy("node_id ASC").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("AgentOSPlanRepo - loadPlanNodeStatuses - builder: %w", err)
	}
	rows, err := r.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("AgentOSPlanRepo - loadPlanNodeStatuses - query: %w", err)
	}
	defer rows.Close()

	nodes := make([]agentos.PlanNodeStatus, 0)
	for rows.Next() {
		var statusJSON []byte
		if err := rows.Scan(&statusJSON); err != nil {
			return nil, fmt.Errorf("AgentOSPlanRepo - loadPlanNodeStatuses - scan: %w", err)
		}
		var node agentos.PlanNodeStatus
		if err := json.Unmarshal(statusJSON, &node); err != nil {
			return nil, fmt.Errorf("AgentOSPlanRepo - loadPlanNodeStatuses - decode status: %w", err)
		}
		nodes = append(nodes, node)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("AgentOSPlanRepo - loadPlanNodeStatuses - rows: %w", err)
	}

	return nodes, nil
}

func (r *AgentOSPlanRepo) planByIdempotencyKey(ctx context.Context, accountID, projectID, idempotencyKey string) (agentos.RunPlanSpec, agentos.RunPlanStatus, bool, error) {
	if idempotencyKey == "" {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, fmt.Errorf("%w: plan idempotency key is required", agentos.ErrInvalidRunPlan)
	}

	sql, args, err := r.Builder.
		Select("spec_json", "status_json").
		From("plans").
		Where(sq.Eq{"account_id": accountID, "project_id": projectID, "idempotency_key": idempotencyKey}).
		ToSql()
	if err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, fmt.Errorf("AgentOSPlanRepo - planByIdempotencyKey - builder: %w", err)
	}

	var specJSON []byte
	var statusJSON []byte
	err = r.Pool.QueryRow(ctx, sql, args...).Scan(&specJSON, &statusJSON)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, nil
		}

		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, fmt.Errorf("AgentOSPlanRepo - planByIdempotencyKey - query: %w", err)
	}

	var spec agentos.RunPlanSpec
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, fmt.Errorf("AgentOSPlanRepo - planByIdempotencyKey - decode spec: %w", err)
	}
	var status agentos.RunPlanStatus
	if err := json.Unmarshal(statusJSON, &status); err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, fmt.Errorf("AgentOSPlanRepo - planByIdempotencyKey - decode status: %w", err)
	}

	return spec, status, true, nil
}

type planTenantScope struct {
	AccountID string
	ProjectID string
}

type planTenantScopeQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func planTenantScopeByPlanID(ctx context.Context, querier planTenantScopeQuerier, planID string) (planTenantScope, bool, error) {
	var scope planTenantScope
	err := querier.QueryRow(ctx, `
SELECT account_id, project_id
FROM plans
WHERE plan_id = $1`, planID).Scan(&scope.AccountID, &scope.ProjectID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return planTenantScope{}, false, nil
		}

		return planTenantScope{}, false, fmt.Errorf("planTenantScopeByPlanID - query: %w", err)
	}

	return scope, true, nil
}

func (r *AgentOSPlanRepo) AppendPlanEvent(ctx context.Context, event agentos.PlanEvent, idempotencyKey string) (agentos.PlanEvent, error) {
	if event.PlanID == "" {
		return agentos.PlanEvent{}, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidPlanEvent)
	}
	if idempotencyKey == "" {
		return agentos.PlanEvent{}, fmt.Errorf("%w: plan event idempotency key is required", agentos.ErrInvalidPlanEvent)
	}
	requestedEvent := agentosplan.NormalizePlanEventAppendRequest(event)
	event = requestedEvent

	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - AppendPlanEvent - begin: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var currentSequence int64
	var scope planTenantScope
	err = tx.QueryRow(ctx, `
SELECT event_sequence, account_id, project_id
FROM plans
WHERE plan_id = $1
FOR UPDATE`, event.PlanID).Scan(&currentSequence, &scope.AccountID, &scope.ProjectID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentos.PlanEvent{}, fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, event.PlanID)
		}

		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - AppendPlanEvent - lock plan: %w", err)
	}

	existing, exists, err := r.planEventByIdempotencyKeyWith(ctx, tx, event.PlanID, idempotencyKey)
	if err != nil {
		return agentos.PlanEvent{}, err
	}
	if exists {
		if err := agentosplan.ValidatePlanEventIdempotency(existing, requestedEvent); err != nil {
			return agentos.PlanEvent{}, err
		}

		return existing, nil
	}

	event.Sequence = currentSequence + 1
	if _, err := tx.Exec(ctx, `
UPDATE plans
SET event_sequence = $2
WHERE plan_id = $1`, event.PlanID, event.Sequence); err != nil {
		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - AppendPlanEvent - update sequence: %w", err)
	}
	event.EventID = fmt.Sprintf("%s:%d", event.PlanID, event.Sequence)
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
    account_id,
    project_id,
    node_id,
    run_id,
    event_type,
    sequence,
    idempotency_key,
    payload_json,
    event_json,
    timestamp
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
RETURNING event_json`

	var storedJSON []byte
	err = tx.QueryRow(ctx, insertSQL,
		event.EventID,
		event.PlanID,
		scope.AccountID,
		scope.ProjectID,
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
		if isPostgresUniqueViolation(err) {
			existing, exists, lookupErr := r.planEventByIdempotencyKeyWith(ctx, tx, event.PlanID, idempotencyKey)
			if lookupErr != nil {
				return agentos.PlanEvent{}, lookupErr
			}
			if exists {
				if err := agentosplan.ValidatePlanEventIdempotency(existing, requestedEvent); err != nil {
					return agentos.PlanEvent{}, err
				}

				return existing, nil
			}
		}

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
	return r.planEventByIdempotencyKeyWith(ctx, r.Pool, planID, idempotencyKey)
}

type planEventRowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (r *AgentOSPlanRepo) planEventByIdempotencyKeyWith(ctx context.Context, querier planEventRowQuerier, planID, idempotencyKey string) (agentos.PlanEvent, bool, error) {
	sql, args, err := r.Builder.
		Select("event_json").
		From("plan_events").
		Where(sq.Eq{"plan_id": planID, "idempotency_key": idempotencyKey}).
		ToSql()
	if err != nil {
		return agentos.PlanEvent{}, false, fmt.Errorf("AgentOSPlanRepo - planEventByIdempotencyKey - builder: %w", err)
	}

	var eventJSON []byte
	err = querier.QueryRow(ctx, sql, args...).Scan(&eventJSON)
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
	if err := agentosplan.ValidatePlanStreamScope(scope); err != nil {
		return nil, err
	}
	spec, _, exists, err := r.GetPlan(ctx, scope.PlanID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, scope.PlanID)
	}
	if err := agentosplan.ValidatePlanTenantAccess(agentos.PlanRef{PlanID: scope.PlanID, AccountID: scope.AccountID, ProjectID: scope.ProjectID}, spec); err != nil {
		return nil, err
	}

	builder := r.Builder.
		Select("event_json").
		From("plan_events").
		Where(sq.Eq{"plan_id": scope.PlanID}).
		Where(sq.Eq{"account_id": scope.AccountID}).
		Where(sq.Eq{"project_id": scope.ProjectID}).
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

func (r *AgentOSPlanRepo) GetPlanMetricCheckpoint(ctx context.Context, exporterID string, ref agentos.PlanRef) (agentosplan.PlanMetricCheckpoint, bool, error) {
	if exporterID == "" {
		return agentosplan.PlanMetricCheckpoint{}, false, fmt.Errorf("%w: metrics exporter id is required", agentos.ErrInvalidRunPlan)
	}
	if err := agentosplan.ValidatePlanRef(ref); err != nil {
		return agentosplan.PlanMetricCheckpoint{}, false, err
	}

	sql, args, err := r.Builder.
		Select("exporter_id", "plan_id", "account_id", "project_id", "sequence", "projection_json", "updated_at").
		From("plan_metric_checkpoints").
		Where(sq.Eq{
			"exporter_id": exporterID,
			"plan_id":     ref.PlanID,
			"account_id":  ref.AccountID,
			"project_id":  ref.ProjectID,
		}).
		ToSql()
	if err != nil {
		return agentosplan.PlanMetricCheckpoint{}, false, fmt.Errorf("AgentOSPlanRepo - GetPlanMetricCheckpoint - builder: %w", err)
	}

	var checkpoint agentosplan.PlanMetricCheckpoint
	var projectionJSON []byte
	err = r.Pool.QueryRow(ctx, sql, args...).Scan(
		&checkpoint.ExporterID,
		&checkpoint.PlanID,
		&checkpoint.AccountID,
		&checkpoint.ProjectID,
		&checkpoint.Sequence,
		&projectionJSON,
		&checkpoint.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentosplan.PlanMetricCheckpoint{}, false, nil
		}

		return agentosplan.PlanMetricCheckpoint{}, false, fmt.Errorf("AgentOSPlanRepo - GetPlanMetricCheckpoint - query: %w", err)
	}
	if len(projectionJSON) > 0 {
		if err := json.Unmarshal(projectionJSON, &checkpoint.Projection); err != nil {
			return agentosplan.PlanMetricCheckpoint{}, false, fmt.Errorf("AgentOSPlanRepo - GetPlanMetricCheckpoint - decode projection: %w", err)
		}
	}
	if err := agentosplan.ValidatePlanMetricCheckpointRef(checkpoint, exporterID, ref); err != nil {
		return agentosplan.PlanMetricCheckpoint{}, false, err
	}

	return checkpoint, true, nil
}

func (r *AgentOSPlanRepo) SavePlanMetricCheckpoint(ctx context.Context, checkpoint agentosplan.PlanMetricCheckpoint) error {
	ref := agentos.PlanRef{
		PlanID:    checkpoint.PlanID,
		AccountID: checkpoint.AccountID,
		ProjectID: checkpoint.ProjectID,
	}
	if err := agentosplan.ValidatePlanMetricCheckpointRef(checkpoint, checkpoint.ExporterID, ref); err != nil {
		return err
	}
	scope, exists, err := planTenantScopeByPlanID(ctx, r.Pool, checkpoint.PlanID)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, checkpoint.PlanID)
	}
	if scope.AccountID != checkpoint.AccountID || scope.ProjectID != checkpoint.ProjectID {
		return fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, checkpoint.PlanID)
	}

	projectionJSON, err := json.Marshal(checkpoint.Projection)
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - SavePlanMetricCheckpoint - marshal projection: %w", err)
	}
	result, err := r.Pool.Exec(ctx, `
INSERT INTO plan_metric_checkpoints (
    exporter_id,
    plan_id,
    account_id,
    project_id,
    sequence,
    projection_json,
    updated_at
) VALUES ($1,$2,$3,$4,$5,$6,NOW())
ON CONFLICT (exporter_id, plan_id) DO UPDATE SET
    account_id = EXCLUDED.account_id,
    project_id = EXCLUDED.project_id,
    sequence = EXCLUDED.sequence,
    projection_json = EXCLUDED.projection_json,
    updated_at = NOW()
WHERE plan_metric_checkpoints.sequence <= EXCLUDED.sequence`,
		checkpoint.ExporterID,
		checkpoint.PlanID,
		checkpoint.AccountID,
		checkpoint.ProjectID,
		checkpoint.Sequence,
		projectionJSON,
	)
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - SavePlanMetricCheckpoint - upsert: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("%w: metrics checkpoint sequence moved backward for plan %q", agentos.ErrInvalidRunPlan, checkpoint.PlanID)
	}

	return nil
}

func (r *AgentOSPlanRepo) RecordPlanMetric(ctx context.Context, sample agentosplan.PlanMetricSample) error {
	if err := agentosplan.ValidatePlanMetricSample(sample); err != nil {
		return err
	}
	scope, exists, err := planTenantScopeByPlanID(ctx, r.Pool, sample.PlanID)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, sample.PlanID)
	}
	if scope.AccountID != sample.AccountID || scope.ProjectID != sample.ProjectID {
		return fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, sample.PlanID)
	}

	labelsJSON, err := json.Marshal(planMetricLabelsForStorage(sample.Labels))
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - RecordPlanMetric - marshal labels: %w", err)
	}
	sample.Timestamp = sample.Timestamp.UTC().Truncate(time.Microsecond)
	result, err := r.Pool.Exec(ctx, `
INSERT INTO plan_metric_samples (
    metric_name,
    plan_id,
    account_id,
    project_id,
    node_id,
    run_id,
    event_id,
    sequence,
    value,
    unit,
    labels_json,
    sample_timestamp
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
ON CONFLICT (metric_name, plan_id, node_id, run_id, event_id, sequence) DO UPDATE SET
    created_at = plan_metric_samples.created_at
WHERE plan_metric_samples.account_id = EXCLUDED.account_id
  AND plan_metric_samples.project_id = EXCLUDED.project_id
  AND plan_metric_samples.value = EXCLUDED.value
  AND plan_metric_samples.unit = EXCLUDED.unit
  AND plan_metric_samples.labels_json = EXCLUDED.labels_json
  AND plan_metric_samples.sample_timestamp = EXCLUDED.sample_timestamp`,
		string(sample.Name),
		sample.PlanID,
		sample.AccountID,
		sample.ProjectID,
		sample.NodeID,
		sample.RunID,
		sample.EventID,
		sample.Sequence,
		sample.Value,
		sample.Unit,
		labelsJSON,
		sample.Timestamp,
	)
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - RecordPlanMetric - upsert: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("%w: metric sample identity conflict for plan %q event %q", agentos.ErrInvalidRunPlan, sample.PlanID, sample.EventID)
	}

	return nil
}

func planMetricLabelsForStorage(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return map[string]string{}
	}

	return labels
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
	scope, exists, err := planTenantScopeByPlanID(ctx, r.Pool, record.PlanID)
	if err != nil {
		return agentosplan.AuditRecord{}, false, err
	}
	if !exists {
		return agentosplan.AuditRecord{}, false, fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, record.PlanID)
	}
	record.AccountID = scope.AccountID
	record.ProjectID = scope.ProjectID
	ref := agentosplan.AuditRefFromRecord(record)
	existing, exists, err := r.GetAuditRecord(ctx, ref)
	if err != nil {
		return agentosplan.AuditRecord{}, false, err
	}
	if exists {
		if err := agentosplan.ValidateAuditIdempotency(existing, record); err != nil {
			return agentosplan.AuditRecord{}, false, err
		}

		return existing, false, nil
	}
	if record.AuditID == "" {
		record.AuditID = agentosplan.AuditIDFromRef(ref)
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
    account_id,
    project_id,
    run_id,
    node_id,
    actor_id,
    action,
    idempotency_key,
    payload_json,
    created_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		record.AuditID,
		record.PlanID,
		record.AccountID,
		record.ProjectID,
		record.RunID,
		record.NodeID,
		record.ActorID,
		string(record.Action),
		record.IdempotencyKey,
		payloadJSON,
		record.CreatedAt,
	)
	if err != nil {
		if isPostgresUniqueViolation(err) {
			existing, exists, lookupErr := r.GetAuditRecord(ctx, ref)
			if lookupErr != nil {
				return agentosplan.AuditRecord{}, false, lookupErr
			}
			if exists {
				if err := agentosplan.ValidateAuditIdempotency(existing, record); err != nil {
					return agentosplan.AuditRecord{}, false, err
				}

				return existing, false, nil
			}
		}

		return agentosplan.AuditRecord{}, false, fmt.Errorf("AgentOSPlanRepo - RecordAudit - insert: %w", err)
	}

	return record, true, nil
}

func (r *AgentOSPlanRepo) GetAuditRecord(ctx context.Context, ref agentosplan.AuditRef) (agentosplan.AuditRecord, bool, error) {
	if err := agentosplan.ValidateAuditRef(ref); err != nil {
		return agentosplan.AuditRecord{}, false, err
	}

	sql, args, err := r.Builder.
		Select("audit_id", "plan_id", "account_id", "project_id", "run_id", "node_id", "actor_id", "action", "idempotency_key", "payload_json", "created_at").
		From("audit_logs").
		Where(sq.Eq{
			"plan_id":         ref.PlanID,
			"account_id":      ref.AccountID,
			"project_id":      ref.ProjectID,
			"idempotency_key": ref.IdempotencyKey,
		}).
		ToSql()
	if err != nil {
		return agentosplan.AuditRecord{}, false, fmt.Errorf("AgentOSPlanRepo - GetAuditRecord - builder: %w", err)
	}

	var record agentosplan.AuditRecord
	var action string
	var payloadJSON []byte
	err = r.Pool.QueryRow(ctx, sql, args...).Scan(
		&record.AuditID,
		&record.PlanID,
		&record.AccountID,
		&record.ProjectID,
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

		return agentosplan.AuditRecord{}, false, fmt.Errorf("AgentOSPlanRepo - GetAuditRecord - query: %w", err)
	}
	if len(payloadJSON) > 0 {
		if err := json.Unmarshal(payloadJSON, &record.Payload); err != nil {
			return agentosplan.AuditRecord{}, false, fmt.Errorf("AgentOSPlanRepo - GetAuditRecord - decode payload: %w", err)
		}
	}
	record.Action = agentosplan.AuditAction(action)

	return record, true, nil
}

func (r *AgentOSPlanRepo) ListAuditRecords(ctx context.Context, scope agentos.PlanAuditScope) ([]agentos.PlanAuditRecord, error) {
	if err := agentosplan.ValidatePlanAuditScope(scope); err != nil {
		return nil, err
	}
	spec, _, exists, err := r.GetPlan(ctx, scope.PlanID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, scope.PlanID)
	}
	if err := agentosplan.ValidatePlanTenantAccess(agentos.PlanRef{PlanID: scope.PlanID, AccountID: scope.AccountID, ProjectID: scope.ProjectID}, spec); err != nil {
		return nil, err
	}

	builder := r.Builder.
		Select("audit_id", "plan_id", "account_id", "project_id", "run_id", "node_id", "actor_id", "action", "idempotency_key", "payload_json", "created_at").
		From("audit_logs").
		Where(sq.Eq{"plan_id": scope.PlanID}).
		Where(sq.Eq{"account_id": scope.AccountID}).
		Where(sq.Eq{"project_id": scope.ProjectID}).
		OrderBy("created_at ASC", "audit_id ASC")
	if scope.NodeID != "" {
		builder = builder.Where(sq.Eq{"node_id": scope.NodeID})
	}
	if scope.RunID != "" {
		builder = builder.Where(sq.Eq{"run_id": scope.RunID})
	}
	if scope.Action != "" {
		builder = builder.Where(sq.Eq{"action": string(scope.Action)})
	}
	if scope.Limit > 0 {
		builder = builder.Limit(uint64(scope.Limit))
	}

	sql, args, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("AgentOSPlanRepo - ListAuditRecords - builder: %w", err)
	}
	rows, err := r.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("AgentOSPlanRepo - ListAuditRecords - query: %w", err)
	}
	defer rows.Close()

	var records []agentos.PlanAuditRecord
	for rows.Next() {
		record, err := scanPlanAuditRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("AgentOSPlanRepo - ListAuditRecords - rows: %w", err)
	}

	return records, nil
}

func scanPlanAuditRecord(scanner interface{ Scan(dest ...any) error }) (agentos.PlanAuditRecord, error) {
	var record agentos.PlanAuditRecord
	var action string
	var payloadJSON []byte
	if err := scanner.Scan(
		&record.AuditID,
		&record.PlanID,
		&record.AccountID,
		&record.ProjectID,
		&record.RunID,
		&record.NodeID,
		&record.ActorID,
		&action,
		&record.IdempotencyKey,
		&payloadJSON,
		&record.CreatedAt,
	); err != nil {
		return agentos.PlanAuditRecord{}, fmt.Errorf("AgentOSPlanRepo - scanPlanAuditRecord: %w", err)
	}
	if len(payloadJSON) > 0 {
		if err := json.Unmarshal(payloadJSON, &record.Payload); err != nil {
			return agentos.PlanAuditRecord{}, fmt.Errorf("AgentOSPlanRepo - scanPlanAuditRecord - decode payload: %w", err)
		}
	}
	record.Action = agentos.PlanAuditAction(action)

	return record, nil
}

func (r *AgentOSPlanRepo) RecordPlanCommand(ctx context.Context, command agentosplan.PlanCommandRecord) (agentosplan.PlanCommandRecord, bool, error) {
	if command.PlanID == "" {
		return agentosplan.PlanCommandRecord{}, false, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	if command.Action == "" {
		return agentosplan.PlanCommandRecord{}, false, fmt.Errorf("%w: command action is required", agentos.ErrInvalidRunPlan)
	}
	if command.IdempotencyKey == "" {
		return agentosplan.PlanCommandRecord{}, false, fmt.Errorf("%w: command idempotency key is required", agentos.ErrInvalidRunPlan)
	}
	scope, exists, err := planTenantScopeByPlanID(ctx, r.Pool, command.PlanID)
	if err != nil {
		return agentosplan.PlanCommandRecord{}, false, err
	}
	if !exists {
		return agentosplan.PlanCommandRecord{}, false, fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, command.PlanID)
	}
	command.AccountID = scope.AccountID
	command.ProjectID = scope.ProjectID
	ref := agentosplan.PlanCommandRefFromRecord(command)
	existing, exists, err := r.GetPlanCommand(ctx, ref)
	if err != nil {
		return agentosplan.PlanCommandRecord{}, false, err
	}
	if exists {
		if err := agentosplan.ValidatePlanCommandIdempotency(existing, command); err != nil {
			return agentosplan.PlanCommandRecord{}, false, err
		}

		return existing, false, nil
	}
	if command.CommandID == "" {
		command.CommandID = agentosplan.PlanCommandIDFromRef(ref)
	}
	if command.Status == "" {
		command.Status = agentosplan.PlanCommandPending
	}
	if command.CreatedAt.IsZero() {
		command.CreatedAt = time.Now().UTC()
	}
	if command.UpdatedAt.IsZero() {
		command.UpdatedAt = command.CreatedAt
	}
	if command.Payload == nil {
		command.Payload = map[string]any{}
	}
	payloadJSON, err := json.Marshal(command.Payload)
	if err != nil {
		return agentosplan.PlanCommandRecord{}, false, fmt.Errorf("AgentOSPlanRepo - RecordPlanCommand - marshal payload: %w", err)
	}

	_, err = r.Pool.Exec(ctx, `
INSERT INTO plan_commands (
    command_id,
    plan_id,
    account_id,
    project_id,
    actor_id,
    action,
    idempotency_key,
    payload_json,
    status,
    failure_reason,
    created_at,
    updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		command.CommandID,
		command.PlanID,
		command.AccountID,
		command.ProjectID,
		command.ActorID,
		string(command.Action),
		command.IdempotencyKey,
		payloadJSON,
		string(command.Status),
		command.FailureReason,
		command.CreatedAt,
		command.UpdatedAt,
	)
	if err != nil {
		if isPostgresUniqueViolation(err) {
			existing, exists, lookupErr := r.GetPlanCommand(ctx, ref)
			if lookupErr != nil {
				return agentosplan.PlanCommandRecord{}, false, lookupErr
			}
			if exists {
				if err := agentosplan.ValidatePlanCommandIdempotency(existing, command); err != nil {
					return agentosplan.PlanCommandRecord{}, false, err
				}

				return existing, false, nil
			}
		}

		return agentosplan.PlanCommandRecord{}, false, fmt.Errorf("AgentOSPlanRepo - RecordPlanCommand - insert: %w", err)
	}

	return command, true, nil
}

func (r *AgentOSPlanRepo) GetPlanCommand(ctx context.Context, ref agentosplan.PlanCommandRef) (agentosplan.PlanCommandRecord, bool, error) {
	if err := agentosplan.ValidatePlanCommandRef(ref); err != nil {
		return agentosplan.PlanCommandRecord{}, false, err
	}

	sql, args, err := r.Builder.
		Select("command_id", "plan_id", "account_id", "project_id", "actor_id", "action", "idempotency_key", "payload_json", "status", "failure_reason", "created_at", "updated_at").
		From("plan_commands").
		Where(sq.Eq{
			"plan_id":         ref.PlanID,
			"account_id":      ref.AccountID,
			"project_id":      ref.ProjectID,
			"idempotency_key": ref.IdempotencyKey,
		}).
		ToSql()
	if err != nil {
		return agentosplan.PlanCommandRecord{}, false, fmt.Errorf("AgentOSPlanRepo - GetPlanCommand - builder: %w", err)
	}

	command, err := r.scanPlanCommandRow(ctx, r.Pool.QueryRow(ctx, sql, args...))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentosplan.PlanCommandRecord{}, false, nil
		}

		return agentosplan.PlanCommandRecord{}, false, err
	}

	return command, true, nil
}

func (r *AgentOSPlanRepo) ListRecoverablePlanCommands(ctx context.Context, scope agentosplan.PlanCommandScope) ([]agentosplan.PlanCommandRecord, error) {
	statuses, err := agentosplan.RecoverablePlanCommandStatuses(scope)
	if err != nil {
		return nil, err
	}
	statusValues := make([]string, 0, len(statuses))
	for _, status := range statuses {
		statusValues = append(statusValues, string(status))
	}

	builder := r.Builder.
		Select("command_id", "plan_id", "account_id", "project_id", "actor_id", "action", "idempotency_key", "payload_json", "status", "failure_reason", "created_at", "updated_at").
		From("plan_commands").
		Where(sq.Eq{"status": statusValues}).
		OrderBy("updated_at ASC", "command_id ASC")
	if scope.PlanID != "" {
		builder = builder.Where(sq.Eq{"plan_id": scope.PlanID})
	}
	if scope.Action != "" {
		builder = builder.Where(sq.Eq{"action": string(scope.Action)})
	}
	if scope.Limit > 0 {
		builder = builder.Limit(uint64(scope.Limit))
	}

	sql, args, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("AgentOSPlanRepo - ListRecoverablePlanCommands - builder: %w", err)
	}
	rows, err := r.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("AgentOSPlanRepo - ListRecoverablePlanCommands - query: %w", err)
	}
	defer rows.Close()

	var commands []agentosplan.PlanCommandRecord
	for rows.Next() {
		command, err := r.scanPlanCommandRow(ctx, rows)
		if err != nil {
			return nil, err
		}
		commands = append(commands, command)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("AgentOSPlanRepo - ListRecoverablePlanCommands - rows: %w", err)
	}

	return commands, nil
}

func (r *AgentOSPlanRepo) MarkPlanCommandDelivered(ctx context.Context, ref agentosplan.PlanCommandRef) (agentosplan.PlanCommandRecord, error) {
	return r.updatePlanCommandStatus(ctx, ref, agentosplan.PlanCommandDelivered, "")
}

func (r *AgentOSPlanRepo) MarkPlanCommandFailed(ctx context.Context, ref agentosplan.PlanCommandRef, reason string) (agentosplan.PlanCommandRecord, error) {
	return r.updatePlanCommandStatus(ctx, ref, agentosplan.PlanCommandFailed, reason)
}

func (r *AgentOSPlanRepo) updatePlanCommandStatus(ctx context.Context, ref agentosplan.PlanCommandRef, status agentosplan.PlanCommandStatus, reason string) (agentosplan.PlanCommandRecord, error) {
	if err := agentosplan.ValidatePlanCommandRef(ref); err != nil {
		return agentosplan.PlanCommandRecord{}, err
	}

	command, exists, err := r.GetPlanCommand(ctx, ref)
	if err != nil {
		return agentosplan.PlanCommandRecord{}, err
	}
	if !exists {
		return agentosplan.PlanCommandRecord{}, fmt.Errorf("%w: command %q", agentos.ErrInvalidRunPlan, ref.IdempotencyKey)
	}
	command.Status = status
	command.FailureReason = reason
	command.UpdatedAt = time.Now().UTC()

	sql := `
UPDATE plan_commands
SET status = $2,
	    failure_reason = $3,
	    updated_at = $4
	WHERE plan_id = $5
	  AND account_id = $6
	  AND project_id = $7
	  AND idempotency_key = $1
	RETURNING command_id, plan_id, account_id, project_id, actor_id, action, idempotency_key, payload_json, status, failure_reason, created_at, updated_at`

	return r.scanPlanCommandRow(ctx, r.Pool.QueryRow(ctx, sql, ref.IdempotencyKey, string(command.Status), command.FailureReason, command.UpdatedAt, ref.PlanID, ref.AccountID, ref.ProjectID))
}

func (r *AgentOSPlanRepo) scanPlanCommandRow(_ context.Context, scanner interface{ Scan(dest ...any) error }) (agentosplan.PlanCommandRecord, error) {
	var command agentosplan.PlanCommandRecord
	var action string
	var status string
	var payloadJSON []byte
	err := scanner.Scan(
		&command.CommandID,
		&command.PlanID,
		&command.AccountID,
		&command.ProjectID,
		&command.ActorID,
		&action,
		&command.IdempotencyKey,
		&payloadJSON,
		&status,
		&command.FailureReason,
		&command.CreatedAt,
		&command.UpdatedAt,
	)
	if err != nil {
		return agentosplan.PlanCommandRecord{}, fmt.Errorf("AgentOSPlanRepo - scanPlanCommand: %w", err)
	}
	if len(payloadJSON) > 0 {
		if err := json.Unmarshal(payloadJSON, &command.Payload); err != nil {
			return agentosplan.PlanCommandRecord{}, fmt.Errorf("AgentOSPlanRepo - scanPlanCommand - decode payload: %w", err)
		}
	}
	command.Action = agentosplan.AuditAction(action)
	command.Status = agentosplan.PlanCommandStatus(status)

	return command, nil
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}

	return value
}

var (
	_ agentosplan.PlanIndex                 = (*AgentOSPlanRepo)(nil)
	_ agentosplan.PlanRefStore              = (*AgentOSPlanRepo)(nil)
	_ agentosplan.PlanStateStore            = (*AgentOSPlanRepo)(nil)
	_ agentosplan.PlanEventStore            = (*AgentOSPlanRepo)(nil)
	_ agentosplan.PlanMetricCheckpointStore = (*AgentOSPlanRepo)(nil)
	_ agentosplan.PlanMetricsSink           = (*AgentOSPlanRepo)(nil)
	_ agentosplan.PlanCommandStore          = (*AgentOSPlanRepo)(nil)
	_ agentosplan.AuditStore                = (*AgentOSPlanRepo)(nil)
)
