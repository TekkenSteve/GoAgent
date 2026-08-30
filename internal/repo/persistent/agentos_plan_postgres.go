package persistent

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	sq "github.com/Masterminds/squirrel"
	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"github.com/jackc/pgx/v5"
)

var errInsertedAuditRecordNotFound = errors.New("inserted audit record not found")

// errcheckIgnore is a helper for intentional error discards.
func errcheckIgnore(_ error) {}

// AgentOSPlanRepo persists RunPlan aggregate state and durable plan events.
type AgentOSPlanRepo struct {
	*postgres.Postgres
}

// NewAgentOSPlanRepo creates a Postgres-backed RunPlan repository.
func NewAgentOSPlanRepo(pg *postgres.Postgres) *AgentOSPlanRepo {
	return &AgentOSPlanRepo{pg}
}

// CreatePlan persists a new RunPlan from its spec and status, returning the stored status and whether the plan was newly created.
func (r *AgentOSPlanRepo) CreatePlan(ctx context.Context, spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus) (agentos.RunPlanStatus, bool, error) {
	if spec.PlanID == "" {
		return agentos.RunPlanStatus{}, false, fmt.Errorf("%w: plan id is required", agentoscore.ErrInvalidRunPlan)
	}

	if spec.IdempotencyKey == "" {
		return agentos.RunPlanStatus{}, false, fmt.Errorf("%w: plan idempotency key is required", agentoscore.ErrInvalidRunPlan)
	}

	normalizedStatus := *status
	if normalizedStatus.PlanID == "" {
		normalizedStatus.PlanID = spec.PlanID
	}

	existing, exists, err := r.existingPlanForCreate(ctx, spec)
	if err != nil {
		return agentos.RunPlanStatus{}, false, err
	}

	if exists {
		return existing, false, nil
	}

	createdStatus, err := r.createPlanState(ctx, &agentosplan.PlanStateSnapshot{
		Spec:   *spec,
		Status: normalizedStatus,
	})
	if err != nil {
		existing, lookupErr := r.resolvePlanCreateConflict(ctx, spec, err)
		if lookupErr != nil {
			return agentos.RunPlanStatus{}, false, lookupErr
		}

		if existing != nil {
			return *existing, false, nil
		}

		return agentos.RunPlanStatus{}, false, err
	}

	return createdStatus, true, nil
}

func (r *AgentOSPlanRepo) existingPlanForCreate(ctx context.Context, spec *agentos.RunPlanSpec) (agentos.RunPlanStatus, bool, error) {
	existingSpec, existing, exists, err := r.planByIdempotencyKey(ctx, spec.AccountID, spec.ProjectID, spec.IdempotencyKey)
	if err != nil || exists {
		return existing, exists, validateExistingPlanStart(&existingSpec, spec, exists)
	}

	existingSpec, existing, exists, err = r.GetPlan(ctx, spec.PlanID)
	if err != nil || exists {
		return existing, exists, validateExistingPlanStart(&existingSpec, spec, exists)
	}

	return agentos.RunPlanStatus{}, false, nil
}

func validateExistingPlanStart(existingSpec, spec *agentos.RunPlanSpec, exists bool) error {
	if !exists {
		return nil
	}

	return agentosplan.ValidatePlanStartIdempotency(existingSpec, spec)
}

func (r *AgentOSPlanRepo) resolvePlanCreateConflict(ctx context.Context, spec *agentos.RunPlanSpec, err error) (*agentos.RunPlanStatus, error) {
	if !isPostgresUniqueViolation(err) {
		return nil, nil
	}

	existingSpec, existing, exists, lookupErr := r.planByIdempotencyKey(ctx, spec.AccountID, spec.ProjectID, spec.IdempotencyKey)
	if lookupErr != nil {
		return nil, lookupErr
	}

	if !exists {
		return nil, nil
	}

	if validateErr := agentosplan.ValidatePlanStartIdempotency(&existingSpec, spec); validateErr != nil {
		return nil, validateErr
	}

	return &existing, nil
}

func (r *AgentOSPlanRepo) createPlanState(ctx context.Context, snapshot *agentosplan.PlanStateSnapshot) (agentos.RunPlanStatus, error) {
	normalized, err := normalizePlanStateSnapshotForPostgres(snapshot)
	if err != nil {
		return agentos.RunPlanStatus{}, err
	}

	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return agentos.RunPlanStatus{}, fmt.Errorf("AgentOSPlanRepo - createPlanState - begin: %w", err)
	}

	defer func() {
		errcheckIgnore(tx.Rollback(ctx))
	}()

	if err := r.savePlanStateWithTx(ctx, tx, &normalized, true); err != nil {
		return agentos.RunPlanStatus{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return agentos.RunPlanStatus{}, fmt.Errorf("AgentOSPlanRepo - createPlanState - commit: %w", err)
	}

	return normalized.Status, nil
}

// GetPlan loads the spec and status of the plan with the given ID.
func (r *AgentOSPlanRepo) GetPlan(ctx context.Context, planID string) (agentos.RunPlanSpec, agentos.RunPlanStatus, bool, error) {
	snapshot, exists, err := r.LoadPlanState(ctx, planID)
	if err != nil || !exists {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, exists, err
	}

	return snapshot.Spec, snapshot.Status, true, nil
}

// GetPlanByRef loads a plan's spec and status within the given tenant-scoped reference.
func (r *AgentOSPlanRepo) GetPlanByRef(ctx context.Context, ref agentos.PlanRef) (agentos.RunPlanSpec, agentos.RunPlanStatus, bool, error) {
	if err := agentosplan.ValidatePlanRef(ref); err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, err
	}

	snapshot, exists, err := r.loadPlanStateByWhere(ctx, sq.Eq{
		_colPlanID:    ref.PlanID,
		_colAccountID: ref.AccountID,
		_colProjectID: ref.ProjectID,
	}, "GetPlanByRef")
	if err != nil || !exists {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, exists, err
	}

	if err := agentosplan.ValidatePlanTenantAccess(ref, &snapshot.Spec); err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, err
	}

	return snapshot.Spec, snapshot.Status, true, nil
}

// ListPlanRefs returns plan references matching the given scope filters.
func (r *AgentOSPlanRepo) ListPlanRefs(ctx context.Context, scope *agentosplan.PlanRefScope) ([]agentos.PlanRef, error) {
	builder, err := r.planRefsBuilder(scope)
	if err != nil {
		return nil, err
	}

	query, args, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("AgentOSPlanRepo - ListPlanRefs - builder: %w", err)
	}

	rows, err := r.Pool.Query(ctx, query, args...)
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

func (r *AgentOSPlanRepo) planRefsBuilder(scope *agentosplan.PlanRefScope) (sq.SelectBuilder, error) {
	if scope.Limit < 0 {
		return sq.SelectBuilder{}, fmt.Errorf("%w: plan ref limit must be non-negative", agentoscore.ErrInvalidPlanScope)
	}

	if slices.Contains(scope.LifecycleStates, "") {
		return sq.SelectBuilder{}, fmt.Errorf("%w: lifecycle state is required", agentoscore.ErrInvalidPlanScope)
	}

	builder := r.Builder.
		Select(_colPlanID, _colAccountID, _colProjectID).
		From("plans").
		OrderBy("updated_at ASC", "plan_id ASC")

	builder = applyOptionalEq(builder, _colAccountID, scope.AccountID)
	builder = applyOptionalEq(builder, _colProjectID, scope.ProjectID)

	if len(scope.LifecycleStates) > 0 {
		builder = builder.Where(sq.Eq{"lifecycle_state": scope.LifecycleStates})
	}

	if !scope.UpdatedAfter.IsZero() {
		builder = builder.Where(sq.Gt{"updated_at": scope.UpdatedAfter})
	}

	return applyOptionalLimit(builder, scope.Limit), nil
}

// SavePlanState persists a full plan state snapshot, upserting the plan row and its node statuses.
func (r *AgentOSPlanRepo) SavePlanState(ctx context.Context, snapshot *agentosplan.PlanStateSnapshot) error {
	normalized, err := normalizePlanStateSnapshotForPostgres(snapshot)
	if err != nil {
		return err
	}

	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - SavePlanState - begin: %w", err)
	}

	defer func() {
		errcheckIgnore(tx.Rollback(ctx))
	}()

	if err := r.savePlanStateWithTx(ctx, tx, &normalized, false); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("AgentOSPlanRepo - SavePlanState - commit: %w", err)
	}

	return nil
}

func normalizePlanStateSnapshotForPostgres(snapshot *agentosplan.PlanStateSnapshot) (agentosplan.PlanStateSnapshot, error) {
	if err := agentosplan.ValidateRunPlanScope(&snapshot.Spec); err != nil {
		return agentosplan.PlanStateSnapshot{}, err
	}

	normalized := *snapshot
	if snapshot.Spec.IdempotencyKey == "" {
		return agentosplan.PlanStateSnapshot{}, fmt.Errorf("%w: plan idempotency key is required", agentoscore.ErrInvalidRunPlan)
	}

	normalized.Spec.RequestedAt = agentosplan.NormalizeDurableTimestamp(normalized.Spec.RequestedAt)
	if normalized.Status.PlanID == "" {
		normalized.Status.PlanID = normalized.Spec.PlanID
	}

	if normalized.Status.LifecycleState == "" {
		normalized.Status.LifecycleState = agentos.PlanLifecyclePending
	}

	if normalized.Status.UpdatedAt.IsZero() {
		normalized.Status.UpdatedAt = time.Now().UTC()
	}

	state, err := agentosplan.NewStateFromStatus(&normalized.Spec, &normalized.Status)
	if err != nil {
		return agentosplan.PlanStateSnapshot{}, err
	}

	normalized.Status = state.Status

	return normalized, nil
}

func (r *AgentOSPlanRepo) savePlanStateWithTx(ctx context.Context, tx pgx.Tx, snapshot *agentosplan.PlanStateSnapshot, allowCreate bool) error {
	specJSON, err := json.Marshal(snapshot.Spec)
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - SavePlanState - marshal spec: %w", err)
	}

	statusJSON, err := json.Marshal(snapshot.Status)
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - SavePlanState - marshal status: %w", err)
	}

	existingSpec, exists, err := r.planSpecForUpdate(ctx, tx, snapshot.Spec.PlanID)
	if err != nil {
		return err
	}

	if err := validatePlanStateSaveTarget(&existingSpec, &snapshot.Spec, exists, allowCreate); err != nil {
		return err
	}

	query, args, err := r.buildPlanUpsertQuery(snapshot, specJSON, statusJSON)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("AgentOSPlanRepo - SavePlanState - plan exec: %w", err)
	}

	capabilityByNode := r.buildNodeCapabilityMap(snapshot.Spec.Nodes)
	for i := range snapshot.Status.Nodes {
		node := &snapshot.Status.Nodes[i]
		if err := r.upsertPlanNode(ctx, tx, snapshot.Spec.PlanID, capabilityByNode[node.NodeID], node); err != nil {
			return err
		}
	}

	return r.deleteStalePlanNodes(ctx, tx, snapshot.Spec.PlanID, snapshot.Status.Nodes)
}

func validatePlanStateSaveTarget(existingSpec, spec *agentos.RunPlanSpec, exists, allowCreate bool) error {
	if exists {
		return agentosplan.ValidatePlanStateIdentity(existingSpec, spec)
	}

	if !allowCreate {
		return fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, spec.PlanID)
	}

	return nil
}

func applyOptionalEq(builder sq.SelectBuilder, column, value string) sq.SelectBuilder {
	if value == "" {
		return builder
	}

	return builder.Where(sq.Eq{column: value})
}

func applyOptionalLimit(builder sq.SelectBuilder, limit int) sq.SelectBuilder {
	if limit <= 0 {
		return builder
	}

	return builder.Limit(uint64(limit))
}

func (r *AgentOSPlanRepo) buildPlanUpsertQuery(snapshot *agentosplan.PlanStateSnapshot, specJSON, statusJSON []byte) (query string, args []any, err error) {
	idempotencyKey := snapshot.Spec.IdempotencyKey

	query, args, err = r.Builder.
		Insert("plans").
		Columns(
			_colPlanID,
			"thread_id",
			_colAccountID,
			_colProjectID,
			_colIDempotencyKey,
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
		return "", nil, fmt.Errorf("AgentOSPlanRepo - SavePlanState - plan builder: %w", err)
	}

	return query, args, nil
}

func (r *AgentOSPlanRepo) buildNodeCapabilityMap(nodes []agentos.PlanNodeSpec) map[string]string {
	capabilityByNode := make(map[string]string, len(nodes))
	for i := range nodes {
		node := &nodes[i]
		capabilityByNode[node.NodeID] = node.Capability
	}

	return capabilityByNode
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

func (r *AgentOSPlanRepo) upsertPlanNode(ctx context.Context, tx pgx.Tx, planID, capability string, node *agentos.PlanNodeStatus) error {
	statusJSON, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - upsertPlanNode - marshal: %w", err)
	}

	query, args, err := r.Builder.
		Insert("plan_nodes").
		Columns(
			_colPlanID,
			"node_id",
			_colRunID,
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

	if _, err := tx.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("AgentOSPlanRepo - upsertPlanNode - exec: %w", err)
	}

	return nil
}

func (r *AgentOSPlanRepo) deleteStalePlanNodes(ctx context.Context, tx pgx.Tx, planID string, nodes []agentos.PlanNodeStatus) error {
	nodeIDs := make([]string, 0, len(nodes))
	for i := range nodes {
		node := nodes[i]
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

// LoadPlanState loads the plan state snapshot for the given plan ID, including node statuses.
func (r *AgentOSPlanRepo) LoadPlanState(ctx context.Context, planID string) (agentosplan.PlanStateSnapshot, bool, error) {
	if planID == "" {
		return agentosplan.PlanStateSnapshot{}, false, fmt.Errorf("%w: plan id is required", agentoscore.ErrInvalidRunPlan)
	}

	return r.loadPlanStateByWhere(ctx, sq.Eq{_colPlanID: planID}, "LoadPlanState")
}

func (r *AgentOSPlanRepo) loadPlanStateByWhere(ctx context.Context, where sq.Eq, op string) (agentosplan.PlanStateSnapshot, bool, error) {
	query, args, err := r.Builder.
		Select("spec_json", "status_json").
		From("plans").
		Where(where).
		ToSql()
	if err != nil {
		return agentosplan.PlanStateSnapshot{}, false, fmt.Errorf("AgentOSPlanRepo - %s - builder: %w", op, err)
	}

	var (
		specJSON   []byte
		statusJSON []byte
	)

	err = r.Pool.QueryRow(ctx, query, args...).Scan(&specJSON, &statusJSON)
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

	state, err := agentosplan.NewStateFromStatus(&spec, &status)
	if err != nil {
		return agentosplan.PlanStateSnapshot{}, false, err
	}

	status = state.Status

	return agentosplan.PlanStateSnapshot{
		Spec:   spec,
		Status: status,
	}, true, nil
}

func (r *AgentOSPlanRepo) loadPlanNodeStatuses(ctx context.Context, planID string) ([]agentos.PlanNodeStatus, error) {
	query, args, err := r.Builder.
		Select("status_json").
		From("plan_nodes").
		Where(sq.Eq{_colPlanID: planID}).
		OrderBy("node_id ASC").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("AgentOSPlanRepo - loadPlanNodeStatuses - builder: %w", err)
	}

	rows, err := r.Pool.Query(ctx, query, args...)
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
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, fmt.Errorf("%w: plan idempotency key is required", agentoscore.ErrInvalidRunPlan)
	}

	query, args, err := r.Builder.
		Select("spec_json", "status_json").
		From("plans").
		Where(sq.Eq{_colAccountID: accountID, _colProjectID: projectID, _colIDempotencyKey: idempotencyKey}).
		ToSql()
	if err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, fmt.Errorf("AgentOSPlanRepo - planByIdempotencyKey - builder: %w", err)
	}

	var (
		specJSON   []byte
		statusJSON []byte
	)

	err = r.Pool.QueryRow(ctx, query, args...).Scan(&specJSON, &statusJSON)
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

// PersistPlanTransition atomically saves the given plan state snapshot and appends its transition event idempotently.
func (r *AgentOSPlanRepo) PersistPlanTransition(ctx context.Context, snapshot *agentosplan.PlanStateSnapshot, event *agentos.PlanEvent, idempotencyKey string) (agentos.PlanEvent, error) {
	normalizedSnapshot, normalizedEvent, transitionIdentity, err := normalizePlanTransitionForPostgres(snapshot, event, idempotencyKey)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - PersistPlanTransition - begin: %w", err)
	}

	defer func() {
		errcheckIgnore(tx.Rollback(ctx))
	}()

	scope, planFound, err := r.lockPlanRow(ctx, tx, normalizedEvent.PlanID)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	existing, exists, err := r.existingPlanTransitionEvent(ctx, tx, scope, planFound, &normalizedEvent, idempotencyKey, transitionIdentity)
	if err != nil || exists {
		return existing, err
	}

	if err := r.savePlanStateWithTx(ctx, tx, &normalizedSnapshot, false); err != nil {
		return agentos.PlanEvent{}, err
	}

	stored, err := r.appendPlanEventWithTx(ctx, tx, &normalizedEvent, idempotencyKey, transitionIdentity)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - PersistPlanTransition - commit: %w", err)
	}

	return stored, nil
}

func normalizePlanTransitionForPostgres(snapshot *agentosplan.PlanStateSnapshot, event *agentos.PlanEvent, idempotencyKey string) (agentosplan.PlanStateSnapshot, agentos.PlanEvent, agentosplan.PlanTransitionSnapshotIdentity, error) {
	normalizedSnapshot, err := normalizePlanStateSnapshotForPostgres(snapshot)
	if err != nil {
		return agentosplan.PlanStateSnapshot{}, agentos.PlanEvent{}, agentosplan.PlanTransitionSnapshotIdentity{}, err
	}

	normalizedEvent, err := normalizePlanEventAppendForPostgres(event, idempotencyKey)
	if err != nil {
		return agentosplan.PlanStateSnapshot{}, agentos.PlanEvent{}, agentosplan.PlanTransitionSnapshotIdentity{}, err
	}

	if normalizedEvent.PlanID != normalizedSnapshot.Spec.PlanID {
		return agentosplan.PlanStateSnapshot{}, agentos.PlanEvent{}, agentosplan.PlanTransitionSnapshotIdentity{}, fmt.Errorf("%w: event plan %q does not match snapshot plan %q", agentoscore.ErrInvalidPlanEvent, normalizedEvent.PlanID, normalizedSnapshot.Spec.PlanID)
	}

	transitionIdentity, err := agentosplan.NewPlanTransitionSnapshotIdentity(&normalizedSnapshot, idempotencyKey)
	if err != nil {
		return agentosplan.PlanStateSnapshot{}, agentos.PlanEvent{}, agentosplan.PlanTransitionSnapshotIdentity{}, err
	}

	normalizedEvent, err = agentosplan.ScopePlanEventToSpec(&normalizedEvent, &normalizedSnapshot.Spec)
	if err != nil {
		return agentosplan.PlanStateSnapshot{}, agentos.PlanEvent{}, agentosplan.PlanTransitionSnapshotIdentity{}, err
	}

	return normalizedSnapshot, normalizedEvent, transitionIdentity, nil
}

func (r *AgentOSPlanRepo) existingPlanTransitionEvent(
	ctx context.Context,
	tx pgx.Tx,
	scope planTenantScope,
	planFound bool,
	event *agentos.PlanEvent,
	idempotencyKey string,
	transitionIdentity agentosplan.PlanTransitionSnapshotIdentity,
) (agentos.PlanEvent, bool, error) {
	if !planFound {
		return agentos.PlanEvent{}, false, nil
	}

	existing, exists, err := r.planEventByIdempotencyKeyWith(ctx, tx, scope, event.PlanID, idempotencyKey)
	if err != nil || !exists {
		return agentos.PlanEvent{}, false, err
	}

	err = r.validatePlanEventIdempotency(&existing.Event, existing.TransitionSnapshotDigest, event, transitionIdentity)

	return existing.Event, true, err
}

func (r *AgentOSPlanRepo) lockPlanRow(ctx context.Context, tx pgx.Tx, planID string) (planTenantScope, bool, error) {
	var scope planTenantScope

	err := tx.QueryRow(ctx, `
SELECT account_id, project_id
FROM plans
WHERE plan_id = $1
FOR UPDATE`, planID).Scan(&scope.AccountID, &scope.ProjectID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return scope, false, nil
		}

		return scope, false, fmt.Errorf("AgentOSPlanRepo - PersistPlanTransition - lock plan: %w", err)
	}

	return scope, true, nil
}

// AppendPlanEvent appends a durable plan event with a monotonically increasing sequence number.
func (r *AgentOSPlanRepo) AppendPlanEvent(ctx context.Context, event *agentos.PlanEvent, idempotencyKey string) (agentos.PlanEvent, error) {
	normalizedEvent, err := normalizePlanEventAppendForPostgres(event, idempotencyKey)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - AppendPlanEvent - begin: %w", err)
	}

	defer func() {
		errcheckIgnore(tx.Rollback(ctx))
	}()

	stored, err := r.appendPlanEventWithTx(ctx, tx, &normalizedEvent, idempotencyKey, agentosplan.PlanTransitionSnapshotIdentity{})
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - AppendPlanEvent - commit: %w", err)
	}

	return stored, nil
}

func normalizePlanEventAppendForPostgres(event *agentos.PlanEvent, idempotencyKey string) (agentos.PlanEvent, error) {
	if event.PlanID == "" {
		return agentos.PlanEvent{}, fmt.Errorf("%w: plan id is required", agentoscore.ErrInvalidPlanEvent)
	}

	if idempotencyKey == "" {
		return agentos.PlanEvent{}, fmt.Errorf("%w: plan event idempotency key is required", agentoscore.ErrInvalidPlanEvent)
	}

	return agentosplan.NormalizePlanEventAppendRequest(event), nil
}

func (r *AgentOSPlanRepo) appendPlanEventWithTx(ctx context.Context, tx pgx.Tx, requestedEvent *agentos.PlanEvent, idempotencyKey string, transitionIdentity agentosplan.PlanTransitionSnapshotIdentity) (agentos.PlanEvent, error) {
	event := *requestedEvent

	var (
		currentSequence int64
		scope           planTenantScope
	)

	err := tx.QueryRow(ctx, `
SELECT event_sequence, account_id, project_id
FROM plans
WHERE plan_id = $1
FOR UPDATE`, event.PlanID).Scan(&currentSequence, &scope.AccountID, &scope.ProjectID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentos.PlanEvent{}, fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, event.PlanID)
		}

		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - AppendPlanEvent - lock plan: %w", err)
	}

	spec := agentos.RunPlanSpec{
		PlanID:    event.PlanID,
		AccountID: scope.AccountID,
		ProjectID: scope.ProjectID,
	}

	event, err = agentosplan.ScopePlanEventToSpec(&event, &spec)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	existing, exists, err := r.planEventByIdempotencyKeyWith(ctx, tx, scope, event.PlanID, idempotencyKey)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	if exists {
		if err := r.validatePlanEventIdempotency(&existing.Event, existing.TransitionSnapshotDigest, &event, transitionIdentity); err != nil {
			return agentos.PlanEvent{}, err
		}

		return existing.Event, nil
	}

	stored, err := r.buildAndInsertPlanEvent(ctx, tx, &event, currentSequence, scope, idempotencyKey, transitionIdentity)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	return stored, nil
}

func (r *AgentOSPlanRepo) buildAndInsertPlanEvent(ctx context.Context, tx pgx.Tx, event *agentos.PlanEvent, currentSequence int64, scope planTenantScope, idempotencyKey string, transitionIdentity agentosplan.PlanTransitionSnapshotIdentity) (agentos.PlanEvent, error) {
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

	event.Timestamp = agentosplan.NormalizeDurableTimestamp(event.Timestamp)
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

	transitionSnapshotDigest := transitionIdentity.Digest

	var transitionSnapshotJSON any
	if transitionIdentity.Digest != "" {
		transitionSnapshotJSON = transitionIdentity.JSON
	}

	var storedJSON []byte

	err = tx.QueryRow(
		ctx, planEventInsertSQL,
		event.EventID,
		event.PlanID,
		scope.AccountID,
		scope.ProjectID,
		event.NodeID,
		event.RunID,
		string(event.EventType),
		event.Sequence,
		idempotencyKey,
		transitionSnapshotDigest,
		transitionSnapshotJSON,
		payloadJSON,
		eventJSON,
		event.Timestamp,
	).Scan(&storedJSON)
	if err != nil {
		return r.resolvePlanEventInsertConflict(ctx, tx, scope, event.PlanID, idempotencyKey, event, transitionIdentity, err)
	}

	return agentos.UnmarshalPlanEvent(storedJSON)
}

const planEventInsertSQL = `
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
    transition_snapshot_digest,
    transition_snapshot_json,
    payload_json,
    event_json,
    timestamp
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
RETURNING event_json`

func (r *AgentOSPlanRepo) validatePlanEventIdempotency(existing *agentos.PlanEvent, existingDigest string, requested *agentos.PlanEvent, transitionIdentity agentosplan.PlanTransitionSnapshotIdentity) error {
	if err := agentosplan.ValidatePlanEventIdempotency(existing, requested); err != nil {
		return err
	}

	if transitionIdentity.Digest != "" {
		if err := agentosplan.ValidatePlanTransitionIdempotency(existingDigest, transitionIdentity); err != nil {
			return err
		}
	}

	return nil
}

func (r *AgentOSPlanRepo) resolvePlanEventInsertConflict(ctx context.Context, tx pgx.Tx, scope planTenantScope, planID, idempotencyKey string, requestedEvent *agentos.PlanEvent, transitionIdentity agentosplan.PlanTransitionSnapshotIdentity, err error) (agentos.PlanEvent, error) {
	if !isPostgresUniqueViolation(err) {
		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - AppendPlanEvent - insert: %w", err)
	}

	existing, exists, lookupErr := r.planEventByIdempotencyKeyWith(ctx, tx, scope, planID, idempotencyKey)
	if lookupErr != nil {
		return agentos.PlanEvent{}, lookupErr
	}

	if !exists {
		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - AppendPlanEvent - insert: %w", err)
	}

	if err := r.validatePlanEventIdempotency(&existing.Event, existing.TransitionSnapshotDigest, requestedEvent, transitionIdentity); err != nil {
		return agentos.PlanEvent{}, err
	}

	return existing.Event, nil
}

type planEventRowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type planEventIdempotencyRecord struct {
	Event                    agentos.PlanEvent
	TransitionSnapshotDigest string
}

func (r *AgentOSPlanRepo) planEventByIdempotencyKeyWith(ctx context.Context, querier planEventRowQuerier, scope planTenantScope, planID, idempotencyKey string) (planEventIdempotencyRecord, bool, error) {
	query, args, err := r.Builder.
		Select("event_json", "transition_snapshot_digest").
		From("plan_events").
		Where(sq.Eq{
			_colAccountID:      scope.AccountID,
			_colProjectID:      scope.ProjectID,
			_colPlanID:         planID,
			_colIDempotencyKey: idempotencyKey,
		}).
		ToSql()
	if err != nil {
		return planEventIdempotencyRecord{}, false, fmt.Errorf("AgentOSPlanRepo - planEventByIdempotencyKey - builder: %w", err)
	}

	var (
		eventJSON                []byte
		transitionSnapshotDigest string
	)

	err = querier.QueryRow(ctx, query, args...).Scan(&eventJSON, &transitionSnapshotDigest)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return planEventIdempotencyRecord{}, false, nil
		}

		return planEventIdempotencyRecord{}, false, fmt.Errorf("AgentOSPlanRepo - planEventByIdempotencyKey - query: %w", err)
	}

	event, err := agentos.UnmarshalPlanEvent(eventJSON)
	if err != nil {
		return planEventIdempotencyRecord{}, false, err
	}

	return planEventIdempotencyRecord{
		Event:                    event,
		TransitionSnapshotDigest: transitionSnapshotDigest,
	}, true, nil
}

// ListPlanEvents returns plan events matching the given stream scope, ordered by sequence.
func (r *AgentOSPlanRepo) ListPlanEvents(ctx context.Context, scope *agentos.PlanStreamScope, limit int) ([]agentos.PlanEvent, error) {
	if err := r.authorizePlanStreamScope(ctx, scope); err != nil {
		return nil, err
	}

	query, args, err := r.planEventsBuilder(scope, limit).ToSql()
	if err != nil {
		return nil, fmt.Errorf("AgentOSPlanRepo - ListPlanEvents - builder: %w", err)
	}

	rows, err := r.Pool.Query(ctx, query, args...)
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

func (r *AgentOSPlanRepo) authorizePlanStreamScope(ctx context.Context, scope *agentos.PlanStreamScope) error {
	if err := agentosplan.ValidatePlanStreamScope(scope); err != nil {
		return err
	}

	spec, _, exists, err := r.GetPlan(ctx, scope.PlanID)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, scope.PlanID)
	}

	return agentosplan.ValidatePlanTenantAccess(agentos.PlanRef{PlanID: scope.PlanID, AccountID: scope.AccountID, ProjectID: scope.ProjectID}, &spec)
}

func (r *AgentOSPlanRepo) planEventsBuilder(scope *agentos.PlanStreamScope, limit int) sq.SelectBuilder {
	builder := r.Builder.
		Select("event_json").
		From("plan_events").
		Where(sq.Eq{_colPlanID: scope.PlanID}).
		Where(sq.Eq{_colAccountID: scope.AccountID}).
		Where(sq.Eq{_colProjectID: scope.ProjectID}).
		Where(sq.Gt{"sequence": scope.AfterSequence}).
		OrderBy("sequence ASC")

	builder = applyOptionalEq(builder, "node_id", scope.NodeID)
	builder = applyOptionalEq(builder, _colRunID, scope.RunID)

	return applyOptionalLimit(builder, limit)
}

// GetPlanMetricCheckpoint loads the metrics checkpoint stored by an exporter for a plan.
func (r *AgentOSPlanRepo) GetPlanMetricCheckpoint(ctx context.Context, exporterID string, ref agentos.PlanRef) (agentosplan.PlanMetricCheckpoint, bool, error) {
	if exporterID == "" {
		return agentosplan.PlanMetricCheckpoint{}, false, fmt.Errorf("%w: metrics exporter id is required", agentoscore.ErrInvalidRunPlan)
	}

	if err := agentosplan.ValidatePlanRef(ref); err != nil {
		return agentosplan.PlanMetricCheckpoint{}, false, err
	}

	query, args, err := r.Builder.
		Select("exporter_id", _colPlanID, _colAccountID, _colProjectID, "sequence", "projection_json", "updated_at").
		From("plan_metric_checkpoints").
		Where(sq.Eq{
			"exporter_id": exporterID,
			_colPlanID:    ref.PlanID,
			_colAccountID: ref.AccountID,
			_colProjectID: ref.ProjectID,
		}).
		ToSql()
	if err != nil {
		return agentosplan.PlanMetricCheckpoint{}, false, fmt.Errorf("AgentOSPlanRepo - GetPlanMetricCheckpoint - builder: %w", err)
	}

	var (
		checkpoint     agentosplan.PlanMetricCheckpoint
		projectionJSON []byte
	)

	err = r.Pool.QueryRow(ctx, query, args...).Scan(
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

	if err := agentosplan.ValidatePlanMetricCheckpointRef(&checkpoint, exporterID, ref); err != nil {
		return agentosplan.PlanMetricCheckpoint{}, false, err
	}

	return checkpoint, true, nil
}

// SavePlanMetricCheckpoint upserts a metrics checkpoint for a plan, rejecting backward sequence moves.
func (r *AgentOSPlanRepo) SavePlanMetricCheckpoint(ctx context.Context, checkpoint *agentosplan.PlanMetricCheckpoint) error {
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
		return fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, checkpoint.PlanID)
	}

	if scope.AccountID != checkpoint.AccountID || scope.ProjectID != checkpoint.ProjectID {
		return fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, checkpoint.PlanID)
	}

	projectionJSON, err := json.Marshal(checkpoint.Projection)
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - SavePlanMetricCheckpoint - marshal projection: %w", err)
	}

	result, err := r.Pool.Exec(
		ctx, `
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
		return fmt.Errorf("%w: metrics checkpoint sequence moved backward for plan %q", agentoscore.ErrInvalidRunPlan, checkpoint.PlanID)
	}

	return nil
}

// RecordPlanMetric persists a plan metric sample idempotently.
func (r *AgentOSPlanRepo) RecordPlanMetric(ctx context.Context, sample *agentosplan.PlanMetricSample) error {
	normalized := agentosplan.NormalizePlanMetricSample(sample)

	sample = &normalized
	if err := agentosplan.ValidatePlanMetricSample(sample); err != nil {
		return err
	}

	scope, exists, err := planTenantScopeByPlanID(ctx, r.Pool, sample.PlanID)
	if err != nil {
		return err
	}

	if err := validatePlanMetricScope(sample, scope, exists); err != nil {
		return err
	}

	labelsJSON, err := json.Marshal(planMetricLabelsForStorage(sample.Labels))
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - RecordPlanMetric - marshal labels: %w", err)
	}

	result, err := r.Pool.Exec(
		ctx, `INSERT INTO plan_metric_samples (metric_name, plan_id, account_id, project_id, node_id, run_id, event_id, sequence, value, unit, labels_json, sample_timestamp) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT DO NOTHING`,
		string(sample.Name), sample.PlanID, sample.AccountID, sample.ProjectID, sample.NodeID, sample.RunID, sample.EventID, sample.Sequence, sample.Value, sample.Unit, labelsJSON, sample.Timestamp,
	)
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - RecordPlanMetric - insert: %w", err)
	}

	if result.RowsAffected() == 1 {
		return nil
	}

	return r.validateMetricSampleConflict(ctx, sample)
}

func validatePlanMetricScope(sample *agentosplan.PlanMetricSample, scope planTenantScope, exists bool) error {
	if !exists {
		return fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, sample.PlanID)
	}

	if scope.AccountID != sample.AccountID || scope.ProjectID != sample.ProjectID {
		return fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, sample.PlanID)
	}

	return nil
}

func (r *AgentOSPlanRepo) validateMetricSampleConflict(ctx context.Context, sample *agentosplan.PlanMetricSample) error {
	key := sample.Key()

	existing, exists, err := r.planMetricSampleByKey(ctx, &key)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("%w: metric sample identity conflict for plan %q event %q", agentoscore.ErrInvalidRunPlan, sample.PlanID, sample.EventID)
	}

	return agentosplan.ValidatePlanMetricSampleIdempotency(&existing, sample)
}

func (r *AgentOSPlanRepo) planMetricSampleByKey(ctx context.Context, key *agentosplan.PlanMetricSampleKey) (agentosplan.PlanMetricSample, bool, error) {
	var (
		sample     agentosplan.PlanMetricSample
		name       string
		labelsJSON []byte
	)

	err := r.Pool.QueryRow(
		ctx, `SELECT metric_name, plan_id, account_id, project_id, node_id, run_id, event_id, sequence, value, unit, labels_json, sample_timestamp FROM plan_metric_samples WHERE metric_name = $1 AND plan_id = $2 AND node_id = $3 AND run_id = $4 AND event_id = $5 AND sequence = $6`,
		string(key.Name), key.PlanID, key.NodeID, key.RunID, key.EventID, key.Sequence,
	).Scan(
		&name, &sample.PlanID, &sample.AccountID, &sample.ProjectID, &sample.NodeID, &sample.RunID, &sample.EventID, &sample.Sequence, &sample.Value, &sample.Unit, &labelsJSON, &sample.Timestamp,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentosplan.PlanMetricSample{}, false, nil
		}

		return agentosplan.PlanMetricSample{}, false, fmt.Errorf("AgentOSPlanRepo - planMetricSampleByKey - query: %w", err)
	}

	if len(labelsJSON) > 0 {
		if err := json.Unmarshal(labelsJSON, &sample.Labels); err != nil {
			return agentosplan.PlanMetricSample{}, false, fmt.Errorf("AgentOSPlanRepo - planMetricSampleByKey - decode labels: %w", err)
		}
	}

	sample.Name = agentosplan.PlanMetricName(name)

	return sample, true, nil
}

func planMetricLabelsForStorage(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return map[string]string{}
	}

	return labels
}

// RecordAudit persists an audit record for a plan, returning the stored record and whether it was newly created.
func (r *AgentOSPlanRepo) RecordAudit(ctx context.Context, record *agentosplan.AuditRecord) (agentosplan.AuditRecord, bool, error) {
	ref, scope, err := r.prepareAuditRecord(ctx, record)
	if err != nil {
		return agentosplan.AuditRecord{}, false, err
	}

	existing, exists, err := r.existingAuditRecord(ctx, ref, record)
	if err != nil || exists {
		return existing, false, err
	}

	if err := r.validateAuditOwnership(ctx, record, scope); err != nil {
		return agentosplan.AuditRecord{}, false, err
	}

	setAuditRecordDefaults(record, ref)

	stored, err := r.insertAuditRecord(ctx, record, ref)
	if err != nil {
		return agentosplan.AuditRecord{}, false, err
	}

	return stored, true, nil
}

func (r *AgentOSPlanRepo) prepareAuditRecord(ctx context.Context, record *agentosplan.AuditRecord) (agentosplan.AuditRef, planTenantScope, error) {
	if record.PlanID == "" {
		return agentosplan.AuditRef{}, planTenantScope{}, fmt.Errorf("%w: plan id is required", agentoscore.ErrInvalidRunPlan)
	}

	if record.Action == "" {
		return agentosplan.AuditRef{}, planTenantScope{}, fmt.Errorf("%w: audit action is required", agentoscore.ErrInvalidRunPlan)
	}

	if record.IdempotencyKey == "" {
		return agentosplan.AuditRef{}, planTenantScope{}, fmt.Errorf("%w: audit idempotency key is required", agentoscore.ErrInvalidRunPlan)
	}

	scope, exists, err := planTenantScopeByPlanID(ctx, r.Pool, record.PlanID)
	if err != nil {
		return agentosplan.AuditRef{}, planTenantScope{}, err
	}

	if !exists {
		return agentosplan.AuditRef{}, planTenantScope{}, fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, record.PlanID)
	}

	record.AccountID = scope.AccountID
	record.ProjectID = scope.ProjectID

	return agentosplan.AuditRefFromRecord(record), scope, nil
}

func (r *AgentOSPlanRepo) existingAuditRecord(ctx context.Context, ref agentosplan.AuditRef, record *agentosplan.AuditRecord) (agentosplan.AuditRecord, bool, error) {
	existing, exists, err := r.GetAuditRecord(ctx, ref)
	if err != nil || !exists {
		return agentosplan.AuditRecord{}, false, err
	}

	if err := agentosplan.ValidateAuditIdempotency(&existing, record); err != nil {
		return agentosplan.AuditRecord{}, false, err
	}

	return existing, true, nil
}

func setAuditRecordDefaults(record *agentosplan.AuditRecord, ref agentosplan.AuditRef) {
	if record.AuditID == "" {
		record.AuditID = agentosplan.AuditIDFromRef(ref)
	}

	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}

	if record.Payload == nil {
		record.Payload = map[string]any{}
	}
}

func (r *AgentOSPlanRepo) insertAuditRecord(ctx context.Context, record *agentosplan.AuditRecord, ref agentosplan.AuditRef) (agentosplan.AuditRecord, error) {
	payloadJSON, err := json.Marshal(record.Payload)
	if err != nil {
		return agentosplan.AuditRecord{}, fmt.Errorf("AgentOSPlanRepo - RecordAudit - marshal payload: %w", err)
	}

	_, err = r.Pool.Exec(
		ctx, `
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
		nullableString(record.RunID),
		nullableString(record.NodeID),
		record.ActorID,
		string(record.Action),
		record.IdempotencyKey,
		payloadJSON,
		record.CreatedAt,
	)
	if err != nil {
		return resolveUniqueInsertConflict(
			err,
			"AgentOSPlanRepo - RecordAudit - insert",
			func() (agentosplan.AuditRecord, bool, error) { return r.GetAuditRecord(ctx, ref) },
			func(existing *agentosplan.AuditRecord) error {
				return agentosplan.ValidateAuditIdempotency(existing, record)
			},
		)
	}

	stored, exists, err := r.GetAuditRecord(ctx, ref)
	if err != nil {
		return agentosplan.AuditRecord{}, err
	}

	if !exists {
		return agentosplan.AuditRecord{}, fmt.Errorf(
			"AgentOSPlanRepo - RecordAudit - insert: %w",
			errInsertedAuditRecordNotFound,
		)
	}

	return stored, nil
}

func resolveUniqueInsertConflict[T any](err error, operation string, lookup func() (T, bool, error), validate func(*T) error) (T, error) {
	var zero T

	if !isPostgresUniqueViolation(err) {
		return zero, fmt.Errorf("%s: %w", operation, err)
	}

	existing, exists, lookupErr := lookup()
	if lookupErr != nil {
		return zero, lookupErr
	}

	if !exists {
		return zero, fmt.Errorf("%s: %w", operation, err)
	}

	if err := validate(&existing); err != nil {
		return zero, err
	}

	return existing, nil
}

// GetAuditRecord loads the audit record identified by the given reference.
func (r *AgentOSPlanRepo) GetAuditRecord(ctx context.Context, ref agentosplan.AuditRef) (agentosplan.AuditRecord, bool, error) {
	if err := agentosplan.ValidateAuditRef(ref); err != nil {
		return agentosplan.AuditRecord{}, false, err
	}

	query, args, err := r.Builder.
		Select("audit_id", _colPlanID, _colAccountID, _colProjectID, "COALESCE(run_id, '') AS run_id", "COALESCE(node_id, '') AS node_id", "actor_id", "action", _colIDempotencyKey, "payload_json", "created_at").
		From("audit_logs").
		Where(sq.Eq{
			_colPlanID:         ref.PlanID,
			_colAccountID:      ref.AccountID,
			_colProjectID:      ref.ProjectID,
			_colIDempotencyKey: ref.IdempotencyKey,
		}).
		ToSql()
	if err != nil {
		return agentosplan.AuditRecord{}, false, fmt.Errorf("AgentOSPlanRepo - GetAuditRecord - builder: %w", err)
	}

	var (
		record      agentosplan.AuditRecord
		action      string
		payloadJSON []byte
	)

	err = r.Pool.QueryRow(ctx, query, args...).Scan(
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

// ListAuditRecords returns audit records matching the given plan audit scope.
func (r *AgentOSPlanRepo) ListAuditRecords(ctx context.Context, scope *agentos.PlanAuditScope) ([]agentos.PlanAuditRecord, error) {
	if err := agentosplan.ValidatePlanAuditScope(scope); err != nil {
		return nil, err
	}

	spec, _, exists, err := r.GetPlan(ctx, scope.PlanID)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, scope.PlanID)
	}

	if err := agentosplan.ValidatePlanTenantAccess(agentos.PlanRef{PlanID: scope.PlanID, AccountID: scope.AccountID, ProjectID: scope.ProjectID}, &spec); err != nil {
		return nil, err
	}

	query, args, err := r.auditRecordsBuilder(scope).ToSql()
	if err != nil {
		return nil, fmt.Errorf("AgentOSPlanRepo - ListAuditRecords - builder: %w", err)
	}

	rows, err := r.Pool.Query(ctx, query, args...)
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

func (r *AgentOSPlanRepo) auditRecordsBuilder(scope *agentos.PlanAuditScope) sq.SelectBuilder {
	builder := r.Builder.
		Select("audit_id", _colPlanID, _colAccountID, _colProjectID, "COALESCE(run_id, '') AS run_id", "COALESCE(node_id, '') AS node_id", "actor_id", "action", _colIDempotencyKey, "payload_json", "created_at").
		From("audit_logs").
		Where(sq.Eq{_colPlanID: scope.PlanID}).
		Where(sq.Eq{_colAccountID: scope.AccountID}).
		Where(sq.Eq{_colProjectID: scope.ProjectID}).
		OrderBy("created_at ASC", "audit_id ASC")

	builder = applyOptionalEq(builder, "node_id", scope.NodeID)
	builder = applyOptionalEq(builder, _colRunID, scope.RunID)

	if scope.Action != "" {
		builder = builder.Where(sq.Eq{"action": string(scope.Action)})
	}

	return applyOptionalLimit(builder, scope.Limit)
}

func scanPlanAuditRecord(scanner interface{ Scan(dest ...any) error }) (agentos.PlanAuditRecord, error) {
	var (
		record      agentos.PlanAuditRecord
		action      string
		payloadJSON []byte
	)

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

func (r *AgentOSPlanRepo) validateAuditOwnership(ctx context.Context, record *agentosplan.AuditRecord, scope planTenantScope) error {
	if err := agentosplan.ValidateAuditNodeRunPair(record); err != nil {
		return err
	}

	if record.NodeID == "" {
		return nil
	}

	var (
		nodeRunID  string
		ownedRunID sql.NullString
	)

	err := r.Pool.QueryRow(ctx, `
SELECT n.run_id, r.run_id
FROM plan_nodes n
LEFT JOIN run_backend_index r
    ON r.plan_id = n.plan_id
   AND r.node_id = n.node_id
   AND r.run_id = $3
   AND r.account_id = $4
   AND r.project_id = $5
WHERE n.plan_id = $1
  AND n.node_id = $2`, record.PlanID, record.NodeID, record.RunID, scope.AccountID, scope.ProjectID).Scan(&nodeRunID, &ownedRunID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: audit node %q is not durable", agentoscore.ErrInvalidRunPlan, record.NodeID)
		}

		return fmt.Errorf("AgentOSPlanRepo - validateAuditOwnership - query: %w", err)
	}

	if nodeRunID != record.RunID {
		return fmt.Errorf("%w: audit node %q has durable run id %q, got %q", agentoscore.ErrInvalidRunPlan, record.NodeID, nodeRunID, record.RunID)
	}

	if !ownedRunID.Valid || ownedRunID.String == "" {
		return fmt.Errorf("%w: %s", agentoscore.ErrRunRouteNotFound, record.RunID)
	}

	return nil
}

// RecordPlanCommand persists a plan command, returning the stored record and whether it was newly created.
func (r *AgentOSPlanRepo) RecordPlanCommand(ctx context.Context, command *agentosplan.PlanCommandRecord) (agentosplan.PlanCommandRecord, bool, error) {
	ref, err := r.preparePlanCommandRecord(ctx, command)
	if err != nil {
		return agentosplan.PlanCommandRecord{}, false, err
	}

	existing, exists, err := r.existingPlanCommand(ctx, ref, command)
	if err != nil || exists {
		return existing, false, err
	}

	if err := setPlanCommandRecordDefaults(command, ref); err != nil {
		return agentosplan.PlanCommandRecord{}, false, err
	}

	stored, err := r.insertPlanCommand(ctx, command, ref)
	if err != nil {
		return agentosplan.PlanCommandRecord{}, false, err
	}

	return stored, true, nil
}

func (r *AgentOSPlanRepo) preparePlanCommandRecord(ctx context.Context, command *agentosplan.PlanCommandRecord) (agentosplan.PlanCommandRef, error) {
	if command.PlanID == "" {
		return agentosplan.PlanCommandRef{}, fmt.Errorf("%w: plan id is required", agentoscore.ErrInvalidRunPlan)
	}

	if command.Action == "" {
		return agentosplan.PlanCommandRef{}, fmt.Errorf("%w: command action is required", agentoscore.ErrInvalidRunPlan)
	}

	if command.IdempotencyKey == "" {
		return agentosplan.PlanCommandRef{}, fmt.Errorf("%w: command idempotency key is required", agentoscore.ErrInvalidRunPlan)
	}

	scope, exists, err := planTenantScopeByPlanID(ctx, r.Pool, command.PlanID)
	if err != nil {
		return agentosplan.PlanCommandRef{}, err
	}

	if !exists {
		return agentosplan.PlanCommandRef{}, fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, command.PlanID)
	}

	command.AccountID = scope.AccountID
	command.ProjectID = scope.ProjectID

	return agentosplan.PlanCommandRefFromRecord(command), nil
}

func (r *AgentOSPlanRepo) existingPlanCommand(ctx context.Context, ref agentosplan.PlanCommandRef, command *agentosplan.PlanCommandRecord) (agentosplan.PlanCommandRecord, bool, error) {
	existing, exists, err := r.GetPlanCommand(ctx, ref)
	if err != nil || !exists {
		return agentosplan.PlanCommandRecord{}, false, err
	}

	if err := agentosplan.ValidatePlanCommandIdempotency(&existing, command); err != nil {
		return agentosplan.PlanCommandRecord{}, false, err
	}

	return existing, true, nil
}

func setPlanCommandRecordDefaults(command *agentosplan.PlanCommandRecord, ref agentosplan.PlanCommandRef) error {
	if command.CommandID == "" {
		command.CommandID = agentosplan.PlanCommandIDFromRef(ref)
	}

	status, err := agentosplan.NormalizeNewPlanCommandStatus(command.Status)
	if err != nil {
		return err
	}

	command.Status = status
	if command.CreatedAt.IsZero() {
		command.CreatedAt = time.Now().UTC()
	}

	if command.UpdatedAt.IsZero() {
		command.UpdatedAt = command.CreatedAt
	}

	if command.Payload == nil {
		command.Payload = map[string]any{}
	}

	return nil
}

func (r *AgentOSPlanRepo) insertPlanCommand(ctx context.Context, command *agentosplan.PlanCommandRecord, ref agentosplan.PlanCommandRef) (agentosplan.PlanCommandRecord, error) {
	payloadJSON, err := json.Marshal(command.Payload)
	if err != nil {
		return agentosplan.PlanCommandRecord{}, fmt.Errorf("AgentOSPlanRepo - RecordPlanCommand - marshal payload: %w", err)
	}

	_, err = r.Pool.Exec(
		ctx, `
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
		return resolveUniqueInsertConflict(
			err,
			"AgentOSPlanRepo - RecordPlanCommand - insert",
			func() (agentosplan.PlanCommandRecord, bool, error) { return r.GetPlanCommand(ctx, ref) },
			func(existing *agentosplan.PlanCommandRecord) error {
				return agentosplan.ValidatePlanCommandIdempotency(existing, command)
			},
		)
	}

	return *command, nil
}

// GetPlanCommand loads the plan command identified by the given reference.
func (r *AgentOSPlanRepo) GetPlanCommand(ctx context.Context, ref agentosplan.PlanCommandRef) (agentosplan.PlanCommandRecord, bool, error) {
	if err := agentosplan.ValidatePlanCommandRef(ref); err != nil {
		return agentosplan.PlanCommandRecord{}, false, err
	}

	query, args, err := r.Builder.
		Select("command_id", _colPlanID, _colAccountID, _colProjectID, "actor_id", "action", _colIDempotencyKey, "payload_json", "status", "failure_reason", "created_at", "updated_at").
		From("plan_commands").
		Where(sq.Eq{
			_colPlanID:         ref.PlanID,
			_colAccountID:      ref.AccountID,
			_colProjectID:      ref.ProjectID,
			_colIDempotencyKey: ref.IdempotencyKey,
		}).
		ToSql()
	if err != nil {
		return agentosplan.PlanCommandRecord{}, false, fmt.Errorf("AgentOSPlanRepo - GetPlanCommand - builder: %w", err)
	}

	command, err := r.scanPlanCommandRow(ctx, r.Pool.QueryRow(ctx, query, args...))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentosplan.PlanCommandRecord{}, false, nil
		}

		return agentosplan.PlanCommandRecord{}, false, err
	}

	return command, true, nil
}

// ListRecoverablePlanCommands returns plan commands in recoverable states matching the given scope.
func (r *AgentOSPlanRepo) ListRecoverablePlanCommands(ctx context.Context, scope *agentosplan.PlanCommandScope) ([]agentosplan.PlanCommandRecord, error) {
	statuses, err := agentosplan.RecoverablePlanCommandStatuses(scope)
	if err != nil {
		return nil, err
	}

	query, args, err := r.recoverablePlanCommandsBuilder(scope, statuses).ToSql()
	if err != nil {
		return nil, fmt.Errorf("AgentOSPlanRepo - ListRecoverablePlanCommands - builder: %w", err)
	}

	rows, err := r.Pool.Query(ctx, query, args...)
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

func (r *AgentOSPlanRepo) recoverablePlanCommandsBuilder(scope *agentosplan.PlanCommandScope, statuses []agentosplan.PlanCommandStatus) sq.SelectBuilder {
	statusValues := make([]string, 0, len(statuses))
	for i := range statuses {
		statusValues = append(statusValues, string(statuses[i]))
	}

	builder := r.Builder.
		Select("command_id", _colPlanID, _colAccountID, _colProjectID, "actor_id", "action", _colIDempotencyKey, "payload_json", "status", "failure_reason", "created_at", "updated_at").
		From("plan_commands").
		Where(sq.Eq{"status": statusValues}).
		OrderBy("updated_at ASC", "command_id ASC")

	builder = applyOptionalEq(builder, _colPlanID, scope.PlanID)
	builder = applyOptionalEq(builder, _colAccountID, scope.AccountID)
	builder = applyOptionalEq(builder, _colProjectID, scope.ProjectID)

	if scope.Action != "" {
		builder = builder.Where(sq.Eq{"action": string(scope.Action)})
	}

	return applyOptionalLimit(builder, scope.Limit)
}

// MarkPlanCommandDelivered transitions a plan command to the delivered state.
func (r *AgentOSPlanRepo) MarkPlanCommandDelivered(ctx context.Context, ref agentosplan.PlanCommandRef) (agentosplan.PlanCommandRecord, error) {
	return r.updatePlanCommandStatus(ctx, ref, agentosplan.PlanCommandDelivered, "")
}

// MarkPlanCommandFailed transitions a plan command to the failed state with the given reason.
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
		return agentosplan.PlanCommandRecord{}, fmt.Errorf("%w: command %q", agentoscore.ErrInvalidRunPlan, ref.IdempotencyKey)
	}

	if err := agentosplan.ValidatePlanCommandStatusTransition(command.Status, status); err != nil {
		return agentosplan.PlanCommandRecord{}, err
	}

	if status == agentosplan.PlanCommandDelivered {
		if err := r.validatePlanCommandDeliveredAudit(ctx, &command); err != nil {
			return agentosplan.PlanCommandRecord{}, err
		}
	}

	command.Status = status
	command.FailureReason = reason
	command.UpdatedAt = time.Now().UTC()

	query := `
UPDATE plan_commands
SET status = $2,
	    failure_reason = $3,
	    updated_at = $4
	WHERE plan_id = $5
	  AND account_id = $6
	  AND project_id = $7
	  AND idempotency_key = $1
	RETURNING command_id, plan_id, account_id, project_id, actor_id, action, idempotency_key, payload_json, status, failure_reason, created_at, updated_at`

	return r.scanPlanCommandRow(ctx, r.Pool.QueryRow(ctx, query, ref.IdempotencyKey, string(command.Status), command.FailureReason, command.UpdatedAt, ref.PlanID, ref.AccountID, ref.ProjectID))
}

func (r *AgentOSPlanRepo) validatePlanCommandDeliveredAudit(ctx context.Context, command *agentosplan.PlanCommandRecord) error {
	commandAudit := agentosplan.AuditRecordFromPlanCommand(command)
	ref := agentosplan.AuditRefFromRecord(&commandAudit)

	audit, exists, err := r.GetAuditRecord(ctx, ref)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("%w: delivered command requires durable audit %q", agentoscore.ErrInvalidRunPlan, command.IdempotencyKey)
	}

	return agentosplan.ValidatePlanCommandDeliveredAudit(command, &audit)
}

func (r *AgentOSPlanRepo) scanPlanCommandRow(_ context.Context, scanner interface{ Scan(dest ...any) error }) (agentosplan.PlanCommandRecord, error) {
	var (
		command     agentosplan.PlanCommandRecord
		action      string
		status      string
		payloadJSON []byte
	)

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
	_ agentosplan.PlanTransitionStore       = (*AgentOSPlanRepo)(nil)
	_ agentosplan.PlanStateStore            = (*AgentOSPlanRepo)(nil)
	_ agentosplan.PlanEventStore            = (*AgentOSPlanRepo)(nil)
	_ agentosplan.PlanMetricCheckpointStore = (*AgentOSPlanRepo)(nil)
	_ agentosplan.PlanMetricsSink           = (*AgentOSPlanRepo)(nil)
	_ agentosplan.PlanCommandStore          = (*AgentOSPlanRepo)(nil)
	_ agentosplan.AuditStore                = (*AgentOSPlanRepo)(nil)
)
